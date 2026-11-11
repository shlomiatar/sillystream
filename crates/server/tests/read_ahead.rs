//! Reading ahead from the object store during a catch-up: several reads in flight per
//! subscription, delivered in id order, under the node's limits (SPEC section 5.1).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::*;
use server::{Event, Subscription};

fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

/// Publishes `count` records of `bytes` each to `stream`, sealing into `object_count`
/// objects, so only the store has them. Returns each record's id.
async fn fill(
    harness: &Harness,
    streams: &[&str],
    count: u64,
    bytes: usize,
    object_count: u64,
) -> Vec<Vec<u64>> {
    let node = harness.node();
    let mut client = node.client(0).await;
    for stream in streams {
        make_stream(&mut client, stream).await;
    }
    let mut ids = vec![Vec::new(); streams.len()];
    let per_object = count.div_ceil(object_count);
    for seq in 1..=count {
        let mut data = vec![0u8; bytes];
        data[..8].copy_from_slice(&seq.to_le_bytes());
        for (at, stream) in streams.iter().enumerate() {
            client
                .publish(publish(stream, &record_with(1, &data), seq, 2))
                .await
                .unwrap();
            let (_, first_id, _) = expect_ack(&mut client).await;
            ids[at].push(first_id);
        }
        if seq % per_object == 0 || seq == count {
            node.seal().await;
        }
    }
    for stream in streams {
        wait_for_memory_to_empty(node, stream).await;
    }
    ids
}

/// The ids and first 8 bytes of data a subscription received, in order.
fn received(heard: &Heard, sub: u32) -> Vec<(u64, u64)> {
    heard
        .records(sub)
        .iter()
        .map(|record| {
            (
                record.0,
                u64::from_le_bytes(record.3[..8].try_into().unwrap()),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_arrive_in_id_order_when_reads_complete_out_of_order() {
    let mut config = manual_config(1);
    // One 64 KiB block per read, so many reads are in flight at once.
    config.catch_up_read_bytes = 16 * 1024;
    let harness = Harness::new(config);
    let count = 2_048;
    let ids = fill(&harness, &["ahead.order"], count, 1024, 4).await;
    // Each read issued later waits less, so later ranges land first.
    *harness.store.get_delay_cycle_ms.lock().unwrap() = vec![40, 30, 20, 10, 1];

    let mut connection = harness.node().connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key("ahead.order"), 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() as u64 >= count
    })
    .await;
    let expected: Vec<(u64, u64)> = ids[0].iter().copied().zip(1..=count).collect();
    assert_eq!(received(&heard, 1), expected);
    assert!(
        harness.store.max_gets_in_flight.load(Ordering::SeqCst) >= 3,
        "reads overlapped"
    );
    drop(connection);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_client_holds_reads_ahead_to_a_bound() {
    let mut config = manual_config(1);
    config.catch_up_read_bytes = 64 * 1024;
    config.subscriber_queue_bytes = 256 * 1024;
    config.connection_window_bytes = 256 * 1024;
    let harness = Harness::new(config);
    let count = 1_024;
    let ids = fill(&harness, &["ahead.stall"], count, 8 * 1024, 2).await;
    harness.store.get_delay_ms.store(1, Ordering::SeqCst);
    let before = harness.store.range_get_bytes.load(Ordering::SeqCst);

    let mut connection = harness.node().connection(0).await.unwrap();
    connection.subscribe(Subscription::new(1, key("ahead.stall"), 0));
    // The client takes nothing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let stalled = harness.store.range_get_bytes.load(Ordering::SeqCst) - before;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let later = harness.store.range_get_bytes.load(Ordering::SeqCst) - before;
    assert_eq!(
        stalled, later,
        "reading stops while the client takes nothing"
    );
    // The window, the queue and four reads ahead, out of 8 MiB.
    assert!(
        later < 2 * 1024 * 1024,
        "read {later} bytes for a stalled client"
    );

    let mut heard = Heard::default();
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() as u64 >= count
    })
    .await;
    let expected: Vec<(u64, u64)> = ids[0].iter().copied().zip(1..=count).collect();
    assert_eq!(received(&heard, 1), expected);
    drop(connection);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_catch_ups_share_one_read_in_flight_fairly() {
    let mut config = manual_config(1);
    config.catch_up_read_bytes = 64 * 1024;
    config.object_reads_in_flight = 1;
    let harness = Harness::new(config);
    let count = 256;
    fill(&harness, &["ahead.a", "ahead.b"], count, 8 * 1024, 2).await;
    harness.store.get_delay_ms.store(5, Ordering::SeqCst);

    let got: Vec<Arc<AtomicU64>> = (0..2).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let mut readers = Vec::new();
    for (at, stream) in ["ahead.a", "ahead.b"].into_iter().enumerate() {
        let mut connection = harness.node().connection(0).await.unwrap();
        let got = Arc::clone(&got[at]);
        readers.push(tokio::spawn(async move {
            connection.subscribe(Subscription::new(1, key(stream), 0));
            while got.load(Ordering::SeqCst) < count {
                if let Some(Event::Data { records, .. }) = connection.recv_within(10_000).await {
                    let frame = records.to_vec();
                    got.fetch_add(
                        format::Records::new(&frame).count() as u64,
                        Ordering::SeqCst,
                    );
                }
            }
            connection
        }));
    }
    let give_up = Instant::now() + Duration::from_millis(WAIT_MS);
    let (first, other) = loop {
        assert!(Instant::now() < give_up, "catch-ups didn't finish");
        let counts: Vec<u64> = got.iter().map(|got| got.load(Ordering::SeqCst)).collect();
        if counts[0] >= count {
            break (0, counts[1]);
        }
        if counts[1] >= count {
            break (1, counts[0]);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    assert_eq!(harness.store.max_gets_in_flight.load(Ordering::SeqCst), 1);
    assert!(
        other * 10 >= count * 6,
        "when {first} finished the other had {other} of {count}"
    );
    for reader in readers {
        drop(reader.await.unwrap());
    }
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsubscribing_drops_reads_in_flight() {
    let mut config = manual_config(1);
    config.catch_up_read_bytes = 64 * 1024;
    let harness = Harness::new(config);
    fill(&harness, &["ahead.drop"], 512, 8 * 1024, 1).await;
    harness.store.get_delay_ms.store(5, Ordering::SeqCst);

    let mut connection = harness.node().connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key("ahead.drop"), 0));
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty()
    })
    .await;
    // Reads from here on take long enough to be caught in flight.
    harness.store.get_delay_ms.store(60_000, Ordering::SeqCst);
    let in_flight = || harness.store.gets_in_flight.load(Ordering::SeqCst);
    hear_until(&mut connection, &mut heard, |_| in_flight() >= 2).await;
    connection.unsubscribe(1);
    let give_up = Instant::now() + Duration::from_millis(WAIT_MS);
    while in_flight() != 0 {
        assert!(
            Instant::now() < give_up,
            "{} reads still in flight",
            in_flight()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(connection);
    harness.finish();
}
