//! A full WAL channel is backpressure on publishers and never blocks a core (D7).

mod common;

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use server::{ClientError, SealOutcome};

const CAPACITY: usize = 2;
const INBOX: usize = 4;

fn stalled_config() -> server::NodeConfig {
    let mut config = manual_config(1);
    config.wal_channel_capacity = CAPACITY;
    config.write_inbox_capacity = INBOX;
    config
}

/// Publishes with `try_publish` until the inbox says full, and returns how many it took.
async fn fill(client: &mut server::Client, records: &[u8], from_seq: u64) -> u64 {
    let mut seq = from_seq;
    loop {
        match client.try_publish(publish("s", records, seq, 2)) {
            Ok(()) => seq += 1,
            Err(ClientError::Full) => {
                // The core may still be reading. It stops once the channel is full.
                tokio::time::sleep(Duration::from_millis(5)).await;
                let again = client.try_publish(publish("s", records, seq, 2));
                match again {
                    Ok(()) => seq += 1,
                    Err(ClientError::Full) => return seq,
                    Err(error) => panic!("{error:?}"),
                }
            }
            Err(error) => panic!("{error:?}"),
        }
    }
}

#[tokio::test]
async fn a_stalled_writer_leaves_the_core_serving_and_loses_nothing() {
    let harness = Harness::new(stalled_config());
    let mut client = harness.node().client(0).await;
    let mut other = harness.node().client(0).await;
    let records = record_with(1, b"hello");

    // Something for the core to serve while stalled.
    other
        .publish(publish("other", &records, 1, 1))
        .await
        .unwrap();
    expect_ack(&mut other).await;
    let other_head = other.snapshot("other").await.unwrap().head;

    harness.wal.hold();
    let next_seq = fill(&mut client, &records, 1).await;
    let stats = harness.node().stats(0).await.unwrap();
    assert_eq!(stats.outbound_count, 1, "{stats:?}");
    // The writer holds one group, the channel `CAPACITY`, the core one, and the inbox `INBOX`.
    assert!(next_seq > (CAPACITY + INBOX) as u64, "{next_seq}");

    // Every kind of work other than writing goes on, and answers promptly.
    for _ in 0..50 {
        let started = std::time::Instant::now();
        assert_eq!(harness.node().ping(0).await, Some(0));
        assert_eq!(other.snapshot("other").await.unwrap().head, other_head);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
    assert!(!harness.node().read_memory(0, "other").await.is_empty());
    assert!(!harness.node().read_memory(0, "s").await.is_empty());
    expect_quiet(&mut client).await;

    // A blocking publish waits rather than failing, and doesn't stop the core either.
    let last_seq = next_seq;
    let blocked = {
        let publish_request = publish("s", &records, last_seq, 2);
        tokio::time::timeout(
            Duration::from_millis(QUIET_MS),
            client.publish(publish_request),
        )
        .await
    };
    assert!(blocked.is_err(), "the inbox is full");
    assert_eq!(harness.node().ping(0).await, Some(0));

    harness.wal.release();
    let total = (1..next_seq).count();
    let mut acked = Vec::new();
    for _ in 0..total {
        acked.push(expect_ack(&mut client).await);
    }
    // Every publish that was accepted is acked, with ids in publish order.
    assert_eq!(
        acked.iter().map(|(seq, _, _)| *seq).collect::<Vec<_>>(),
        (1..next_seq).collect::<Vec<_>>()
    );
    assert!(acked.windows(2).all(|pair| pair[0].1 < pair[1].1));

    // And the WAL has them once each, in that order.
    let entries = harness.wal.stream_entries("s");
    assert_eq!(
        entries.iter().map(|e| e.producer_seq).collect::<Vec<_>>(),
        (1..next_seq).collect::<Vec<_>>()
    );
    let ids: Vec<u64> = entries
        .iter()
        .map(|entry| parse_records(&entry.records)[0].0)
        .collect();
    assert_eq!(ids, acked.iter().map(|(_, id, _)| *id).collect::<Vec<_>>());
    assert_eq!(harness.node().stats(0).await.unwrap().outbound_count, 0);
    harness.finish();
}

#[tokio::test]
async fn a_seal_waits_for_room_and_only_takes_what_the_channel_took() {
    let harness = Harness::new(stalled_config());
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    harness.wal.hold();
    let next_seq = fill(&mut client, &records, 1).await;
    {
        let seal = harness.node().seal();
        tokio::pin!(seal);
        tokio::select! {
            outcome = &mut seal => panic!("rotation can't finish with the WAL stalled: {outcome:?}"),
            _ = tokio::time::sleep(Duration::from_millis(QUIET_MS)) => {}
        }
        assert_eq!(harness.node().ping(0).await, Some(0));
        harness.wal.release();
        match seal.await {
            SealOutcome::Uploaded { extents, .. } => {
                assert!(extents.iter().all(|extent| &*extent.stream == "s"));
            }
            other => panic!("{other:?}"),
        }
    }
    for _ in 1..next_seq {
        expect_ack(&mut client).await;
    }
    // Whatever that seal left, the next one has, and nothing is in two extents.
    let _ = harness.node().seal().await;
    assert!(harness.store.put_count.load(Ordering::SeqCst) >= 1);
    harness.finish();
}

#[tokio::test]
async fn a_memory_cap_makes_publishers_wait_until_uploads_free_slabs() {
    let mut config = manual_config(1);
    config.slab_bytes = 4096;
    config.memory_cap_bytes = 3 * 4096;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    harness.store.hold();
    let records = record_with(1, &[7; 1000]);
    let mut seq = 1;
    while client.pinned_bytes() < 3 * 4096 {
        client
            .publish(publish("s", &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        seq += 1;
    }
    assert_eq!(
        client.try_publish(publish("s", &records, seq, 1)),
        Err(ClientError::Full)
    );
    let blocked = tokio::time::timeout(
        Duration::from_millis(QUIET_MS),
        client.publish(publish("s", &records, seq, 1)),
    )
    .await;
    assert!(blocked.is_err());
    assert_eq!(harness.node().ping(0).await, Some(0));

    // The upload completing is what lets the slabs go.
    {
        let seal = harness.node().seal();
        tokio::pin!(seal);
        tokio::select! {
            outcome = &mut seal => panic!("{outcome:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
        harness.store.release();
        assert!(matches!(seal.await, SealOutcome::Uploaded { .. }));
    }
    client.retire_slab();
    let wait_until = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while client.pinned_bytes() >= 3 * 4096 {
        assert!(std::time::Instant::now() < wait_until, "slabs never freed");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Level 2, so that everything before it is in the WAL too when this ack arrives.
    client
        .publish(publish("s", &records, seq, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    let seqs: HashSet<u64> = harness
        .wal
        .stream_entries("s")
        .iter()
        .map(|entry| entry.producer_seq)
        .collect();
    assert_eq!(seqs.len() as u64, seq);
    harness.finish();
}
