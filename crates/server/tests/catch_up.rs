//! Catching up through memory, the local tier and the object store, and the switch to live
//! (SPEC section 5.1).

mod common;

use common::*;
use format::Clause;
use proptest::prelude::*;
use proto::ErrorCode;
use server::{Event, LocalCacheConfig, SealOutcome, Subscription};

const STREAM: &str = "catch.stream";
const MS_PER_DAY: u64 = 24 * 60 * 60 * 1000;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap()
}

/// Three tiers' worth of published records. `(first_id, last_id, publish number)`.
struct Tiers {
    records: Vec<(u64, u64)>,
    /// The publish numbers the tiers start at, in the order object, local, memory.
    object_first_id: u64,
    local_first_id: u64,
    memory_first_id: u64,
}

async fn fill_tiers(
    harness: &Harness,
    client: &mut server::Client,
    cache_dir: &std::path::Path,
    per_tier_count: u64,
) -> Tiers {
    let node = harness.node();
    make_stream(client, STREAM).await;
    let mut seq = 0;
    let mut records = Vec::new();
    let mut publish_some = async |client: &mut server::Client, records: &mut Vec<(u64, u64)>| {
        for _ in 0..per_tier_count {
            seq += 1;
            let mut bytes = Vec::new();
            // Every third record holds several items, so an id can fall inside one.
            let item_count = if seq % 3 == 0 { 5 } else { 1 };
            add_record(&mut bytes, seq, &seq.to_le_bytes(), item_count);
            client
                .publish(publish(STREAM, &bytes, seq, 2))
                .await
                .unwrap();
            let (_, first_id, _) = expect_ack(client).await;
            records.push((first_id, first_id + u64::from(item_count) - 1));
        }
    };

    publish_some(client, &mut records).await;
    let object_first_id = records[0].0;
    let SealOutcome::Uploaded { path, .. } = node.seal().await else {
        panic!("the seal should have uploaded");
    };
    // Gone from the cache, so only the store has it.
    std::fs::remove_file(cache_dir.join("cached").join(path.as_ref())).unwrap();

    publish_some(client, &mut records).await;
    let local_first_id = records[per_tier_count as usize].0;
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    wait_for_memory_to_empty(node, STREAM).await;

    publish_some(client, &mut records).await;
    let memory_first_id = records[2 * per_tier_count as usize].0;
    Tiers {
        records,
        object_first_id,
        local_first_id,
        memory_first_id,
    }
}

fn cached_config(core_count: usize, dir: &std::path::Path) -> server::NodeConfig {
    let mut config = manual_config(core_count);
    config.local_cache = Some(LocalCacheConfig {
        dir: dir.to_path_buf(),
        cap_bytes: 1024 * 1024 * 1024,
    });
    config
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn ids_arrive_in_order_with_no_gap_or_repeat_across_every_switch(
        start in 0usize..7,
        offset_percent in 0u64..100,
        small_share in any::<bool>(),
        live_count in 0u64..12,
        seal_while_reading in any::<bool>(),
        move_while_reading in any::<bool>(),
    ) {
        runtime().block_on(async move {
            let cache = tempfile::tempdir().unwrap();
            let mut config = cached_config(2, cache.path());
            if small_share {
                config.fair_share_bytes = 64;
                config.catch_up_read_bytes = 256;
                config.subscriber_queue_bytes = 512;
            }
            let harness = Harness::new(config);
            let node = harness.node();
            let mut client = node.client(0).await;
            let tiers = fill_tiers(&harness, &mut client, cache.path(), 30).await;
            let mut all = tiers.records.clone();

            let at = (all.len() as u64 * offset_percent / 100) as usize;
            let multi = all.iter().find(|(first, last)| first != last).unwrap();
            let from = match start {
                0 => 0,
                1 => multi.0 + 2, // inside a record: the whole record is sent
                2 => u64::MAX,
                3 => all[at].0,
                4 => tiers.object_first_id + 1,
                5 => tiers.local_first_id,
                _ => tiers.memory_first_id,
            };
            let head = all.last().unwrap().1;

            let mut connection = node.connection(1).await.unwrap();
            let mut heard = Heard::default();
            connection.subscribe(Subscription::new(1, STREAM, from));

            // Publishing carries on while it reads.
            let mut later = Vec::new();
            let more = live_count + 1;
            publish_more_level_1(&mut client, &mut later, 1000, more / 2).await;
            hear_for(&mut connection, &mut heard, 2).await;
            if move_while_reading {
                // The owner changes under it, and some batches stay behind on the old core.
                let shard = server::shard_of(STREAM, node.virtual_shard_count());
                let at = node.placement().await[shard as usize] as usize;
                assert!(node.move_shard(shard, 1 - at).await);
            }
            if seal_while_reading {
                // What it was about to read from memory moves out from under it.
                assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
            }
            publish_more_level_1(&mut client, &mut later, 1000 + more / 2, more - more / 2).await;
            all.extend(later);

            let last_id = all.last().unwrap().1;
            hear_until(&mut connection, &mut heard, |heard| {
                heard.records(1).last().is_some_and(|last| last.1 >= last_id)
            }).await;

            let expected: Vec<u64> = all
                .iter()
                .filter(|(_, last)| from == u64::MAX && *last > head || from != u64::MAX && *last > from)
                .map(|(first, _)| *first)
                .collect();
            let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
            assert_eq!(got, expected, "from {from}");
            drop((connection, client));
            harness.finish();
        });
    }
}

/// Published while a subscriber is reading, so the switch to live has a moving target.
async fn publish_more_level_1(
    client: &mut server::Client,
    records: &mut Vec<(u64, u64)>,
    first_seq: u64,
    count: u64,
) {
    for seq in first_seq..first_seq + count {
        let bytes = record_with(seq, &seq.to_le_bytes());
        client
            .publish(publish(STREAM, &bytes, seq, 1))
            .await
            .unwrap();
        let (_, first_id, _) = expect_ack(client).await;
        records.push((first_id, first_id));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_tier_is_read_from_where_it_is_and_bytes_are_identical() {
    let cache = tempfile::tempdir().unwrap();
    let harness = Harness::new(cached_config(1, cache.path()));
    let node = harness.node();
    let mut client = node.client(0).await;
    let tiers = fill_tiers(&harness, &mut client, cache.path(), 40).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    let last_id = tiers.records.last().unwrap().1;
    hear_until(&mut connection, &mut heard, |heard| {
        heard
            .records(1)
            .last()
            .is_some_and(|last| last.1 == last_id)
    })
    .await;

    let stats = node.stats(0).await.unwrap();
    assert!(stats.catch_up_object_bytes > 0, "{stats:?}");
    assert!(stats.catch_up_local_bytes > 0, "{stats:?}");
    assert!(stats.catch_up_memory_bytes > 0, "{stats:?}");

    let mut stored = Vec::new();
    for entry in harness.wal.stream_entries(STREAM) {
        stored.extend_from_slice(&entry.records);
    }
    let mut got = Vec::new();
    for event in &heard.events {
        if let Event::Data { records, .. } = event {
            got.extend_from_slice(&records.to_vec());
        }
    }
    assert_eq!(got, stored, "byte for byte, through every tier");
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_local_tier_sealed_records_come_from_the_object_store() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let records = record_with(1, b"sealed");
    client
        .publish(publish(STREAM, &records, 1, 3))
        .await
        .unwrap();
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    expect_ack(&mut client).await;
    wait_for_memory_to_empty(node, STREAM).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;
    let stats = node.stats(0).await.unwrap();
    assert!(stats.catch_up_object_bytes > 0);
    assert_eq!(stats.catch_up_local_bytes + stats.catch_up_memory_bytes, 0);
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rare_filter_reads_fewer_blocks_than_a_full_scan() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    // Each publish is about 8 KiB in its own batches, so there are many 64 KiB blocks.
    let data = vec![3u8; 1024];
    let rare_seq = 123;
    let total_count = 2_048u64;
    for seq in 1..=total_count {
        let options = if seq == rare_seq { 0x42 } else { 1 };
        let records = record_with(options, &data);
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    wait_for_memory_to_empty(node, STREAM).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() as u64 == total_count
    })
    .await;
    let full = node.stats(0).await.unwrap();
    assert!(full.catch_up_blocks_read_count > 8, "{full:?}");
    assert_eq!(full.catch_up_blocks_skipped_count, 0);

    let rare = [Clause::Eq {
        mask: 0xFF,
        value: 0x42,
    }];
    let mut subscription = Subscription::new(2, STREAM, 0);
    subscription.clauses = &rare;
    connection.subscribe(subscription);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(2).len() == 1
    })
    .await;
    assert_eq!(heard.records(2)[0].2, 0x42);
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.progress(2).is_empty()
    })
    .await;
    let after = node.stats(0).await.unwrap();
    let rare_read = after.catch_up_blocks_read_count - full.catch_up_blocks_read_count;
    let rare_skipped = after.catch_up_blocks_skipped_count - full.catch_up_blocks_skipped_count;
    assert_eq!(
        rare_read + rare_skipped,
        full.catch_up_blocks_read_count,
        "every block is either read or skipped"
    );
    assert_eq!(rare_read, 1, "only the block holding the match");
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filtered_catch_up_ends_in_progress_at_the_head() {
    let mut config = manual_config(1);
    config.progress_interval_ms = 60_000;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut last_id = 0;
    for seq in 1..=20 {
        let records = record_with(1, b"nothing it wants");
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        last_id = expect_ack(&mut client).await.1;
    }
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let rare = [Clause::Eq {
        mask: 0xFF,
        value: 0x42,
    }];
    let mut subscription = Subscription::new(1, STREAM, 0);
    subscription.clauses = &rare;
    connection.subscribe(subscription);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.progress(1).last() == Some(&last_id)
    })
    .await;
    assert!(heard.records(1).is_empty());
    assert!(heard.progress(1).len() <= 2, "{:?}", heard.progress(1));
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_does_not_see_what_is_not_visible_yet() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let records = record_with(1, b"durable");
    client
        .publish(publish(STREAM, &records, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;

    harness.wal.hold();
    let records = record_with(2, b"waiting for the WAL");
    client
        .publish(publish(STREAM, &records, 2, 2))
        .await
        .unwrap();

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;
    hear_for(&mut connection, &mut heard, QUIET_MS).await;
    assert_eq!(heard.records(1).len(), 1);

    harness.wal.release();
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 2
    })
    .await;
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_that_moved_is_read_from_every_core_that_stamped_it() {
    let harness = Harness::new(manual_config(3));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let shard = server::shard_of(STREAM, node.virtual_shard_count());
    let mut all = Vec::new();
    let mut seq = 0;
    for (round, core) in [1usize, 2, 0, 1, 2].into_iter().enumerate() {
        for _ in 0..8 {
            seq += 1;
            let records = record_with(seq, &seq.to_le_bytes());
            client
                .publish(publish(STREAM, &records, seq, 2))
                .await
                .unwrap();
            all.push(expect_ack(&mut client).await.1);
        }
        assert!(node.move_shard(shard, core).await);
        // Some of it sealed, the rest in memory on whichever core stamped it.
        if round == 2 {
            assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
        }
    }
    let mut connection = node.connection(1).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == all.len()
    })
    .await;
    let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
    assert_eq!(got, all);

    // And it is live, on whichever core has it now.
    let records = record_with(99, b"after");
    client
        .publish(publish(STREAM, &records, seq + 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == all.len() + 1
    })
    .await;
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reading_before_retention_by_explicit_id_is_error_7() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let records = record_with(1, b"old");
    client
        .publish(publish(STREAM, &records, 1, 2))
        .await
        .unwrap();
    let old_id = expect_ack(&mut client).await.1;
    let records = record_with(1, b"old too");
    client
        .publish(publish(STREAM, &records, 2, 2))
        .await
        .unwrap();
    let old_head = expect_ack(&mut client).await.1;
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));

    harness.clock.advance(2 * MS_PER_DAY);
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();

    // Records it would have had to read have expired.
    connection.subscribe(Subscription::new(1, STREAM, old_id - 1));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
    })
    .await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::RetentionExpired));

    // Holding the head misses nothing, however old it is.
    connection.subscribe(Subscription::new(2, STREAM, old_head));
    assert_eq!(subscribed(&mut connection, &mut heard, 2).await, old_head);

    // `from = 0` is the start of retention, which has nothing older in it.
    connection.subscribe(Subscription::new(3, STREAM, 0));
    subscribed(&mut connection, &mut heard, 3).await;
    let records = record_with(1, b"new");
    client
        .publish(publish(STREAM, &records, 3, 2))
        .await
        .unwrap();
    let new_id = expect_ack(&mut client).await.1;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(2).len() == 1 && heard.records(3).len() == 1
    })
    .await;
    assert_eq!(heard.records(2)[0].0, new_id);
    assert_eq!(heard.records(3)[0].0, new_id);
    drop((connection, client));
    harness.finish();
}

/// A store outage costs a reader time, not its subscription, and it reads on from where it was
/// with nothing missed (D87).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_read_that_fails_is_retried_until_it_works() {
    const FAILURE_COUNT: i64 = 5;
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let records = record_with(1, b"sealed");
    client
        .publish(publish(STREAM, &records, 1, 3))
        .await
        .unwrap();
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    expect_ack(&mut client).await;
    wait_for_memory_to_empty(node, STREAM).await;

    harness
        .store
        .failing_get_count
        .store(FAILURE_COUNT, std::sync::atomic::Ordering::SeqCst);
    let started = std::time::Instant::now();
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;
    assert_eq!(heard.error_code(1), None);
    assert_eq!(
        harness
            .store
            .failing_get_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "every scripted failure was met"
    );
    // At least one wait of 100 ms between tries.
    assert!(started.elapsed() >= std::time::Duration::from_millis(100));
    drop((connection, client));
    harness.finish();
}

/// The local tier is only a cache: a copy that fails its CRC falls through to the object store,
/// whose bytes are what the reader gets (D87).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_local_copy_falls_through_to_the_object_store() {
    let cache = tempfile::tempdir().unwrap();
    let harness = Harness::new(cached_config(1, cache.path()));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let records = record_with(1, &[0x42; 4096]);
    client
        .publish(publish(STREAM, &records, 1, 3))
        .await
        .unwrap();
    let SealOutcome::Uploaded { path, .. } = node.seal().await else {
        panic!("the seal should have uploaded");
    };
    expect_ack(&mut client).await;
    wait_for_memory_to_empty(node, STREAM).await;
    let local = cache.path().join("cached").join(path.as_ref());
    let mut bytes = std::fs::read(&local).unwrap();
    // Every byte, so that the block, the footer or both fail whichever is read first.
    for byte in &mut bytes {
        *byte ^= 0xFF;
    }
    // A new file under the old name, as nothing changes a segment in place.
    let corrupt = local.with_extension("corrupt");
    std::fs::write(&corrupt, &bytes).unwrap();
    std::fs::rename(&corrupt, &local).unwrap();

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, STREAM, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;
    assert_eq!(heard.error_code(1), None);
    assert_eq!(heard.records(1)[0].3, vec![0x42; 4096]);
    let stats = node.stats(0).await.unwrap();
    assert!(stats.catch_up_object_bytes > 0, "{stats:?}");
    assert_eq!(stats.catch_up_local_bytes, 0, "{stats:?}");
    drop((connection, client));
    harness.finish();
}
