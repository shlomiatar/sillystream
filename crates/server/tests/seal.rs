//! Seal and handoff to the uploader (SPEC sections 6.2, 6.4, 7.3).

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use format::id_ms;
use object_store::ObjectStore;
use server::SealOutcome;
use storage::{SegmentReader, dedicated_path, seal_path};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

#[tokio::test]
async fn one_put_per_seal_however_many_streams_and_cores() {
    let harness = Harness::new(manual_config(4));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let mut expected = Vec::new();
    for index in 0..300 {
        let name = format!("many.{index}");
        client
            .publish(publish(&name, &records, 1, 2))
            .await
            .unwrap();
        let (_, first_id, _) = expect_ack(&mut client).await;
        expected.push((name, first_id));
    }
    let lowest = expected.iter().map(|(_, id)| *id).min().unwrap();
    let (path, extents) = match node.seal().await {
        SealOutcome::Uploaded { path, extents, .. } => (path, extents),
        other => panic!("{other:?}"),
    };
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 1);
    assert_eq!(path, seal_path("node", 1, lowest));
    assert_eq!(id_ms(lowest), START_MS);
    assert_eq!(extents.len(), expected.len());

    let store: Arc<dyn ObjectStore> = harness.store.clone();
    let reader = SegmentReader::new(store, path);
    for (name, first_id) in &expected {
        let stored = reader.read_stream(key(name)).await.unwrap();
        // Identical to the wire bytes, with only id and author stamped.
        let parsed = parse_records(&stored);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, *first_id);
        assert_eq!(parsed[0].3, b"payload");
        assert_eq!(stored.len(), records.len());
    }

    // A seal with nothing new writes nothing, and still finishes.
    assert!(matches!(node.seal().await, SealOutcome::Empty { .. }));
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 1);
    harness.finish();
}

#[tokio::test]
async fn a_section_is_only_what_arrived_before_it_and_the_rest_goes_next() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    client.publish(publish("s", &records, 1, 2)).await.unwrap();
    let (_, first_id, _) = expect_ack(&mut client).await;
    let first = match node.seal().await {
        SealOutcome::Uploaded { extents, .. } => extents,
        other => panic!("{other:?}"),
    };
    assert_eq!(first[0].id_first, first_id);
    assert_eq!(first[0].id_last, first_id);

    client.publish(publish("s", &records, 2, 2)).await.unwrap();
    let (_, second_id, _) = expect_ack(&mut client).await;
    let second = match node.seal().await {
        SealOutcome::Uploaded { extents, .. } => extents,
        other => panic!("{other:?}"),
    };
    assert_eq!(second[0].id_first, second_id);
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 2);
    harness.finish();
}

/// The cap is one total for the node, counted by the WAL writer, so half of it, which was a
/// core's share when cores counted, seals nothing (D268).
#[tokio::test]
async fn reaching_the_segment_cap_seals_early() {
    let mut config = manual_config(2);
    config.segment_cap_bytes = 32 * 1024;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    let records = record_with(1, &[5; 4096]);
    for seq in 1..=4 {
        client
            .publish(publish("s", &records, seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 0);

    // Level 3 acks come only from a seal, and nothing here asks for one.
    for seq in 5..=8 {
        client
            .publish(publish("s", &records, seq, 3))
            .await
            .unwrap();
    }
    for _ in 0..4 {
        expect_ack(&mut client).await;
    }
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 1);
    harness.finish();
}

#[tokio::test]
async fn below_the_cap_nothing_seals_without_the_window() {
    let mut config = manual_config(2);
    config.segment_cap_bytes = 1024 * 1024;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    let records = record_with(1, &[5; 4096]);
    client.publish(publish("s", &records, 1, 3)).await.unwrap();
    expect_quiet(&mut client).await;
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 0);
    harness.finish();
}

#[tokio::test]
async fn a_stream_filling_its_buffer_gets_a_dedicated_object() {
    let mut config = manual_config(1);
    config.stream_buffer_bytes = 8 * 1024;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    let records = record_with(1, &[5; 4096]);
    let small = record_with(1, b"small");
    client
        .publish(publish("quiet", &small, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    client
        .publish(publish("busy", &records, 1, 3))
        .await
        .unwrap();
    let (_, first_id, _) = {
        client
            .publish(publish("busy", &records, 2, 3))
            .await
            .unwrap();
        expect_ack(&mut client).await
    };
    expect_ack(&mut client).await;
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 1);

    let store: Arc<dyn ObjectStore> = harness.store.clone();
    let reader = SegmentReader::new(store, dedicated_path(key("busy"), 1, first_id));
    let stored = reader.read_stream(key("busy")).await.unwrap();
    assert_eq!(parse_records(&stored).len(), 2);

    // The stream's bytes aren't in the next seal, and the quiet one's are.
    match harness.node().seal().await {
        SealOutcome::Uploaded { extents, .. } => {
            assert_eq!(extents.len(), 1);
            assert_eq!(extents[0].stream, key("quiet"));
        }
        other => panic!("{other:?}"),
    }
    harness.finish();
}

#[tokio::test]
async fn a_held_upload_keeps_a_seal_pending_and_the_core_serving() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    harness.store.hold();
    let records = record_with(1, b"x");
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    expect_ack(&mut client).await;
    {
        let seal = harness.node().seal();
        tokio::pin!(seal);
        tokio::select! {
            outcome = &mut seal => panic!("{outcome:?}"),
            _ = tokio::time::sleep(Duration::from_millis(QUIET_MS)) => {}
        }
        // Writing and pinging go on during it.
        client.publish(publish("s", &records, 2, 2)).await.unwrap();
        expect_ack(&mut client).await;
        assert_eq!(harness.node().ping(0).await, Some(0));
        harness.store.release();
        assert!(matches!(seal.await, SealOutcome::Uploaded { .. }));
    }
    // Its bytes stayed readable from memory until then. The second publish is still there.
    let core_held: usize = {
        let mut held = 0;
        for core in 0..2 {
            held += harness.node().read_memory(core, key("s")).await.len();
        }
        held
    };
    assert_eq!(core_held, 1);
    let exit = harness.finish();
    assert_eq!(exit.unfinished_upload_count, 0);
    assert!(exit.wal.is_ok());
}

/// With the uploader's channel at its smallest and its PUT held, seals pile up behind it. The
/// cores go on taking level 2 writes, and once the store is back everything is uploaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_upload_channel_at_a_seal_doesnt_stop_writes_or_lose_anything() {
    const CORE_COUNT: usize = 2;
    const ROUND_COUNT: u64 = 6;
    let mut config = manual_config(CORE_COUNT);
    config.upload_channel_capacity = 1;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"x");
    harness.store.hold();
    let mut seq = 0;
    let mut seals = Vec::new();
    for _ in 0..ROUND_COUNT {
        for stream in ["a", "b", "c", "d"] {
            seq += 1;
            client
                .publish(publish(stream, &records, seq, 2))
                .await
                .unwrap();
            expect_ack(&mut client).await;
        }
        seals.push(Box::pin(harness.node().seal()));
        // A seal is asked for by polling it for a moment. It stays pending.
        let seal = seals.last_mut().unwrap();
        tokio::select! {
            outcome = seal.as_mut() => panic!("a held upload finished: {outcome:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    assert_eq!(harness.node().ping(0).await, Some(0));
    assert_eq!(harness.store.put_count.load(Ordering::SeqCst), 0);
    harness.store.release();
    for seal in seals {
        assert!(!matches!(seal.await, SealOutcome::Failed { .. }));
    }
    assert!(matches!(
        harness.node().seal().await,
        SealOutcome::Uploaded { .. } | SealOutcome::Empty { .. }
    ));
    let exit = harness.finish();
    assert_eq!(exit.unfinished_upload_count, 0);
    assert!(exit.wal.is_ok());
}
