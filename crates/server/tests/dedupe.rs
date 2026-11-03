//! Idempotency through the write path (SPEC section 3).

mod common;

use std::mem::size_of;

use common::*;
use server::{
    DEDUPE_CAP_COUNT, DEDUPE_ENTRY_BYTES, DEDUPE_IDLE_MS, DedupeCheck, DedupeEntry, DedupeTable,
    Reply,
};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

#[test]
fn entries_stay_28_bytes() {
    assert_eq!(size_of::<DedupeEntry>(), 28);
    assert_eq!(DEDUPE_ENTRY_BYTES, 28);
    // And really cost that in a table.
    assert_eq!(size_of::<[DedupeEntry; 1000]>(), 28_000);
}

#[tokio::test]
async fn a_retry_of_the_latest_gets_the_original_ack_and_writes_nothing() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 5, 2)).await.unwrap();
    let original = expect_ack(&mut client).await;
    let entries_before = harness.wal.stream_entries("s").len();
    let head_before = client.snapshot(key("s")).await.unwrap().head;

    client.publish(publish("s", &records, 5, 2)).await.unwrap();
    assert_eq!(expect_ack(&mut client).await, original);
    assert_eq!(harness.wal.stream_entries("s").len(), entries_before);
    assert_eq!(client.snapshot(key("s")).await.unwrap().head, head_before);
    harness.finish();
}

#[tokio::test]
async fn a_retry_of_an_older_one_gets_first_id_zero() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 5, 2)).await.unwrap();
    client.publish(publish("s", &records, 9, 2)).await.unwrap();
    expect_ack(&mut client).await;
    expect_ack(&mut client).await;
    let entries_before = harness.wal.stream_entries("s").len();

    // Older than the latest, seen or not: a gap is allowed, so 7 can't be told apart.
    for seq in [5, 7] {
        client
            .publish(publish("s", &records, seq, 2))
            .await
            .unwrap();
        assert_eq!(expect_ack(&mut client).await, (seq, 0, 0));
    }
    assert_eq!(harness.wal.stream_entries("s").len(), entries_before);
    harness.finish();
}

#[tokio::test]
async fn a_retry_doesnt_ack_before_the_original_is_durable() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    harness.wal.hold();
    client.publish(publish("s", &records, 5, 2)).await.unwrap();
    client.publish(publish("s", &records, 5, 2)).await.unwrap();
    client.publish(publish("s", &records, 4, 2)).await.unwrap();
    expect_quiet(&mut client).await;
    harness.wal.release();
    let mut acks = [
        expect_ack(&mut client).await,
        expect_ack(&mut client).await,
        expect_ack(&mut client).await,
    ];
    acks.sort();
    assert_eq!(acks[0].0, 4);
    assert_eq!(acks[0].1, 0);
    assert_eq!(acks[1].1, acks[2].1);
    assert!(acks[1].1 > 0);
    harness.finish();
}

#[tokio::test]
async fn a_retry_at_level_3_waits_for_the_upload() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 5, 1)).await.unwrap();
    let original = expect_ack(&mut client).await;
    client.publish(publish("s", &records, 5, 3)).await.unwrap();
    expect_quiet(&mut client).await;
    assert!(matches!(
        harness.node().seal().await,
        server::SealOutcome::Uploaded { .. }
    ));
    assert_eq!(expect_ack(&mut client).await, original);
    // Uploaded and let go of by now, so a later retry acks at once.
    client.publish(publish("s", &records, 5, 3)).await.unwrap();
    assert_eq!(expect_ack(&mut client).await, original);
    harness.finish();
}

#[tokio::test]
async fn a_rejected_publish_does_not_use_up_its_seq() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let mut bad = record_with(1, b"hello");
    bad[16] = 1; // A non-zero author.
    client.publish(publish("s", &bad, 5, 2)).await.unwrap();
    assert!(matches!(
        expect_reply(&mut client).await,
        Reply::Nack { .. }
    ));
    // The stream wasn't even created by that.
    assert!(client.snapshot(key("s")).await.is_err());

    let good = record_with(1, b"hello");
    client.publish(publish("s", &good, 5, 2)).await.unwrap();
    let (seq, first_id, count) = expect_ack(&mut client).await;
    assert_eq!((seq, count), (5, 1));
    assert!(first_id > 0);
    harness.finish();
}

#[tokio::test]
async fn an_entry_expires_at_24_hours_idle() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 5, 1)).await.unwrap();
    let original = expect_ack(&mut client).await;

    // One ms before: still a duplicate.
    harness.clock.advance(DEDUPE_IDLE_MS - 1);
    client.publish(publish("s", &records, 5, 1)).await.unwrap();
    assert_eq!(expect_ack(&mut client).await, original);

    // At 24 hours: gone, so the retry creates a duplicate, as the spec accepts.
    harness.clock.advance(1);
    client.publish(publish("s", &records, 5, 1)).await.unwrap();
    let (_, first_id, _) = expect_ack(&mut client).await;
    assert!(first_id > original.1);
    // Level 2 and a later seq, so everything before it has reached the WAL.
    client.publish(publish("s", &records, 6, 2)).await.unwrap();
    expect_ack(&mut client).await;
    assert_eq!(harness.wal.stream_entries("s").len(), 3);
    harness.finish();
}

#[tokio::test]
async fn the_1001st_producer_evicts_the_oldest() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    let mut request = publish("s", &records, 1, 1);
    for producer_id in 1..=DEDUPE_CAP_COUNT as u64 {
        request.producer_id = producer_id;
        client.publish(request.clone()).await.unwrap();
        expect_ack(&mut client).await;
        harness.clock.advance(1);
    }
    let first_of_second = {
        request.producer_id = 2;
        client.publish(request.clone()).await.unwrap();
        expect_ack(&mut client).await.1
    };
    request.producer_id = 1001;
    client.publish(request.clone()).await.unwrap();
    expect_ack(&mut client).await;

    // Producer 1 was oldest, and is forgotten: its retry commits again.
    request.producer_id = 1;
    client.publish(request.clone()).await.unwrap();
    let again = expect_ack(&mut client).await.1;
    assert!(again > first_of_second);
    harness.finish();
}

#[test]
fn the_table_keeps_at_most_1000_and_evicts_by_age_not_by_producer_id() {
    let now_ms = START_MS;
    let mut table = DedupeTable::new();
    // Producer ids run opposite to age.
    for index in 0..1000u64 {
        let id = format::make_id(now_ms + index, 0);
        table.record(5000 - index, 1, id, 1, now_ms + index);
    }
    assert_eq!(table.len(), DEDUPE_CAP_COUNT);
    table.record(1, 1, format::make_id(now_ms + 2000, 0), 1, now_ms + 2000);
    assert_eq!(table.len(), DEDUPE_CAP_COUNT);
    assert_eq!(table.check(5000, 1, now_ms + 2000), DedupeCheck::New);
    assert!(matches!(
        table.check(4999, 1, now_ms + 2000),
        DedupeCheck::Latest { .. }
    ));
    // Expired entries go before a live one is evicted.
    let mut table = DedupeTable::new();
    table.record(1, 1, format::make_id(now_ms, 0), 1, now_ms);
    for index in 0..999u64 {
        let id = format::make_id(now_ms + DEDUPE_IDLE_MS - 1 + index, 0);
        table.record(100 + index, 1, id, 1, now_ms + DEDUPE_IDLE_MS - 1 + index);
    }
    let late = now_ms + DEDUPE_IDLE_MS + 1000;
    table.record(2, 1, format::make_id(late, 0), 1, late);
    assert_eq!(table.len(), 1000);
    assert!(matches!(
        table.check(100, 1, late),
        DedupeCheck::Latest { .. }
    ));
}

#[tokio::test]
async fn producer_0_has_no_idempotency() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    let mut request = publish("s", &records, 5, 1);
    request.producer_id = 0;
    client.publish(request.clone()).await.unwrap();
    let first = expect_ack(&mut client).await.1;
    client.publish(request).await.unwrap();
    let second = expect_ack(&mut client).await.1;
    assert!(second > first);
    harness.finish();
}
