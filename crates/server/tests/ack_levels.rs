//! Each ack level waits for what it promises, and nothing else (SPEC section 7.2).

mod common;

use common::*;
use server::{Reply, SealOutcome, StreamConfig};

#[tokio::test]
async fn level_2_never_acks_while_the_fsync_is_held() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    harness.wal.hold();
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 2)).await.unwrap();
    expect_quiet(&mut client).await;
    assert_eq!(
        harness
            .wal
            .sync_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    harness.wal.release();
    let (seq, first_id, count) = expect_ack(&mut client).await;
    assert_eq!((seq, count), (1, 1));
    assert!(first_id > 0);
    assert!(
        harness
            .wal
            .sync_count
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
    );
    harness.finish();
}

#[tokio::test]
async fn level_3_never_acks_while_the_upload_is_held_and_acks_after_it() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    harness.store.hold();
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 3)).await.unwrap();
    // Not until there is a seal, whatever the WAL has done.
    expect_quiet(&mut client).await;
    {
        let seal = harness.node().seal();
        tokio::pin!(seal);
        tokio::select! {
            outcome = &mut seal => panic!("the seal can't finish with the PUT held: {outcome:?}"),
            reply = client.recv_within(QUIET_MS) => assert_eq!(reply, None),
        }
        harness.store.release();
        assert!(matches!(seal.await, SealOutcome::Uploaded { .. }));
    }
    let (seq, first_id, _) = expect_ack(&mut client).await;
    assert_eq!(seq, 1);
    assert!(first_id > 0);
    harness.finish();
}

#[tokio::test]
async fn level_1_acks_with_the_fsync_and_the_upload_both_held() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    harness.wal.hold();
    harness.store.hold();
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    let (seq, first_id, count) = expect_ack(&mut client).await;
    assert_eq!((seq, count), (1, 1));
    assert!(first_id > 0);
    assert_eq!(
        harness
            .store
            .put_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    harness.finish();
}

#[tokio::test]
async fn level_0_sends_nothing_but_still_commits_and_a_rejection_still_nacks() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 0)).await.unwrap();
    expect_quiet(&mut client).await;
    let snapshot = client.snapshot("s").await.unwrap();
    assert!(snapshot.head > 0);

    let mut bad = record_with(1, b"hello");
    bad[0] = 1; // A non-zero id.
    client.publish(publish("s", &bad, 2, 0)).await.unwrap();
    match expect_reply(&mut client).await {
        Reply::Nack {
            code, producer_seq, ..
        } => {
            assert_eq!(code, proto::ErrorCode::Malformed);
            assert_eq!(producer_seq, 2);
        }
        other => panic!("expected a nack, got {other:?}"),
    }
    harness.finish();
}

#[tokio::test]
async fn a_publish_below_the_streams_minimum_is_raised_to_it() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    client
        .configure(
            "s",
            StreamConfig {
                default_ack_level: 2,
                min_ack_level: 2,
            },
        )
        .await;
    harness.wal.hold();
    let records = record_with(1, b"hello");
    // Asks for 0 and for 1: both wait for the fsync, and level 0 gets an ack.
    client.publish(publish("s", &records, 1, 0)).await.unwrap();
    client.publish(publish("s", &records, 2, 1)).await.unwrap();
    expect_quiet(&mut client).await;
    harness.wal.release();
    let (seq_a, _, _) = expect_ack(&mut client).await;
    let (seq_b, _, _) = expect_ack(&mut client).await;
    assert_eq!((seq_a, seq_b), (1, 2));
    harness.finish();
}

#[tokio::test]
async fn no_ack_level_takes_the_streams_default() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"hello");
    // The default default is level 2 (section 7.2).
    harness.wal.hold();
    let mut request = publish("s", &records, 1, 0);
    request.ack_level = None;
    client.publish(request.clone()).await.unwrap();
    expect_quiet(&mut client).await;
    harness.wal.release();
    expect_ack(&mut client).await;

    client
        .configure(
            "s",
            StreamConfig {
                default_ack_level: 1,
                min_ack_level: 0,
            },
        )
        .await;
    harness.wal.hold();
    request.producer_seq = 2;
    client.publish(request).await.unwrap();
    expect_ack(&mut client).await;
    harness.finish();
}

#[tokio::test]
async fn a_failed_sync_stops_level_2_acks_and_leaves_the_core_serving() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    harness
        .wal
        .fail_sync
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 2)).await.unwrap();
    expect_quiet(&mut client).await;
    assert_eq!(harness.node().ping(0).await, Some(0));
    harness.node.as_ref().unwrap().stats(0).await.unwrap();
    let exit = harness.finish();
    assert!(exit.wal.is_err());
}
