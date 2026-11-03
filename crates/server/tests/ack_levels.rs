//! Each ack level waits for what it promises, and nothing else (SPEC section 7.2).

mod common;

use common::*;
use server::{Reply, SealOutcome, StreamConfig};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

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
    let snapshot = client.snapshot(key("s")).await.unwrap();
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
            key("s"),
            StreamConfig {
                default_ack_level: 2,
                min_ack_level: 2,
                ..Default::default()
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
            key("s"),
            StreamConfig {
                default_ack_level: 1,
                min_ack_level: 0,
                ..Default::default()
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
    // Never an ack: it wasn't made durable (D79 made this an error rather than silence).
    assert!(matches!(
        expect_reply(&mut client).await,
        Reply::Error { .. }
    ));
    expect_quiet(&mut client).await;
    assert_eq!(harness.node().ping(0).await, Some(0));
    harness.node.as_ref().unwrap().stats(0).await.unwrap();
    let exit = harness.finish();
    assert!(exit.wal.is_err());
}

#[tokio::test]
async fn a_dead_wal_is_reported_to_every_publisher_instead_of_looking_like_backpressure() {
    let mut config = manual_config(1);
    config.write_inbox_capacity = 4;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    harness
        .wal
        .fail_sync
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let records = record_with(1, b"hello");
    let send_count = 40u64;
    for seq in 1..=send_count {
        tokio::time::timeout(
            std::time::Duration::from_millis(WAIT_MS),
            client.publish(publish("s", &records, seq, 2)),
        )
        .await
        .expect("a dead WAL must not look like a full inbox for ever")
        .unwrap();
    }
    // A stamped write's outcome isn't known, and a refused one committed nothing.
    for _ in 0..send_count {
        match expect_reply(&mut client).await {
            Reply::Error { code, .. } | Reply::Nack { code, .. } => {
                assert_eq!(code, proto::ErrorCode::RateLimited);
            }
            other => panic!("nothing can be durable, got {other:?}"),
        }
    }
    client
        .state_op(server::StateOpRequest {
            stream: key("s"),
            req: 7,
            author: AUTHOR,
            expect_ver: 0,
            op: server::StateOp::Set,
            doc: cbor(&int(1)),
        })
        .await
        .unwrap();
    match expect_reply(&mut client).await {
        Reply::Error {
            reference, code, ..
        } => {
            assert_eq!(reference, 7);
            assert_eq!(code, proto::ErrorCode::RateLimited);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        harness.node().seal().await,
        server::SealOutcome::WalStopped
    ));
    assert_eq!(harness.node().ping(0).await, Some(0));
    assert!(harness.finish().wal.is_err());
}

#[tokio::test]
async fn a_dead_wal_ends_level_3_waits_and_reaches_a_core_with_nothing_queued() {
    let harness = Harness::new(manual_config(2));
    let mut client = harness.node().client(0).await;
    harness
        .wal
        .fail_sync
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 3)).await.unwrap();
    assert!(matches!(
        expect_reply(&mut client).await,
        Reply::Error {
            code: proto::ErrorCode::RateLimited,
            ..
        }
    ));
    // The other core has never tried the WAL.
    let name = (0..)
        .map(|n| format!("t{n}"))
        .find(|name| server::shard_of(key(name), harness.node().virtual_shard_count()) % 2 == 1)
        .unwrap();
    let mut other = harness.node().client(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(QUIET_MS)).await;
    assert!(harness.node().stats(1).await.unwrap().wal_stopped);
    other.publish(publish(&name, &records, 1, 1)).await.unwrap();
    assert!(matches!(
        expect_reply(&mut other).await,
        Reply::Nack {
            code: proto::ErrorCode::RateLimited,
            ..
        }
    ));
    harness.finish();
}
