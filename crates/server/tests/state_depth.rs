//! An `incr` path can't be used to nest state deeper than state can be read back: a long
//! one must not take the node down, and a shorter one must not commit a doc nothing can
//! parse (SPEC section 10). A failure of the first kind aborts this whole test binary,
//! which is why these have a file of their own.

mod common;

use ciborium::value::Value;
use common::*;
use proto::ErrorCode;
use server::{Reply, StateOp, StateOpRequest};

fn request<'a>(stream: &'a str, req: u32, op: StateOp, doc: &Value) -> StateOpRequest<'a> {
    StateOpRequest {
        stream,
        req,
        author: AUTHOR,
        expect_ver: 0,
        op,
        doc: cbor(doc),
    }
}

fn incr_path(keys: usize) -> Value {
    map(&[
        ("path", Value::Array((0..keys).map(|_| int(1)).collect())),
        ("n", int(1)),
    ])
}

async fn expect_malformed(client: &mut server::Client, req: u32) {
    match expect_reply(client).await {
        Reply::Error {
            reference, code, ..
        } => {
            assert_eq!(reference, req);
            assert_eq!(code, ErrorCode::Malformed);
        }
        other => panic!("expected error 8, got {other:?}"),
    }
}

#[tokio::test]
async fn a_path_of_any_length_is_refused_and_the_node_stays_up() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    // The longest fits in 64 KB: one byte per key.
    for (req, keys) in [(1, 300), (2, 30_000), (3, 65_000)] {
        client
            .state_op(request("s", req, StateOp::Incr, &incr_path(keys)))
            .await
            .unwrap();
        expect_malformed(&mut client, req).await;
    }
    assert_eq!(harness.wal.stream_entries("s").len(), 0);
    assert_eq!(harness.node().ping(0).await, Some(0));
    harness.finish();
}

#[tokio::test]
async fn the_same_path_in_a_publish_is_refused_and_commits_nothing() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"payload");
    let mut with_state = publish("s", &records, 1, 2);
    with_state.state = Some(server::StateChange {
        expect_ver: 0,
        op: StateOp::Incr,
        doc: cbor(&incr_path(60_000)),
    });
    client.publish(with_state).await.unwrap();
    match expect_reply(&mut client).await {
        Reply::Nack { code, .. } => assert_eq!(code, ErrorCode::Malformed),
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_eq!(harness.wal.stream_entries("s").len(), 0);
    harness.finish();
}

#[tokio::test]
async fn a_path_too_deep_to_read_back_leaves_state_usable() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let start = map(&[("a", int(1))]);
    client
        .state_op(request("s", 1, StateOp::Set, &start))
        .await
        .unwrap();
    assert!(matches!(
        expect_reply(&mut client).await,
        Reply::StateResult { ok: true, .. }
    ));
    client
        .state_op(request("s", 2, StateOp::Incr, &incr_path(300)))
        .await
        .unwrap();
    expect_malformed(&mut client, 2).await;
    // Nothing changed, so a later merge still works on what was there.
    client
        .state_op(request("s", 3, StateOp::Merge, &map(&[("b", int(2))])))
        .await
        .unwrap();
    assert!(matches!(
        expect_reply(&mut client).await,
        Reply::StateResult { ok: true, .. }
    ));
    let snapshot = client.snapshot("s").await.unwrap();
    assert_eq!(
        from_cbor(&snapshot.state),
        map(&[("a", int(1)), ("b", int(2))])
    );
    harness.finish();
}

/// Every depth around the limit, on a core's own thread: whatever is accepted can be read
/// back and built on, and whatever isn't leaves no trace.
#[tokio::test]
async fn every_accepted_depth_stays_readable() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let mut accepted_count = 0;
    for keys in 240..280 {
        let stream = format!("s{keys}");
        client
            .state_op(request(&stream, 1, StateOp::Incr, &incr_path(keys)))
            .await
            .unwrap();
        match expect_reply(&mut client).await {
            Reply::StateResult { ok: true, .. } => {
                accepted_count += 1;
                client
                    .state_op(request(&stream, 2, StateOp::Incr, &incr_path(keys)))
                    .await
                    .unwrap();
                assert!(
                    matches!(
                        expect_reply(&mut client).await,
                        Reply::StateResult { ok: true, .. }
                    ),
                    "{keys} keys committed, and then could not be built on"
                );
            }
            Reply::Error { code, .. } => assert_eq!(code, ErrorCode::Malformed),
            other => panic!("{other:?}"),
        }
    }
    assert!(accepted_count > 0, "no depth in the range was accepted");
    assert!(accepted_count < 40, "every depth in the range was accepted");
    harness.finish();
}
