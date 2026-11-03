//! State through the write path, and atomic publish (SPEC sections 3, 10).

mod common;

use ciborium::value::Value;
use common::*;
use format::{ENTRY_STATE_MERGE, ENTRY_STATE_SET, Records, SystemRecord};
use proptest::prelude::*;
use proto::ErrorCode;
use server::{MAX_STATE_BYTES, Reply, StateChange, StateOp, StateOpRequest, StreamState};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

fn op(stream: &str, req: u32, expect_ver: u64, op: StateOp, doc: &Value) -> StateOpRequest {
    StateOpRequest {
        stream: key(stream),
        req,
        author: AUTHOR,
        expect_ver,
        op,
        doc: cbor(doc),
    }
}

async fn expect_result(client: &mut server::Client) -> (u32, bool, u64) {
    match expect_reply(client).await {
        Reply::StateResult { req, ok, ver } => (req, ok, ver),
        other => panic!("expected a state result, got {other:?}"),
    }
}

async fn expect_error(client: &mut server::Client, want_req: u32) -> ErrorCode {
    match expect_reply(client).await {
        Reply::Error {
            reference, code, ..
        } => {
            assert_eq!(reference, want_req);
            code
        }
        other => panic!("expected an error, got {other:?}"),
    }
}

fn incr(path: &str, n: i64) -> Value {
    map(&[
        ("path", Value::Array(vec![Value::Text(path.to_owned())])),
        ("n", int(n)),
    ])
}

#[tokio::test]
async fn each_op_applies_and_its_version_is_its_system_records_id() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;

    client
        .state_op(op(
            "s",
            1,
            0,
            StateOp::Set,
            &map(&[("a", int(1)), ("b", int(2))]),
        ))
        .await
        .unwrap();
    let (_, ok, set_ver) = expect_result(&mut client).await;
    assert!(ok);

    client
        .state_op(op(
            "s",
            2,
            0,
            StateOp::Merge,
            &map(&[("a", Value::Null), ("c", int(3))]),
        ))
        .await
        .unwrap();
    let (_, ok, merge_ver) = expect_result(&mut client).await;
    assert!(ok);

    client
        .state_op(op("s", 3, 0, StateOp::Incr, &incr("c", 4)))
        .await
        .unwrap();
    let (_, ok, incr_ver) = expect_result(&mut client).await;
    assert!(ok);

    client
        .state_op(op("s", 4, 0, StateOp::Incr, &incr("new", 1)))
        .await
        .unwrap();
    let (_, ok, created_ver) = expect_result(&mut client).await;
    assert!(ok);
    assert!(set_ver < merge_ver && merge_ver < incr_ver && incr_ver < created_ver);

    let snapshot = client.snapshot(key("s")).await.unwrap();
    assert_eq!(snapshot.state_ver, created_ver);
    assert_eq!(snapshot.head, created_ver);
    assert_eq!(
        from_cbor(&snapshot.state),
        map(&[("b", int(2)), ("c", int(7)), ("new", int(1))])
    );

    // The WAL holds the operations as system records, with the version as id.
    let entries = harness.wal.stream_entries("s");
    assert_eq!(entries.len(), 4);
    let mut replayed = StreamState::new();
    for (entry, (ver, entry_type)) in entries.iter().zip([
        (set_ver, ENTRY_STATE_SET),
        (merge_ver, ENTRY_STATE_MERGE),
        (incr_ver, format::ENTRY_STATE_INCR),
        (created_ver, format::ENTRY_STATE_INCR),
    ]) {
        assert_eq!(entry.producer_id, 0);
        let mut records = Records::new(&entry.records);
        let view = records.next().unwrap().unwrap();
        assert!(records.next().is_none());
        assert_eq!(view.header.id, ver);
        assert_eq!(view.header.author, 0);
        assert_eq!(view.header.options, entry_type);
        let system = SystemRecord::parse(&view).unwrap();
        assert_eq!(system.actor, AUTHOR.get());
        replayed.replay(&view).unwrap();
    }
    assert_eq!(replayed.doc(), &snapshot.state);
    assert_eq!(replayed.ver(), snapshot.state_ver);
    harness.finish();
}

#[tokio::test]
async fn a_failure_leaves_the_state_and_the_log_alone() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    client
        .state_op(op(
            "s",
            1,
            0,
            StateOp::Set,
            &map(&[("text", Value::Text("x".into()))]),
        ))
        .await
        .unwrap();
    let (_, _, ver) = expect_result(&mut client).await;
    let before = client.snapshot(key("s")).await.unwrap();

    // Incr on a non-number.
    client
        .state_op(op("s", 2, 0, StateOp::Incr, &incr("text", 1)))
        .await
        .unwrap();
    assert_eq!(expect_error(&mut client, 2).await, ErrorCode::Malformed);
    // Not CBOR.
    let mut bad = op("s", 3, 0, StateOp::Set, &int(1));
    bad.doc = bytes::Bytes::from_static(&[0xFF]);
    client.state_op(bad).await.unwrap();
    assert_eq!(expect_error(&mut client, 3).await, ErrorCode::Malformed);
    // Doc over 64 KB.
    let over = Value::Text("x".repeat(MAX_STATE_BYTES));
    client
        .state_op(op("s", 4, 0, StateOp::Set, &over))
        .await
        .unwrap();
    assert_eq!(expect_error(&mut client, 4).await, ErrorCode::Malformed);
    // Result over 64 KB, from a legal doc.
    let half = Value::Text("x".repeat(MAX_STATE_BYTES / 2));
    client
        .state_op(op("s", 5, 0, StateOp::Merge, &map(&[("a", half.clone())])))
        .await
        .unwrap();
    assert!(expect_result(&mut client).await.1);
    let before_big = client.snapshot(key("s")).await.unwrap();
    client
        .state_op(op("s", 6, 0, StateOp::Merge, &map(&[("b", half)])))
        .await
        .unwrap();
    assert_eq!(expect_error(&mut client, 6).await, ErrorCode::Malformed);
    assert_eq!(client.snapshot(key("s")).await.unwrap(), before_big);

    // Version conflict: `ok = 0` carrying the current version (D6b).
    let current = before_big.state_ver;
    client
        .state_op(op("s", 7, ver, StateOp::Set, &int(1)))
        .await
        .unwrap();
    assert_eq!(expect_result(&mut client).await, (7, false, current));
    assert_eq!(client.snapshot(key("s")).await.unwrap(), before_big);
    assert!(before.state_ver < current);

    // The matching version works, and is the only record those attempts left.
    client
        .state_op(op("s", 8, current, StateOp::Set, &int(1)))
        .await
        .unwrap();
    let (req, ok, _) = expect_result(&mut client).await;
    assert_eq!((req, ok), (8, true));
    assert_eq!(harness.wal.stream_entries("s").len(), 3);
    harness.finish();
}

#[tokio::test]
async fn the_first_state_write_creates_the_stream_and_expect_ver_guards_it() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    assert_eq!(client.snapshot(key("s")).await, Err(ErrorCode::NotFound));
    client
        .state_op(op("s", 1, 5, StateOp::Set, &int(1)))
        .await
        .unwrap();
    assert_eq!(expect_result(&mut client).await, (1, false, 0));
    assert_eq!(client.snapshot(key("s")).await, Err(ErrorCode::NotFound));
    client
        .state_op(op("s", 2, 0, StateOp::Set, &int(1)))
        .await
        .unwrap();
    assert!(expect_result(&mut client).await.1);
    assert!(client.snapshot(key("s")).await.is_ok());
    harness.finish();
}

#[tokio::test]
async fn a_state_op_acks_after_the_wal() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    harness.wal.hold();
    client
        .state_op(op("s", 1, 0, StateOp::Set, &int(1)))
        .await
        .unwrap();
    expect_quiet(&mut client).await;
    harness.wal.release();
    assert!(expect_result(&mut client).await.1);
    harness.finish();
}

#[tokio::test]
async fn a_snapshot_is_state_and_head_as_one_step() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"x");
    client
        .state_op(op("s", 1, 0, StateOp::Set, &int(1)))
        .await
        .unwrap();
    let (_, _, ver) = expect_result(&mut client).await;
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    let (_, id, _) = expect_ack(&mut client).await;
    let snapshot = client.snapshot(key("s")).await.unwrap();
    assert_eq!(snapshot.state_ver, ver);
    assert_eq!(snapshot.head, id);
    assert!(snapshot.head > snapshot.state_ver);
    harness.finish();
}

fn atomic<'a>(records: &'a [u8], seq: u64, expect_ver: u64, doc: Value) -> server::Publish<'a> {
    let mut request = publish("s", records, seq, 2);
    request.state = Some(StateChange {
        expect_ver,
        op: StateOp::Merge,
        doc: cbor(&doc),
    });
    request
}

#[tokio::test]
async fn a_failed_expect_ver_commits_nothing_and_shows_no_ids() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let mut records = Vec::new();
    add_record(&mut records, 1, b"one", 1);
    add_record(&mut records, 2, b"two", 3);

    client
        .publish(atomic(&records, 1, 0, map(&[("n", int(1))])))
        .await
        .unwrap();
    let (_, first_id, _) = expect_ack(&mut client).await;
    let before = client.snapshot(key("s")).await.unwrap();
    let entries_before = harness.wal.stream_entries("s").len();

    client
        .publish(atomic(
            &records,
            2,
            before.state_ver + 1,
            map(&[("n", int(2))]),
        ))
        .await
        .unwrap();
    match expect_reply(&mut client).await {
        Reply::Nack {
            code, producer_seq, ..
        } => {
            assert_eq!(code, ErrorCode::VersionConflict);
            assert_eq!(producer_seq, 2);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(client.snapshot(key("s")).await.unwrap(), before);
    assert_eq!(harness.wal.stream_entries("s").len(), entries_before);
    // No ids were used, with the clock not moving.
    client.publish(publish("s", &records, 2, 1)).await.unwrap();
    let (seq, next_id, _) = expect_ack(&mut client).await;
    assert_eq!(seq, 2, "the rejected publish didn't use up its seq");
    assert_eq!(next_id, before.head + 1);
    assert!(next_id > first_id);
    harness.finish();
}

#[tokio::test]
async fn a_rejected_doc_commits_nothing_on_a_new_stream_either() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"x");
    let mut request = publish("s", &records, 1, 2);
    request.state = Some(StateChange {
        expect_ver: 0,
        op: StateOp::Set,
        doc: bytes::Bytes::from_static(&[0xFF]),
    });
    client.publish(request).await.unwrap();
    assert!(matches!(
        expect_reply(&mut client).await,
        Reply::Nack {
            code: ErrorCode::Malformed,
            ..
        }
    ));
    assert_eq!(client.snapshot(key("s")).await, Err(ErrorCode::NotFound));
    assert!(harness.wal.stream_entries("s").is_empty());
    harness.finish();
}

#[tokio::test]
async fn success_has_consecutive_ids_with_the_state_record_last_in_one_wal_entry() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let mut records = Vec::new();
    add_record(&mut records, 1, b"one", 1);
    add_record(&mut records, 2, b"two two", 3);
    let doc = map(&[("n", int(1))]);

    client
        .publish(atomic(&records, 1, 0, doc.clone()))
        .await
        .unwrap();
    let (_, first_id, count) = expect_ack(&mut client).await;
    assert_eq!(count, 5, "1 + 3 items + the system record (D5)");

    let entries = harness.wal.stream_entries("s");
    assert_eq!(entries.len(), 1, "one WAL entry");
    assert_eq!(entries[0].producer_id, 1);
    assert_eq!(entries[0].producer_seq, 1);
    let views: Vec<_> = Records::new(&entries[0].records)
        .map(Result::unwrap)
        .collect();
    assert_eq!(views.len(), 3);
    assert_eq!(views[0].header.id, first_id);
    assert_eq!(views[0].header.author, AUTHOR.get());
    assert_eq!(views[0].data, b"one");
    assert_eq!(views[1].header.id, first_id + 1);
    assert_eq!(views[1].data, b"two two");
    let system = &views[2];
    assert_eq!(system.header.id, first_id + 4);
    assert_eq!(system.header.author, 0);
    let parsed = SystemRecord::parse(system).unwrap();
    assert_eq!(parsed.actor, AUTHOR.get());
    assert_eq!(parsed.doc, &cbor(&doc)[..]);

    let snapshot = client.snapshot(key("s")).await.unwrap();
    assert_eq!(snapshot.state_ver, first_id + 4);
    assert_eq!(snapshot.head, first_id + 4);
    assert_eq!(from_cbor(&snapshot.state), doc);

    // A retry is a duplicate, even though `expect_ver` would now fail.
    let mut retry = atomic(&records, 1, 1, doc);
    retry.ack_level = Some(1);
    client.publish(retry).await.unwrap();
    assert_eq!(expect_ack(&mut client).await, (1, first_id, 5));
    harness.finish();
}

#[tokio::test]
async fn a_state_only_publish_is_allowed() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    client
        .publish(atomic(&[], 1, 0, map(&[("n", int(1))])))
        .await
        .unwrap();
    let (_, first_id, count) = expect_ack(&mut client).await;
    assert_eq!(count, 1);
    assert_eq!(client.snapshot(key("s")).await.unwrap().state_ver, first_id);
    harness.finish();
}

async fn live_against_replay(ops: Vec<(u8, Value, bool)>) {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, b"x");
    for (at, (kind, doc, with_records)) in ops.iter().enumerate() {
        let state_op = match kind {
            0 => StateOp::Set,
            1 => StateOp::Merge,
            _ => StateOp::Incr,
        };
        if *with_records {
            let mut request = publish("s", &records, at as u64 + 1, 1);
            request.state = Some(StateChange {
                expect_ver: 0,
                op: state_op,
                doc: cbor(doc),
            });
            client.publish(request).await.unwrap();
        } else {
            client
                .state_op(op("s", at as u32, 0, state_op, doc))
                .await
                .unwrap();
        }
        let _ = expect_reply(&mut client).await;
    }
    // Level 2 and a new stream's worth of nothing: everything before is in the WAL now.
    let barrier = record_with(1, b"barrier");
    client
        .publish(publish("s", &barrier, 1 << 40, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;

    let live = client.snapshot(key("s")).await.unwrap();
    let mut replayed = StreamState::new();
    for entry in harness.wal.stream_entries("s") {
        for view in Records::new(&entry.records) {
            let view = view.unwrap();
            if view.header.is_system() {
                replayed.replay(&view).unwrap();
            }
        }
    }
    assert_eq!(replayed.doc(), &live.state);
    assert_eq!(replayed.ver(), live.state_ver);
    harness.finish();
}

fn arb_doc() -> impl Strategy<Value = (u8, Value, bool)> {
    let key = prop_oneof![Just("a"), Just("b"), Just("c")];
    let value = prop_oneof![
        Just(Value::Null),
        (-5i64..5).prop_map(int),
        Just(Value::Text("t".to_owned())),
        (-5i64..5).prop_map(|n| map(&[("a", int(n)), ("b", Value::Null)])),
    ];
    let doc = proptest::collection::vec((key.clone(), value), 0..3).prop_map(|pairs| map(&pairs));
    prop_oneof![
        (doc.clone(), any::<bool>()).prop_map(|(doc, with)| (0u8, doc, with)),
        (doc, any::<bool>()).prop_map(|(doc, with)| (1u8, doc, with)),
        (key, -5i64..5, any::<bool>()).prop_map(|(key, n, with)| (2u8, incr(key, n), with)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// What the WAL holds, replayed, is what the live node ended up with.
    #[test]
    fn replaying_the_wal_equals_the_live_state(ops in proptest::collection::vec(arb_doc(), 1..30)) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(live_against_replay(ops));
    }
}

/// `STATE_OP` has no ack field, so nobody asked for less than section 10's "then acked".
#[tokio::test]
async fn a_state_op_waits_for_the_wal_whatever_the_streams_default_level() {
    for default_ack_level in [0, 1, 2] {
        let harness = Harness::new(manual_config(1));
        let mut client = harness.node().client(0).await;
        client
            .configure(
                key("s"),
                server::StreamConfig {
                    default_ack_level,
                    min_ack_level: 0,
                    ..Default::default()
                },
            )
            .await;
        harness.wal.hold();
        client
            .state_op(StateOpRequest {
                stream: key("s"),
                req: 1,
                author: AUTHOR,
                expect_ver: 0,
                op: StateOp::Set,
                doc: cbor(&int(1)),
            })
            .await
            .unwrap();
        expect_quiet(&mut client).await;
        harness.wal.release();
        assert!(matches!(
            expect_reply(&mut client).await,
            Reply::StateResult { ok: true, .. }
        ));
        assert_eq!(harness.wal.stream_entries("s").len(), 1);
        harness.finish();
    }
}
