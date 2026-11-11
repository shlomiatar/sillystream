//! What a connection's bounds must and must not do: `STATE` frames return their window, a
//! stalled subscriber can't hold more memory than its bound, frames stay within `max_frame`,
//! and a subscriber that is keeping up never gets `BEHIND` (SPEC section 5.2, D95 to D98).

mod common;

use common::*;
use format::Clause;
use server::{Event, NodeConfig, Reply, StateOp, StateOpRequest, Subscription};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const STREAM: &str = "bounds.stream";
const STATE_STREAM: &str = "bounds.state";

async fn set_state(client: &mut server::Client, stream: &str, req: u32, doc: bytes::Bytes) -> u64 {
    client
        .state_op(StateOpRequest {
            stream: key(stream),
            req,
            author: AUTHOR,
            expect_ver: 0,
            op: StateOp::Set,
            doc,
        })
        .await
        .unwrap();
    match expect_reply(client).await {
        Reply::StateResult { ok: true, ver, .. } => ver,
        other => panic!("expected a state result, got {other:?}"),
    }
}

fn data_lens(heard: &Heard) -> Vec<u64> {
    heard
        .events
        .iter()
        .filter_map(|event| match event {
            Event::Data { records, .. } => Some(records.len_bytes()),
            _ => None,
        })
        .collect()
}

fn state_frame_count(heard: &Heard) -> usize {
    heard
        .events
        .iter()
        .filter(|event| matches!(event, Event::State { .. }))
        .count()
}

fn behind_count(heard: &Heard) -> usize {
    heard
        .events
        .iter()
        .filter(|event| matches!(event, Event::Behind { .. }))
        .count()
}

/// A client reading everything must keep hearing, however much state went by.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_frames_a_client_has_read_return_their_window() {
    let mut config = manual_config(1);
    config.connection_window_bytes = 16 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    make_stream(&mut client, STATE_STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    let mut state_only = Subscription::new(2, key(STATE_STREAM), u64::MAX);
    state_only.state_only = true;
    connection.subscribe(state_only);
    subscribed(&mut connection, &mut heard, 1).await;
    subscribed(&mut connection, &mut heard, 2).await;

    let doc = cbor(&map(&[("pad", ciborium::Value::Text("x".repeat(1024)))]));
    let change_count = 40;
    for req in 1..=change_count {
        set_state(&mut client, STATE_STREAM, req, doc.clone()).await;
        hear_until(&mut connection, &mut heard, |heard| {
            state_frame_count(heard) as u32 >= req
        })
        .await;
    }
    let records = record_with(1, b"after the state");
    client
        .publish(publish(STREAM, &records, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty()
    })
    .await;
    drop((connection, client));
    harness.finish();
}

/// A doc bigger than the whole window, on a connection with nothing but state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_state_only_connection_recovers_from_a_doc_over_its_window() {
    let mut config = manual_config(1);
    config.connection_window_bytes = 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STATE_STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut state_only = Subscription::new(1, key(STATE_STREAM), u64::MAX);
    state_only.state_only = true;
    connection.subscribe(state_only);
    subscribed(&mut connection, &mut heard, 1).await;

    let doc = cbor(&map(&[("pad", ciborium::Value::Text("x".repeat(8192)))]));
    for req in 1..=5 {
        set_state(&mut client, STATE_STREAM, req, doc.clone()).await;
        hear_until(&mut connection, &mut heard, |heard| {
            state_frame_count(heard) as u32 >= req
        })
        .await;
    }
    drop((connection, client));
    harness.finish();
}

/// Publishes a 4-byte record the filter wants next to a large one it doesn't, sealing now and
/// then so that only the subscriber's references can hold the slabs.
async fn publish_rare_matches(
    node: &server::Node,
    client: &mut server::Client,
    round_count: u64,
) -> Option<u64> {
    let unwanted = vec![0u8; 60 * 1024];
    for seq in 1..=round_count {
        let mut records = Vec::new();
        add_record(&mut records, 1, b"rare", 1);
        add_record(&mut records, 2, &unwanted, 1);
        let sent = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.publish(publish(STREAM, &records, seq, 2)),
        )
        .await;
        match sent {
            Ok(result) => result.unwrap(),
            Err(_) => return Some(seq),
        }
        expect_ack(client).await;
        if seq % 8 == 0 {
            node.seal().await;
        }
    }
    None
}

fn rare_config() -> NodeConfig {
    let mut config = manual_config(1);
    config.memory_cap_bytes = 4 * 1024 * 1024;
    config
}

/// The subscriber's queue counts what it keeps alive, so it falls behind and lets go long
/// before the publisher feels it. Its matched bytes alone are far below every bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_subscriber_with_a_rare_filter_falls_behind_and_lets_go_of_slabs() {
    let harness = Harness::new(rare_config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut stalled = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let clauses = [Clause::Eq {
        mask: 0xFF,
        value: 1,
    }];
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &clauses;
    stalled.subscribe(subscription);
    subscribed(&mut stalled, &mut heard, 1).await;

    // Nothing reads `stalled` from here on.
    let round_count = 600;
    let blocked_at = publish_rare_matches(node, &mut client, round_count).await;
    assert_eq!(
        blocked_at, None,
        "the publisher was stopped by one subscriber"
    );
    node.seal().await;
    let behind = total_stat(node, |stats| stats.behind_count).await;
    assert_eq!(behind, 1, "it should have been told once");

    // When it comes back, it has everything: nothing was lost by letting go.
    hear_until(&mut stalled, &mut heard, |heard| {
        heard.records(1).len() as u64 == round_count
    })
    .await;
    assert_eq!(behind_count(&heard), 1);
    drop((stalled, client));
    harness.finish();
}

/// Unread frames count what they keep alive too: a frame per slab, each small, with a
/// window that the matched bytes would never close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unread_small_frames_do_not_hold_slabs_past_the_window() {
    let mut config = rare_config();
    config.connection_window_bytes = 4 * 1024 * 1024;
    config.subscriber_queue_bytes = 256 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut stalled = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let clauses = [Clause::Eq {
        mask: 0xFF,
        value: 1,
    }];
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &clauses;
    stalled.subscribe(subscription);
    subscribed(&mut stalled, &mut heard, 1).await;

    let blocked_at = publish_rare_matches(node, &mut client, 600).await;
    assert_eq!(blocked_at, None, "unread frames stopped the publisher");
    assert!(total_stat(node, |stats| stats.behind_count).await >= 1);
    drop((stalled, client));
    harness.finish();
}

/// The same with a small window, a tight cap and a queue whose bound the cap has to lower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tight_memory_cap_lowers_the_bound_so_the_publisher_still_runs() {
    let mut config = manual_config(1);
    config.memory_cap_bytes = 2 * 1024 * 1024;
    config.connection_window_bytes = 1024;
    config.subscriber_queue_bytes = 1024 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut stalled = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let clauses = [Clause::Eq { mask: 1, value: 1 }];
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &clauses;
    stalled.subscribe(subscription);
    subscribed(&mut stalled, &mut heard, 1).await;

    let blocked_at = publish_rare_matches(node, &mut client, 400).await;
    assert_eq!(
        blocked_at, None,
        "the publisher was stopped by one subscriber"
    );
    assert_eq!(total_stat(node, |stats| stats.behind_count).await, 1);
    drop((stalled, client));
    harness.finish();
}

/// Counting slabs mustn't cost a client that is reading: each frame crosses a slab here, and
/// it never falls behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reading_subscriber_with_a_rare_filter_never_gets_behind() {
    let harness = Harness::new(rare_config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let clauses = [Clause::Eq {
        mask: 0xFF,
        value: 1,
    }];
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &clauses;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;

    let unwanted = vec![0u8; 60 * 1024];
    let round_count = 200u64;
    for seq in 1..=round_count {
        let mut records = Vec::new();
        add_record(&mut records, 1, b"rare", 1);
        add_record(&mut records, 2, &unwanted, 1);
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        // Heard records would hold their slabs, as an unread frame does.
        hear_until(&mut connection, &mut heard, |heard| {
            !heard.records(1).is_empty()
        })
        .await;
        assert_eq!(
            behind_count(&heard),
            0,
            "this client read everything at once"
        );
        heard.events.clear();
        if seq % 8 == 0 {
            node.seal().await;
        }
    }
    drop((connection, client));
    harness.finish();
}

async fn lingering_frames(
    max_frame_bytes: u64,
    fair_share_bytes: u64,
    sizes: &[usize],
) -> Vec<u64> {
    let mut config = manual_config(1);
    config.max_frame_bytes = max_frame_bytes;
    config.fair_share_bytes = fair_share_bytes;
    config.subscriber_queue_bytes = 1024 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.linger_ms = 200;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;
    for (seq, size) in sizes.iter().enumerate() {
        let records = record_with(1, &vec![3u8; *size]);
        client
            .publish(publish(STREAM, &records, seq as u64 + 1, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == sizes.len()
    })
    .await;
    let lens = data_lens(&heard);
    drop((connection, client));
    harness.finish();
    lens
}

/// Each record fits a frame alone, so no frame has to go over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_record_after_a_small_one_starts_a_new_frame() {
    let lens = lingering_frames(4096, 4096, &[64, 4000]).await;
    assert_eq!(lens, vec![96, 4032]);
    let lens = lingering_frames(8192, 8192, &[4000, 8000]).await;
    assert_eq!(lens, vec![4032, 8032]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separate_batches_do_not_add_up_past_the_frame_limit() {
    let lens = lingering_frames(1024, 1024, &[500, 500, 500]).await;
    assert!(lens.iter().all(|len| *len <= 1014), "{lens:?}");
    assert_eq!(lens.iter().sum::<u64>(), 3 * 536);
}

/// A record over the limit has nowhere else to go, but it goes alone (D91).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_over_the_frame_limit_goes_alone() {
    let lens = lingering_frames(1024, 1024, &[100, 2000, 100]).await;
    assert_eq!(lens, vec![136, 2032, 136]);
}

/// A reader can't see a version that a crash could still take back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribed_and_snapshot_show_no_state_the_wal_has_not_taken() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let first_doc = cbor(&map(&[("k", int(1))]));
    let first_ver = set_state(&mut client, STREAM, 1, first_doc.clone()).await;

    harness.wal.hold();
    client
        .state_op(StateOpRequest {
            stream: key(STREAM),
            req: 2,
            author: AUTHOR,
            expect_ver: 0,
            op: StateOp::Set,
            doc: cbor(&map(&[("k", int(2))])),
        })
        .await
        .unwrap();
    expect_quiet(&mut client).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut with_state = Subscription::new(1, key(STREAM), u64::MAX);
    with_state.with_state = true;
    with_state.include_system = true;
    connection.subscribe(with_state);
    let mut state_only = Subscription::new(2, key(STREAM), u64::MAX);
    state_only.state_only = true;
    connection.subscribe(state_only);
    let head = subscribed(&mut connection, &mut heard, 1).await;
    subscribed(&mut connection, &mut heard, 2).await;
    for event in &heard.events {
        if let Event::Subscribed {
            state_ver, state, ..
        } = event
        {
            assert_eq!(*state_ver, first_ver);
            assert_eq!(*state, first_doc);
        }
    }
    assert_eq!(head, first_ver, "the head goes with the state beside it");

    let snapshot = client.snapshot(key(STREAM)).await.unwrap();
    assert_eq!(snapshot.state_ver, first_ver);
    assert_eq!(snapshot.state, first_doc);
    assert_eq!(snapshot.head, first_ver);

    // Once it is durable it is seen, by both kinds of subscriber, and nothing was skipped:
    // state plus the log from `head` must add up to the current state.
    harness.wal.release();
    let Reply::StateResult { ok: true, ver, .. } = expect_reply(&mut client).await else {
        panic!("expected a state result");
    };
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty() && state_frame_count(heard) == 2
    })
    .await;
    assert_eq!(heard.records(1)[0].0, ver);
    let snapshot = client.snapshot(key(STREAM)).await.unwrap();
    assert_eq!(snapshot.state_ver, ver);
    drop((connection, client));
    harness.finish();
}

/// The queue bound is below the frame size by default, so a frame-sized trigger would never
/// fire and a lingering subscriber that is keeping up would overflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lingering_subscriber_that_is_reading_never_gets_behind() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.linger_ms = 10_000;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;

    let data = vec![5u8; 1024 * 1024];
    let record_count = 12u64;
    for seq in 1..=record_count {
        let records = record_with(1, &data);
        client
            .publish(publish(STREAM, &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        hear_for(&mut connection, &mut heard, 10).await;
    }
    assert_eq!(behind_count(&heard), 0);
    assert!(
        heard.records(1).len() >= 4,
        "the size trigger should have sent some before the 10 s timer"
    );
    drop((connection, client));
    harness.finish();
}

/// A batch that would overflow a lingering queue, with the window open, is sent rather than
/// treated as a slow client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_past_the_bound_with_the_window_open_is_not_behind() {
    let mut config = manual_config(1);
    config.subscriber_queue_bytes = 1024 * 1024;
    config.connection_window_bytes = 16 * 1024 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.linger_ms = 10_000;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;

    for (seq, size) in [(1, 400 * 1024), (2, 900 * 1024)] {
        let records = record_with(1, &vec![5u8; size]);
        client
            .publish(publish(STREAM, &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 2
    })
    .await;
    assert_eq!(behind_count(&heard), 0);
    drop((connection, client));
    harness.finish();
}
