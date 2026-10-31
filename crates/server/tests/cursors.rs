//! Named cursors and `COMMIT` (SPEC section 5.1).

mod common;

use common::*;
use proto::ErrorCode;
use server::{Connection, Subscription};

const STREAM: &str = "cursor.stream";

async fn publish_records(client: &mut server::Client, count: u64, first_seq: u64) -> Vec<u64> {
    let mut ids = Vec::new();
    for seq in first_seq..first_seq + count {
        let records = record_with(seq, b"record");
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        ids.push(expect_ack(client).await.1);
    }
    ids
}

fn named<'a>(sub: u32, name: &'a str, from: u64) -> Subscription<'a> {
    let mut subscription = Subscription::new(sub, STREAM, from);
    subscription.name = Some(name);
    subscription
}

async fn wait_for_records(connection: &mut Connection, heard: &mut Heard, sub: u32, count: usize) {
    hear_until(connection, heard, |heard| heard.records(sub).len() >= count).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursors_move_forward_only_and_only_a_reset_rewinds() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let ids = publish_records(&mut client, 6, 1).await;

    let mut connection = node.connection(1).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(named(1, "worker", 0));
    wait_for_records(&mut connection, &mut heard, 1, 6).await;

    connection.commit(1, ids[3]);
    assert_eq!(connection.read_cursor(STREAM, "worker").await, Some(ids[3]));
    // Lower: ignored, and no error.
    connection.commit(1, ids[1]);
    assert_eq!(connection.read_cursor(STREAM, "worker").await, Some(ids[3]));
    connection.commit(1, ids[4]);
    assert_eq!(connection.read_cursor(STREAM, "worker").await, Some(ids[4]));

    // A new subscription resumes after the cursor, whatever `from` says: at-least-once.
    connection.subscribe(named(2, "worker", 0));
    wait_for_records(&mut connection, &mut heard, 2, 1).await;
    assert_eq!(heard.records(2)[0].0, ids[5]);

    // Reset can go back.
    connection.reset_cursor(STREAM, "worker", ids[0]);
    assert_eq!(connection.read_cursor(STREAM, "worker").await, Some(ids[0]));
    connection.subscribe(named(3, "worker", u64::MAX));
    wait_for_records(&mut connection, &mut heard, 3, 5).await;
    let got: Vec<u64> = heard.records(3).iter().map(|record| record.0).collect();
    assert_eq!(got, ids[1..]);

    assert!(heard.error_code(1).is_none());
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_cursors_are_refused() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();

    connection.subscribe(named(1, "sink:7", 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
    })
    .await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::Malformed));

    connection.reset_cursor(STREAM, "sink:7", 5);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(0).is_some()
    })
    .await;
    assert_eq!(heard.error_code(0), Some(ErrorCode::Malformed));
    assert_eq!(connection.read_cursor(STREAM, "sink:7").await, None);
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_needs_a_named_cursor_and_an_id_that_exists() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let ids = publish_records(&mut client, 1, 1).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();

    connection.subscribe(Subscription::new(1, STREAM, u64::MAX));
    subscribed(&mut connection, &mut heard, 1).await;
    connection.commit(1, ids[0]);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
    })
    .await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::Malformed));

    connection.subscribe(named(2, "worker", 0));
    subscribed(&mut connection, &mut heard, 2).await;
    connection.commit(2, ids[0] + 1_000_000);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(2).is_some()
    })
    .await;
    assert_eq!(heard.error_code(2), Some(ErrorCode::Malformed));
    assert_ne!(
        connection.read_cursor(STREAM, "worker").await,
        Some(ids[0] + 1_000_000)
    );
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_moves_with_its_stream_and_can_commit_progress() {
    let mut config = manual_config(2);
    config.progress_interval_ms = 20;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let shard = server::shard_of(STREAM, node.virtual_shard_count());
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let rare = [format::Clause::Eq {
        mask: 0xFF,
        value: 0xFE,
    }];
    let mut subscription = named(1, "rare", u64::MAX);
    subscription.clauses = &rare;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;

    let ids = publish_records(&mut client, 3, 1).await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.progress(1).last() == Some(&ids[2])
    })
    .await;
    connection.commit(1, ids[2]);
    assert_eq!(connection.read_cursor(STREAM, "rare").await, Some(ids[2]));
    for core in 0..2 {
        assert!(node.move_shard(shard, core).await);
    }
    assert_eq!(connection.read_cursor(STREAM, "rare").await, Some(ids[2]));
    drop((connection, client));
    harness.finish();
}

/// A `read` grant can write cursors, so a stream's table and its names must have a limit, or
/// one such token can fill a node's memory, and a restart loads it all again (D218).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_cursors_are_limited_per_stream_and_by_name_length() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let ids = publish_records(&mut client, 2, 1).await;

    let mut connection = node.connection(1).await.unwrap();
    let mut heard = Heard::default();
    for at in 0..server::NAMED_CURSOR_CAP_COUNT {
        connection.reset_cursor(STREAM, &format!("c{at}"), ids[0]);
    }
    let last = format!("c{}", server::NAMED_CURSOR_CAP_COUNT - 1);
    assert_eq!(connection.read_cursor(STREAM, &last).await, Some(ids[0]));

    // Full: a new name is refused, by a subscribe or a reset, and nothing is stored.
    connection.subscribe(named(1, "by_subscribe", 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
    })
    .await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::TooManySubscriptions));
    assert_eq!(connection.read_cursor(STREAM, "by_subscribe").await, None);
    connection.reset_cursor(STREAM, "by_reset", ids[0]);
    assert_eq!(connection.read_cursor(STREAM, "by_reset").await, None);

    // A subscribe under a name it already has is fine.
    connection.subscribe(named(3, "c0", u64::MAX));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.events.iter().any(
            |event| matches!(event, server::Event::Subscribed { sub, .. } if *sub == 3),
        )
    })
    .await;

    // One already there still moves.
    connection.reset_cursor(STREAM, "c0", ids[1]);
    assert_eq!(connection.read_cursor(STREAM, "c0").await, Some(ids[1]));

    // A name over the limit is malformed, at a subscribe.
    let long = "n".repeat(server::CURSOR_NAME_MAX_BYTES + 1);
    connection.subscribe(named(2, &long, 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(2).is_some()
    })
    .await;
    assert_eq!(heard.error_code(2), Some(ErrorCode::Malformed));
    drop((connection, client));
    harness.finish();
}
