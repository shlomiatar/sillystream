//! What readers may not do to the rest of a node: stalled subscribers can't hold the memory
//! cap against publishers, catching up can't stop a core's writes, and unread `PROGRESS`
//! can't grow without limit. Also where retention's error 7 does and doesn't apply, moves
//! included (SPEC sections 5.1, 5.2, 7.4; D89, D99 to D102).

mod common;

use std::time::Duration;

use common::*;
use format::Clause;
use proto::ErrorCode;
use server::{Event, SealOutcome, Subscription};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const STREAM: &str = "pressure.stream";
const MS_PER_DAY: u64 = 24 * 60 * 60 * 1000;
/// Options no published record has.
const NEVER: [Clause; 1] = [Clause::Eq {
    mask: 1 << 40,
    value: 1 << 40,
}];
const PUBLISH_WAIT_MS: u64 = 5_000;

fn tight_config() -> server::NodeConfig {
    let mut config = manual_config(1);
    config.slab_bytes = 1024 * 1024;
    config.memory_cap_bytes = 32 * 1024 * 1024;
    config.subscriber_queue_bytes = 8 * 1024 * 1024;
    config.connection_window_bytes = 4 * 1024 * 1024;
    config
}

/// Publishes one large record and waits for its ack, sealing when memory runs short. A wait
/// past `PUBLISH_WAIT_MS` is what this file exists to catch.
async fn publish_large(node: &server::Node, client: &mut server::Client, seq: u64) -> u64 {
    if client.pinned_bytes() >= 24 * 1024 * 1024 {
        assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
        wait_for_memory_to_empty(node, STREAM).await;
    }
    let data = vec![0x5A; 64 * 1024];
    let records = record_with(1, &data);
    let sent = tokio::time::timeout(
        Duration::from_millis(PUBLISH_WAIT_MS),
        client.publish(publish(STREAM, &records, seq, 2)),
    )
    .await;
    assert!(
        sent.is_ok(),
        "publish {seq} still waiting with {} bytes pinned",
        client.pinned_bytes()
    );
    sent.unwrap().unwrap();
    expect_ack(client).await.1
}

/// Slabs pinned by one connection are mostly others' too, so it takes connections that
/// start at different points to add up.
async fn stalled_connection(node: &server::Node) -> server::Connection {
    let connection = node.connection(0).await.unwrap();
    connection.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    connection
}

/// The reported deadlock: enough stalled connections to pin the whole cap, with a reader
/// that keeps up alongside them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_subscribers_do_not_stop_publishers_or_a_reader_that_keeps_up() {
    let harness = Harness::new(tight_config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;

    let mut reader = node.connection(0).await.unwrap();
    reader.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    let mut heard = Heard::default();
    subscribed(&mut reader, &mut heard, 1).await;
    let reading = tokio::spawn(async move {
        let mut behind_count = 0;
        let mut bytes = 0;
        while let Some(event) = reader.recv().await {
            match event {
                Event::Behind { .. } => behind_count += 1,
                Event::Data { records, .. } => bytes += records.len_bytes(),
                _ => {}
            }
        }
        (behind_count, bytes)
    });

    let mut stalled = Vec::new();
    let mut seq = 0;
    for _ in 0..24 {
        stalled.push(stalled_connection(node).await);
        for _ in 0..64 {
            seq += 1;
            publish_large(node, &mut client, seq).await;
        }
    }
    assert!(
        total_stat(node, |stats| stats.stalled_release_count).await > 0,
        "nothing was released, so this tested nothing"
    );
    drop((client, stalled));
    let exit = harness.finish();
    assert!(exit.wal.is_ok());
    let (behind_count, bytes) = reading.await.unwrap();
    assert_eq!(behind_count, 0, "a reader that keeps up got BEHIND");
    assert!(bytes >= seq * 64 * 1024, "the reader got {bytes} bytes");
}

/// With unread frames taken back, the client must still get everything, in order, once it
/// wakes, and nothing may be claimed as skipped that it hasn't seen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_subscriber_that_wakes_misses_nothing() {
    let harness = Harness::new(tight_config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;

    let mut sleepy = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    sleepy.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    subscribed(&mut sleepy, &mut heard, 1).await;
    let mut others = Vec::new();

    let mut ids = Vec::new();
    for seq in 1..=1_536 {
        if seq % 64 == 0 {
            others.push(stalled_connection(node).await);
        }
        ids.push(publish_large(node, &mut client, seq).await);
    }
    assert!(total_stat(node, |stats| stats.stalled_release_count).await > 0);

    let last = *ids.last().unwrap();
    hear_until(&mut sleepy, &mut heard, |heard| {
        heard
            .records(1)
            .last()
            .is_some_and(|record| record.0 >= last)
    })
    .await;
    let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
    assert_eq!(got, ids);
    assert!(
        heard
            .events
            .iter()
            .any(|event| matches!(event, Event::Behind { .. }))
    );
    drop((client, sleepy, others));
    harness.finish();
}

/// `PROGRESS` a stalled client hadn't read must not outlive the frames it follows: it says
/// nothing matches up to its id, and a named cursor can commit that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn taking_back_frames_takes_back_the_progress_behind_them() {
    let mut config = tight_config();
    config.progress_interval_ms = 1;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;

    let mut sleepy = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    // Record 1 in 2 matches, and the others leave something for PROGRESS to report.
    let every_other = [Clause::Eq { mask: 1, value: 1 }];
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &every_other;
    sleepy.subscribe(subscription);
    subscribed(&mut sleepy, &mut heard, 1).await;
    let mut others = Vec::new();

    let data = vec![0x5A; 64 * 1024];
    let mut matching = Vec::new();
    for seq in 1..=1_536u64 {
        if seq % 64 == 0 {
            others.push(stalled_connection(node).await);
        }
        if client.pinned_bytes() >= 24 * 1024 * 1024 {
            assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
            wait_for_memory_to_empty(node, STREAM).await;
        }
        let records = record_with(seq % 2, &data);
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        let id = expect_ack(&mut client).await.1;
        if seq % 2 == 1 {
            matching.push(id);
        }
        // Lets the 1 ms PROGRESS timer see the skipped ones.
        tokio::task::yield_now().await;
    }
    assert!(total_stat(node, |stats| stats.stalled_release_count).await > 0);

    let last = *matching.last().unwrap();
    let mut seen = Vec::new();
    let give_up = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while seen.last().is_none_or(|id| *id < last) {
        assert!(std::time::Instant::now() < give_up, "gave up at {seen:?}");
        match sleepy.recv_within(100).await {
            Some(Event::Data { records, .. }) => {
                seen.extend(parse_records(&records.to_vec()).iter().map(|r| r.0));
            }
            Some(Event::Progress { id, .. }) => {
                let next = matching[seen.len()..].first();
                assert!(
                    next.is_none_or(|next| id < *next),
                    "PROGRESS {id} claims past undelivered record {next:?}"
                );
            }
            _ => {}
        }
    }
    assert_eq!(seen, matching);
    drop((client, sleepy, others));
    harness.finish();
}

/// A subscriber going through what is in memory must not keep the core from writes. Whether
/// a write got through is read from how many catch-ups had finished by then, not from a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catching_up_does_not_hold_up_writes() {
    const SUB_COUNT: u32 = 400;
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut records = Vec::new();
    for _ in 0..200 {
        add_record(&mut records, 0, &[0x5A; 32], 1);
    }
    for seq in 1..=400 {
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }

    let mut connection = node.connection(0).await.unwrap();
    for sub in 0..SUB_COUNT {
        let mut subscription = Subscription::new(sub, key(STREAM), 0);
        subscription.clauses = &NEVER;
        connection.subscribe(subscription);
    }
    for (seq, level) in [(1_000, 1), (1_001, 2)] {
        let one = record_with(0, b"a write among catch-ups");
        client
            .publish(publish(STREAM, &one, seq, level))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        // Each sends one `PROGRESS` after its first read, and one on reaching the head.
        let sent = total_stat(node, |stats| stats.progress_count).await;
        assert!(
            sent < 2 * u64::from(SUB_COUNT),
            "the level {level} ack waited for every catch-up: {sent} PROGRESS frames"
        );
    }

    // And they still all finish.
    let mut heard = Heard::default();
    hear_until(&mut connection, &mut heard, |heard| {
        (0..SUB_COUNT).all(|sub| !heard.progress(sub).is_empty())
    })
    .await;
    drop((client, connection));
    harness.finish();
}

async fn publish_small(client: &mut server::Client, seq: u64) -> u64 {
    let records = record_with(1, b"a record");
    client
        .publish(publish(STREAM, &records, seq, 2))
        .await
        .unwrap();
    expect_ack(client).await.1
}

async fn subscribe_and_hear(
    node: &server::Node,
    from: u64,
    last: u64,
) -> (Option<ErrorCode>, Vec<u64>) {
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(STREAM), from));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
            || heard
                .records(1)
                .last()
                .is_some_and(|record| record.0 >= last)
    })
    .await;
    let ids = heard.records(1).iter().map(|record| record.0).collect();
    (heard.error_code(1), ids)
}

/// Records left unsealed on a stream's last core count as much as the owner's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_id_is_error_7_with_its_records_on_a_previous_owner() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let old_id = publish_small(&mut client, 1).await;
    publish_small(&mut client, 2).await;
    let shard = server::shard_of(key(STREAM), node.virtual_shard_count());
    let from_core = node.placement().await[shard as usize] as usize;
    assert!(node.move_shard(shard, 1 - from_core).await);
    harness.clock.advance(2 * MS_PER_DAY);
    let new_id = publish_small(&mut client, 3).await;

    let (error, ids) = subscribe_and_hear(node, old_id - 1, new_id).await;
    assert_eq!(error, Some(ErrorCode::RetentionExpired));
    assert!(ids.is_empty(), "expired records were delivered: {ids:?}");
    drop(client);
    harness.finish();
}

/// The same with the previous owner's stretch ending after the start of retention, so only
/// asking that core can show what is at the old end of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_id_is_error_7_inside_a_previous_owners_later_stretch() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let old_id = publish_small(&mut client, 1).await;
    harness.clock.advance(2 * MS_PER_DAY);
    let newer_id = publish_small(&mut client, 2).await;
    let shard = server::shard_of(key(STREAM), node.virtual_shard_count());
    let from_core = node.placement().await[shard as usize] as usize;
    assert!(node.move_shard(shard, 1 - from_core).await);
    publish_small(&mut client, 3).await;

    let (error, ids) = subscribe_and_hear(node, old_id - 1, newer_id).await;
    assert_eq!(error, Some(ErrorCode::RetentionExpired));
    assert!(ids.is_empty(), "expired records were delivered: {ids:?}");
    drop(client);
    harness.finish();
}

/// And one inside retention isn't refused for what a previous owner holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_in_retention_is_served_across_a_move() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    publish_small(&mut client, 1).await;
    harness.clock.advance(2 * MS_PER_DAY);
    let kept_id = publish_small(&mut client, 2).await;
    let last_there = publish_small(&mut client, 3).await;
    let shard = server::shard_of(key(STREAM), node.virtual_shard_count());
    let from_core = node.placement().await[shard as usize] as usize;
    assert!(node.move_shard(shard, 1 - from_core).await);
    let new_id = publish_small(&mut client, 4).await;

    let (error, ids) = subscribe_and_hear(node, kept_id, new_id).await;
    assert_eq!(error, None);
    assert_eq!(ids, vec![last_there, new_id]);
    drop(client);
    harness.finish();
}

/// Three records in one extent, ten seconds apart, with `first_id` before retention's start.
async fn sealed_across_the_start(harness: &Harness) -> (server::Client, [u64; 3]) {
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut ids = [0; 3];
    for (at, seq) in (1..=3).enumerate() {
        ids[at] = publish_small(&mut client, seq).await;
        harness.clock.advance(10_000);
    }
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    // 30 s after the first record, so the start is 5 s after it: only `ids[0]` is before.
    harness.clock.advance(MS_PER_DAY - 25_000);
    (client, ids)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_in_retention_is_served_when_its_extent_began_before() {
    let harness = Harness::new(manual_config(1));
    let (client, ids) = sealed_across_the_start(&harness).await;
    let (error, got) = subscribe_and_hear(harness.node(), ids[1], ids[2]).await;
    assert_eq!(error, None);
    assert_eq!(got, vec![ids[2]]);
    drop(client);
    harness.finish();
}

/// `ids[0]` itself is before the start, but what follows it isn't, so nothing is missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_before_retention_with_nothing_expired_after_it_is_served() {
    let harness = Harness::new(manual_config(1));
    let (client, ids) = sealed_across_the_start(&harness).await;
    let (error, got) = subscribe_and_hear(harness.node(), ids[0], ids[2]).await;
    assert_eq!(error, None);
    assert_eq!(got, vec![ids[1], ids[2]]);
    drop(client);
    harness.finish();
}

/// A client not reading stops getting `PROGRESS` once its window is used up, however long
/// records go by, and gets the rest when it reads again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unread_progress_is_bounded_by_the_window() {
    const PUBLISH_COUNT: u64 = 400;
    /// `type u8 | sub u32 | id u64`.
    const PROGRESS_FRAME_BYTES: u64 = 13;
    let mut config = manual_config(1);
    config.progress_interval_ms = 1;
    config.connection_window_bytes = 1024;
    config.subscriber_queue_bytes = 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &NEVER;
    connection.subscribe(subscription);
    let mut heard = Heard::default();
    subscribed(&mut connection, &mut heard, 1).await;

    let mut last = 0;
    for seq in 1..=PUBLISH_COUNT {
        last = publish_small(&mut client, seq).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let limit = 1024 / PROGRESS_FRAME_BYTES + 1;
    let sent = total_stat(node, |stats| stats.progress_count).await;
    assert!(
        sent > 0 && sent <= limit,
        "{sent} PROGRESS frames went to a client not reading, over {limit}"
    );

    // Reading returns the window, and the position comes through.
    hear_until(&mut connection, &mut heard, |heard| {
        heard.progress(1).last().is_some_and(|id| *id >= last)
    })
    .await;
    drop((client, connection));
    harness.finish();
}
