//! A stream moving between cores keeps its order and loses nothing (SPEC sections 6.1, 6.4).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use server::{Reply, SealOutcome, shard_of};
use storage::SegmentReader;

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

/// Ids that go up, with none missing, against the acks.
fn check_ids(records: &[(u64, u64, u64, Vec<u8>)], acks: &[(u64, u64, u32)]) {
    let ids: Vec<u64> = records.iter().map(|record| record.0).collect();
    let acked: Vec<u64> = acks.iter().map(|ack| ack.1).collect();
    assert_eq!(ids, acked);
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn a_stream_moved_mid_load_keeps_order_and_acks_and_seals_as_one_extent() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let shard = shard_of(key("moving"), node.virtual_shard_count());
    let from = node.placement().await[shard as usize] as usize;
    let to = 1 - from;

    // A level 2 publish pending across the move: its ack is the old core's to send.
    harness.wal.hold();
    client
        .publish(publish("moving", &records, 1, 2))
        .await
        .unwrap();
    expect_quiet(&mut client).await;
    let mut publisher_seq = 2;
    for _ in 0..50 {
        client
            .publish(publish("moving", &records, publisher_seq, 2))
            .await
            .unwrap();
        publisher_seq += 1;
    }
    node.move_shard(shard, to).await;
    assert_eq!(node.placement().await[shard as usize] as usize, to);
    // The client still sends to the old core, which passes it on.
    for _ in 0..50 {
        client
            .publish(publish("moving", &records, publisher_seq, 2))
            .await
            .unwrap();
        publisher_seq += 1;
    }
    expect_quiet(&mut client).await;
    harness.wal.release();

    let mut acks = Vec::new();
    while (acks.len() as u64) < publisher_seq - 1 {
        acks.push(expect_ack(&mut client).await);
    }
    // Acks come per core, so order them before comparing.
    acks.sort();
    assert_eq!(
        acks.iter().map(|ack| ack.0).collect::<Vec<_>>(),
        (1..publisher_seq).collect::<Vec<_>>()
    );
    assert!(acks.windows(2).all(|pair| pair[0].1 < pair[1].1));
    assert!(node.stats(from).await.unwrap().forwarded_count > 0);

    // The WAL has each once.
    let entries = harness.wal.stream_entries("moving");
    assert_eq!(entries.len() as u64, publisher_seq - 1);

    // The seal is the WAL file, which has the stream in id order whichever core wrote it, so it
    // is one extent with all of it (D268).
    let (path, extents) = match node.seal().await {
        SealOutcome::Uploaded { path, extents, .. } => (path, extents),
        other => panic!("{other:?}"),
    };
    let own: Vec<_> = extents
        .iter()
        .filter(|extent| extent.stream == key("moving"))
        .collect();
    assert_eq!(own.len(), 1, "{extents:?}");
    let store: Arc<dyn object_store::ObjectStore> = harness.store.clone();
    let reader = SegmentReader::new(store, path);
    let top = reader.read_top_index().await.unwrap();
    let in_footer = reader.all_extents(&top).await.unwrap();
    assert_eq!(
        in_footer
            .iter()
            .filter(|extent| extent.stream == key("moving"))
            .count(),
        1
    );
    let stored = reader.read_stream(key("moving")).await.unwrap();
    check_ids(&parse_records(&stored), &acks);
    harness.finish();
}

#[tokio::test]
async fn state_dedupe_and_config_move_with_the_stream() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let mut request = publish("s", &records, 7, 1);
    request.state = Some(server::StateChange {
        expect_ver: 0,
        op: server::StateOp::Set,
        doc: cbor(&int(5)),
    });
    client.publish(request.clone()).await.unwrap();
    let original = expect_ack(&mut client).await;
    let before = client.snapshot(key("s")).await.unwrap();

    let shard = shard_of(key("s"), node.virtual_shard_count());
    let from = node.placement().await[shard as usize] as usize;
    node.move_shard(shard, 1 - from).await;

    // A snapshot follows the stream even from a stale route.
    assert_eq!(client.snapshot(key("s")).await.unwrap(), before);
    // The retry is recognised on the new core, and so doesn't commit or re-check state.
    request.ack_level = Some(1);
    client.publish(request.clone()).await.unwrap();
    assert_eq!(expect_ack(&mut client).await, original);
    assert_eq!(node.stats(1 - from).await.unwrap().stream_count, 1);
    assert_eq!(node.stats(from).await.unwrap().stream_count, 0);
    assert_eq!(node.stats(1 - from).await.unwrap().duplicate_count, 1);
    // Ids carry on above what the old core gave out, with the clock not moving.
    client.publish(publish("s", &records, 8, 1)).await.unwrap();
    let (_, next_id, _) = expect_ack(&mut client).await;
    assert_eq!(next_id, before.head + 1);
    harness.finish();
}

#[tokio::test]
async fn a_retry_after_a_move_waits_for_the_core_that_stamped_it() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let shard = shard_of(key("s"), node.virtual_shard_count());
    let from = node.placement().await[shard as usize] as usize;

    harness.wal.hold();
    client.publish(publish("s", &records, 1, 2)).await.unwrap();
    expect_quiet(&mut client).await;
    node.move_shard(shard, 1 - from).await;
    client.publish(publish("s", &records, 1, 2)).await.unwrap();
    expect_quiet(&mut client).await;
    harness.wal.release();
    let a = expect_ack(&mut client).await;
    let b = expect_ack(&mut client).await;
    assert_eq!(a, b);
    assert_eq!(harness.wal.stream_entries("s").len(), 1);

    // At level 3 too: uploaded only when the old core's section has been.
    client.publish(publish("s", &records, 2, 1)).await.unwrap();
    expect_ack(&mut client).await;
    node.move_shard(shard, from).await;
    client.publish(publish("s", &records, 2, 3)).await.unwrap();
    expect_quiet(&mut client).await;
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    expect_ack(&mut client).await;
    harness.finish();
}

#[tokio::test]
async fn moving_there_and_back_in_one_window_never_overlaps_extents() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let shard = shard_of(key("s"), node.virtual_shard_count());
    let home = node.placement().await[shard as usize] as usize;
    let mut seq = 1;
    let mut acks = Vec::new();
    for round in 0..4 {
        for _ in 0..5 {
            client
                .publish(publish("s", &records, seq, 2))
                .await
                .unwrap();
            acks.push(expect_ack(&mut client).await);
            seq += 1;
        }
        let to = if round % 2 == 0 { 1 - home } else { home };
        node.move_shard(shard, to).await;
    }
    let mut seen = Vec::new();
    // Each seal takes one stay per core, so a few seals clear all of them.
    for _ in 0..4 {
        match node.seal().await {
            SealOutcome::Uploaded { path, extents, .. } => {
                let mut own: Vec<_> = extents
                    .iter()
                    .filter(|extent| extent.stream == key("s"))
                    .collect();
                own.sort_by_key(|extent| extent.id_first);
                assert!(
                    own.windows(2)
                        .all(|pair| pair[0].id_last < pair[1].id_first),
                    "{own:?}"
                );
                let store: Arc<dyn object_store::ObjectStore> = harness.store.clone();
                let stored = SegmentReader::new(store, path)
                    .read_stream(key("s"))
                    .await
                    .unwrap();
                seen.extend(parse_records(&stored));
            }
            SealOutcome::Empty { .. } => break,
            other => panic!("{other:?}"),
        }
    }
    seen.sort_by_key(|record| record.0);
    check_ids(&seen, &acks);
    harness.finish();
}

#[tokio::test]
async fn a_client_learns_the_new_route_and_keeps_order_doing_so() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let shard = shard_of(key("s"), node.virtual_shard_count());
    let from = node.placement().await[shard as usize] as usize;
    let to = 1 - from;
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    expect_ack(&mut client).await;
    assert_eq!(client.route_of(key("s")), from);
    node.move_shard(shard, to).await;

    // Pipelined across the route change: if any overtook another, the older one would come
    // back with first_id = 0, as a duplicate.
    let mut seq = 2;
    let mut acks = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while client.route_of(key("s")) != to {
        assert!(
            std::time::Instant::now() < deadline,
            "the route never changed"
        );
        client
            .publish(publish("s", &records, seq, 1))
            .await
            .unwrap();
        seq += 1;
        acks.push(expect_ack(&mut client).await);
    }
    for _ in 0..200 {
        client
            .publish(publish("s", &records, seq, 1))
            .await
            .unwrap();
        seq += 1;
    }
    while (acks.len() as u64) < seq - 2 {
        acks.push(expect_ack(&mut client).await);
    }
    assert!(acks.iter().all(|ack| ack.1 > 0), "a publish was overtaken");
    assert!(
        acks.windows(2)
            .all(|pair| pair[0].0 < pair[1].0 && pair[0].1 < pair[1].1)
    );
    // After the switch, nothing more is forwarded.
    let forwarded = node.stats(from).await.unwrap().forwarded_count;
    client
        .publish(publish("s", &records, seq, 1))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    assert_eq!(node.stats(from).await.unwrap().forwarded_count, forwarded);
    harness.finish();
}

#[tokio::test]
async fn publishers_racing_many_moves_lose_nothing() {
    let cores = 4;
    let harness = Harness::new(manual_config(cores));
    let node = harness.node();
    let streams = ["m0", "m1", "m2", "m3", "m4", "m5"];
    let records = record_with(1, b"payload");
    let mut clients = Vec::new();
    for _ in 0..streams.len() {
        clients.push(node.client(0).await);
    }
    let mut seqs = vec![1u64; streams.len()];
    let mut acks: Vec<Vec<(u64, u64, u32)>> = vec![Vec::new(); streams.len()];
    for round in 0..40u64 {
        for (index, stream) in streams.iter().enumerate() {
            clients[index]
                .publish(publish(stream, &records, seqs[index], 1))
                .await
                .unwrap();
            seqs[index] += 1;
            let shard = shard_of(key(stream), node.virtual_shard_count());
            if round % 3 == index as u64 % 3 {
                node.move_shard(shard, (round as usize + index) % cores)
                    .await;
            }
        }
    }
    for (index, client) in clients.iter_mut().enumerate() {
        while (acks[index].len() as u64) < seqs[index] - 1 {
            match expect_reply(client).await {
                Reply::Ack {
                    producer_seq,
                    first_id,
                    count,
                    ..
                } => acks[index].push((producer_seq, first_id, count)),
                other => panic!("{other:?}"),
            }
        }
        assert!(acks[index].iter().all(|ack| ack.1 > 0));
        assert!(
            acks[index]
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0 && pair[0].1 < pair[1].1)
        );
    }
    // Level 2 on each, to know all of the above is in the WAL by now.
    for (index, stream) in streams.iter().enumerate() {
        clients[index]
            .publish(publish(stream, &records, seqs[index], 2))
            .await
            .unwrap();
        expect_ack(&mut clients[index]).await;
        let entries = harness.wal.stream_entries(stream);
        assert_eq!(entries.len() as u64, seqs[index]);
    }
    harness.finish();
}

/// A stream whose hashed shard starts on `core`, as placement is `shard % core_count`.
fn name_on_core(core: usize, node: &server::Node, prefix: &str) -> String {
    (0..)
        .map(|n| format!("{prefix}{n}"))
        .find(|name| {
            shard_of(key(name), node.virtual_shard_count()) as usize % node.core_count() == core
        })
        .expect("names go on for ever")
}

/// Publishes to `stream` until `core` has a stamped entry it can't get into the WAL
/// channel, so that it stops reading writes. The WAL has to be held.
async fn park_outbound(node: &server::Node, core: usize, stream: &str, records: &[u8]) {
    let mut client = node.client(core).await;
    let mut seq = 1;
    while node.stats(core).await.unwrap().outbound_count == 0 {
        assert!(seq < 100, "the channel should have filled by now");
        client
            .publish(publish(stream, records, seq, 1))
            .await
            .unwrap();
        seq += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn stalled_two_cores() -> server::NodeConfig {
    let mut config = manual_config(2);
    config.wal_channel_capacity = 2;
    config
}

#[tokio::test]
async fn a_publish_sent_while_the_barrier_is_out_cant_be_overtaken() {
    let harness = Harness::new(stalled_two_cores());
    let node = harness.node();
    let name = name_on_core(0, node, "m");
    let shard = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");
    let mut client = node.client(0).await;

    harness.wal.hold();
    node.move_shard(shard, 1).await;
    client
        .publish(publish(&name, &records, 1, 1))
        .await
        .unwrap();
    // This takes the `Moved` too, so the barrier goes down the old route.
    assert_eq!(expect_ack(&mut client).await.0, 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    park_outbound(node, 0, &name_on_core(0, node, "other"), &records).await;

    // Stuck in core 0's inbox, if it goes that way, until the WAL moves.
    client
        .publish(publish(&name, &records, 2, 1))
        .await
        .unwrap();
    let mut acks = Vec::new();
    if let Some(Reply::Ack {
        producer_seq,
        first_id,
        count,
        ..
    }) = client.recv_within(30).await
    {
        acks.push((producer_seq, first_id, count));
    }
    assert_eq!(client.route_of(key(&name)), 1);
    client
        .publish(publish(&name, &records, 3, 1))
        .await
        .unwrap();
    while acks.len() < 2 {
        if acks.len() == 1 && acks[0].0 == 3 {
            harness.wal.release();
        }
        match client.recv_within(100).await {
            Some(Reply::Ack {
                producer_seq,
                first_id,
                count,
                ..
            }) => acks.push((producer_seq, first_id, count)),
            Some(other) => panic!("{other:?}"),
            None => harness.wal.release(),
        }
    }
    harness.wal.release();
    assert!(acks.iter().all(|ack| ack.1 > 0), "{acks:?}");
    client
        .publish(publish(&name, &records, 4, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    let seqs: Vec<u64> = harness
        .wal
        .stream_entries(&name)
        .iter()
        .map(|entry| entry.producer_seq)
        .collect();
    assert_eq!(seqs, [1, 2, 3, 4]);
    harness.finish();
}

#[tokio::test]
async fn a_write_waits_for_a_barrier_still_on_its_way() {
    let harness = Harness::new(stalled_two_cores());
    let node = harness.node();
    let name = name_on_core(0, node, "m");
    let shard = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");
    let mut client = node.client(0).await;

    harness.wal.hold();
    node.move_shard(shard, 1).await;
    client
        .publish(publish(&name, &records, 1, 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    park_outbound(node, 0, &name_on_core(0, node, "other"), &records).await;
    // The barrier goes out only now, into an inbox that isn't being read.
    assert_eq!(expect_ack(&mut client).await.0, 1);

    // Sent down the old route, it wouldn't be covered by that barrier.
    let waited = tokio::time::timeout(
        Duration::from_millis(50),
        client.publish(publish(&name, &records, 2, 1)),
    )
    .await;
    assert!(waited.is_err(), "the publish went ahead: {waited:?}");
    assert_eq!(
        client.try_publish(publish(&name, &records, 2, 1)),
        Err(server::ClientError::Full)
    );
    let state_op = tokio::time::timeout(
        Duration::from_millis(50),
        client.state_op(server::StateOpRequest {
            stream: key(&name),
            req: 1,
            author: AUTHOR,
            expect_ver: 0,
            op: server::StateOp::Set,
            doc: cbor(&int(1)),
        }),
    )
    .await;
    assert!(state_op.is_err(), "the state op went ahead");

    harness.wal.release();
    client
        .publish(publish(&name, &records, 2, 1))
        .await
        .unwrap();
    assert_eq!(client.route_of(key(&name)), 1);
    let (seq, first_id, _) = expect_ack(&mut client).await;
    assert_eq!(seq, 2);
    assert!(first_id > 0);
    harness.finish();
}

#[tokio::test]
async fn a_write_waiting_for_a_barrier_ends_when_the_node_stops() {
    let harness = Harness::new(stalled_two_cores());
    let node = harness.node();
    let name = name_on_core(0, node, "m");
    let shard = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");
    let mut client = node.client(0).await;

    harness.wal.hold();
    node.move_shard(shard, 1).await;
    client
        .publish(publish(&name, &records, 1, 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    park_outbound(node, 0, &name_on_core(0, node, "other"), &records).await;
    assert_eq!(expect_ack(&mut client).await.0, 1);

    harness.finish();
    let ended = tokio::time::timeout(
        Duration::from_secs(3),
        client.publish(publish(&name, &records, 2, 1)),
    )
    .await;
    assert_eq!(ended, Ok(Err(server::ClientError::NodeStopped)));
}

/// Sleeps once, on one thread, the next time that thread changes a slab's count. So
/// `read_memory` can make a core miss its retries for as long as a test needs.
struct StallOnce {
    thread_name: String,
    armed: std::sync::atomic::AtomicBool,
}

impl server::SlabHook for StallOnce {
    fn ref_changed(&self, _slab_id: u64, _delta: i32) {
        if std::thread::current().name() == Some(self.thread_name.as_str())
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    fn slab_freed(&self, _slab_id: u64) {}
}

async fn wait_for_parked(node: &server::Node, core: usize) {
    let deadline = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while node.stats(core).await.unwrap().outbound_count == 0 {
        assert!(std::time::Instant::now() < deadline, "nothing parked");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// `first_id` of each WAL entry for the stream, which have to go up.
fn assert_wal_in_id_order(harness: &Harness, stream: &str) -> Vec<u64> {
    let ids: Vec<u64> = harness
        .wal
        .stream_entries(stream)
        .iter()
        .map(|entry| parse_records(&entry.records)[0].0)
        .collect();
    assert!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "WAL is out of id order: {ids:?}"
    );
    ids
}

fn set_op(stream: &str, req: u32, value: i64) -> server::StateOpRequest {
    server::StateOpRequest {
        stream: key(stream),
        req,
        author: AUTHOR,
        expect_ver: 0,
        op: server::StateOp::Set,
        doc: cbor(&int(value)),
    }
}

fn replay_state(harness: &Harness, stream: &str) -> server::StreamState {
    let mut replayed = server::StreamState::new();
    for entry in harness.wal.stream_entries(stream) {
        for view in format::Records::new(&entry.records) {
            let view = view.unwrap();
            if view.header.is_system() {
                replayed.replay(&view).unwrap();
            }
        }
    }
    replayed
}

/// The old core misses the slot the WAL frees, and the new core is there to take it.
#[tokio::test]
async fn a_move_with_a_parked_entry_keeps_the_wal_in_id_order_and_replay_equal_to_live() {
    let hook = Arc::new(StallOnce {
        thread_name: "core-0".to_owned(),
        armed: std::sync::atomic::AtomicBool::new(false),
    });
    let mut config = manual_config(2);
    config.wal_channel_capacity = 1;
    config.slab_hook = Some(hook.clone());
    let harness = Harness::new(config);
    let node = harness.node();
    let name = name_on_core(0, node, "m");
    let shard = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");
    let mut client = node.client(0).await;

    harness.wal.hold();
    client
        .publish(publish(&name, &records, 1, 2))
        .await
        .unwrap();
    // Once the writer is in its held sync, the next fills the channel and the last parks.
    // If it took the first two as one group, nothing would park at all.
    let deadline = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while harness.wal.stream_entries(&name).is_empty() {
        assert!(std::time::Instant::now() < deadline, "nothing written");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    client
        .publish(publish(&name, &records, 2, 2))
        .await
        .unwrap();
    let mut last = publish(&name, &records, 3, 2);
    last.state = Some(server::StateChange {
        expect_ver: 0,
        op: server::StateOp::Set,
        doc: cbor(&int(1)),
    });
    client.publish(last).await.unwrap();
    wait_for_parked(node, 0).await;

    node.move_shard(shard, 1).await;
    let mut moved = node.client(1).await;
    moved.state_op(set_op(&name, 1, 2)).await.unwrap();
    // Core 1's own wake-up comes from another of its entries becoming durable.
    let mut neighbour = node.client(1).await;
    neighbour
        .publish(publish(&name_on_core(1, node, "n"), &records, 1, 2))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    hook.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    node.read_memory(0, key(&name)).await;
    harness.wal.release();

    let ver = match expect_reply(&mut moved).await {
        Reply::StateResult { ok: true, ver, .. } => ver,
        other => panic!("{other:?}"),
    };
    for _ in 0..3 {
        expect_ack(&mut client).await;
    }
    let ids = assert_wal_in_id_order(&harness, &name);
    assert_eq!(ids.len(), 4);
    let live = client.snapshot(key(&name)).await.unwrap();
    assert_eq!(live.state_ver, ver);
    let replayed = replay_state(&harness, &name);
    assert_eq!(replayed.doc(), &live.state);
    assert_eq!(replayed.ver(), live.state_ver);
    harness.finish();
}

#[tokio::test]
async fn a_parked_entry_keeps_its_place_across_many_moves() {
    for (cores, path) in [(3, vec![1, 2]), (2, vec![1, 0]), (3, vec![1, 0, 2])] {
        let mut config = manual_config(cores);
        config.wal_channel_capacity = 1;
        let harness = Harness::new(config);
        let node = harness.node();
        let name = name_on_core(0, node, "m");
        let shard = shard_of(key(&name), node.virtual_shard_count());
        let records = record_with(1, b"payload");
        let mut client = node.client(0).await;

        harness.wal.hold();
        let mut seq = 1;
        while node.stats(0).await.unwrap().outbound_count == 0 {
            client
                .publish(publish(&name, &records, seq, 1))
                .await
                .unwrap();
            seq += 1;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for to in &path {
            node.move_shard(shard, *to).await;
            // At the first stop, a write that is stamped there and has to wait its turn.
            if *to == path[0] {
                let mut there = node.client(*to).await;
                there
                    .publish(publish(&name, &records, seq, 1))
                    .await
                    .unwrap();
                seq += 1;
                expect_ack(&mut there).await;
            }
        }
        let mut last = node.client(*path.last().unwrap()).await;
        harness.wal.release();
        last.publish(publish(&name, &records, seq, 2))
            .await
            .unwrap();
        expect_ack(&mut last).await;
        assert_wal_in_id_order(&harness, &name);
        assert_eq!(harness.wal.stream_entries(&name).len() as u64, seq);
        harness.finish();
    }
}

/// Two cores each waiting on the other's stream mustn't wait for ever.
#[tokio::test]
async fn parked_streams_swapping_cores_all_get_through() {
    let mut config = manual_config(2);
    config.wal_channel_capacity = 1;
    let harness = Harness::new(config);
    let node = harness.node();
    let on_zero = name_on_core(0, node, "x");
    let on_one = name_on_core(1, node, "y");
    let records = record_with(1, b"payload");
    let mut zero = node.client(0).await;
    let mut one = node.client(1).await;

    harness.wal.hold();
    let mut seq = 1;
    while node.stats(0).await.unwrap().outbound_count == 0
        || node.stats(1).await.unwrap().outbound_count == 0
    {
        assert!(seq < 100, "both should have parked by now");
        zero.publish(publish(&on_zero, &records, seq, 1))
            .await
            .unwrap();
        one.publish(publish(&on_one, &records, seq, 1))
            .await
            .unwrap();
        seq += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    node.move_shard(shard_of(key(&on_zero), node.virtual_shard_count()), 1)
        .await;
    node.move_shard(shard_of(key(&on_one), node.virtual_shard_count()), 0)
        .await;
    harness.wal.release();

    let mut after_zero = node.client(1).await;
    let mut after_one = node.client(0).await;
    after_zero
        .publish(publish(&on_zero, &records, 1000, 2))
        .await
        .unwrap();
    after_one
        .publish(publish(&on_one, &records, 1000, 2))
        .await
        .unwrap();
    expect_ack(&mut after_zero).await;
    expect_ack(&mut after_one).await;
    assert_wal_in_id_order(&harness, &on_zero);
    assert_wal_in_id_order(&harness, &on_one);
    harness.finish();
}

fn one_slot_wal() -> server::NodeConfig {
    let mut config = manual_config(2);
    config.wal_channel_capacity = 1;
    config
}

/// Moves the stream to core 1 and leaves core 1 unable to read writes, with the WAL held.
async fn own_on_stalled_core_1(harness: &Harness, name: &str) {
    let node = harness.node();
    node.move_shard(shard_of(key(name), node.virtual_shard_count()), 1)
        .await;
    harness.wal.hold();
    let filler = record_with(1, b"filler");
    park_outbound(node, 1, &name_on_core(1, node, "filler"), &filler).await;
}

/// Waits for core 0 to pass something on, then hands the stream back to it.
async fn move_back_behind_the_forward(harness: &Harness, name: &str, forwarded_before: u64) {
    let node = harness.node();
    let deadline = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while node.stats(0).await.unwrap().forwarded_count == forwarded_before {
        assert!(std::time::Instant::now() < deadline, "nothing forwarded");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    node.move_shard(shard_of(key(name), node.virtual_shard_count()), 0)
        .await;
}

async fn a_forward_stays_ahead_after_its_stream_moves_back(hear_moved_first: bool) {
    let harness = Harness::new(one_slot_wal());
    let node = harness.node();
    let name = name_on_core(0, node, "back");
    let records = record_with(1, b"payload");
    let mut client = node.client(0).await;
    client
        .publish(publish(&name, &records, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;

    own_on_stalled_core_1(&harness, &name).await;
    let forwarded = node.stats(0).await.unwrap().forwarded_count;
    client
        .publish(publish(&name, &records, 2, 1))
        .await
        .unwrap();
    move_back_behind_the_forward(&harness, &name, forwarded).await;
    assert_eq!(client.route_of(key(&name)), 0);

    let mut acks = Vec::new();
    if hear_moved_first
        && let Some(Reply::Ack {
            producer_seq,
            first_id,
            count,
            ..
        }) = client.recv_within(100).await
    {
        acks.push((producer_seq, first_id, count));
    }
    // A barrier that went out waits on a core with a full queue, and seq 3 waits for it.
    if hear_moved_first {
        harness.wal.release();
    }
    client
        .publish(publish(&name, &records, 3, 1))
        .await
        .unwrap();
    harness.wal.release();
    while acks.len() < 2 {
        acks.push(expect_ack(&mut client).await);
    }
    assert!(acks.iter().all(|ack| ack.1 > 0), "{acks:?}");
    acks.sort();
    assert!(acks[0].1 < acks[1].1, "{acks:?}");

    client
        .publish(publish(&name, &records, 4, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    let seqs: Vec<u64> = harness
        .wal
        .stream_entries(&name)
        .iter()
        .map(|entry| entry.producer_seq)
        .collect();
    assert_eq!(seqs, [1, 2, 3, 4]);
    assert_wal_in_id_order(&harness, &name);
    harness.finish();
}

#[tokio::test]
async fn a_forward_stays_ahead_after_a_move_back_with_no_barrier_out() {
    a_forward_stays_ahead_after_its_stream_moves_back(false).await;
}

#[tokio::test]
async fn a_forward_stays_ahead_after_a_move_back_with_a_barrier_answered() {
    a_forward_stays_ahead_after_its_stream_moves_back(true).await;
}

/// With no producer id there is no dedupe to drop it, so this would commit out of order.
#[tokio::test]
async fn publishes_with_no_producer_id_keep_their_order_across_a_move_back() {
    let harness = Harness::new(one_slot_wal());
    let node = harness.node();
    let name = name_on_core(0, node, "back");
    let first = record_with(1, b"first");
    let second = record_with(1, b"second");
    let mut client = node.client(0).await;
    own_on_stalled_core_1(&harness, &name).await;
    let forwarded = node.stats(0).await.unwrap().forwarded_count;
    let mut request = publish(&name, &first, 0, 2);
    request.producer_id = 0;
    client.publish(request).await.unwrap();
    move_back_behind_the_forward(&harness, &name, forwarded).await;
    let mut request = publish(&name, &second, 0, 2);
    request.producer_id = 0;
    client.publish(request).await.unwrap();
    harness.wal.release();
    expect_ack(&mut client).await;
    expect_ack(&mut client).await;
    let datas: Vec<Vec<u8>> = harness
        .wal
        .stream_entries(&name)
        .iter()
        .map(|entry| parse_records(&entry.records)[0].3.clone())
        .collect();
    assert!(datas[0].starts_with(b"first"), "{datas:?}");
    assert!(datas[1].starts_with(b"second"), "{datas:?}");
    assert_wal_in_id_order(&harness, &name);
    harness.finish();
}

#[tokio::test]
async fn state_ops_keep_their_order_across_a_move_back() {
    let harness = Harness::new(one_slot_wal());
    let node = harness.node();
    let name = name_on_core(0, node, "back");
    let mut client = node.client(0).await;
    own_on_stalled_core_1(&harness, &name).await;
    let forwarded = node.stats(0).await.unwrap().forwarded_count;
    client.state_op(set_op(&name, 1, 10)).await.unwrap();
    move_back_behind_the_forward(&harness, &name, forwarded).await;
    client.state_op(set_op(&name, 2, 20)).await.unwrap();
    harness.wal.release();
    for _ in 0..2 {
        assert!(matches!(
            expect_reply(&mut client).await,
            Reply::StateResult { ok: true, .. }
        ));
    }
    let snapshot = client.snapshot(key(&name)).await.unwrap();
    assert_eq!(from_cbor(&snapshot.state), int(20));
    assert_eq!(replay_state(&harness, &name), {
        let mut live = server::StreamState::new();
        live.install(snapshot.state.clone(), snapshot.state_ver);
        live
    });
    harness.finish();
}

/// A shard going back and forth under load, with the WAL stalling on and off: what each
/// client sent commits once, in the order it was sent, however the routes changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shard_bouncing_under_stalls_keeps_every_client_in_order() {
    let mut config = manual_config(3);
    config.wal_channel_capacity = 1;
    let harness = Harness::new(config);
    let node = harness.node();
    let name = name_on_core(0, node, "bounce");
    let shard = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");
    let mut clients = Vec::new();
    for home in 0..3 {
        clients.push(node.client(home).await);
    }
    let send_count = 120u64;
    for seq in 1..=send_count {
        for (index, client) in clients.iter_mut().enumerate() {
            let mut request = publish(&name, &records, seq, 1);
            request.producer_id = index as u64 + 1;
            // A full inbox waits for a stalled core, which only the release can end.
            while client.try_publish(request.clone()) == Err(server::ClientError::Full) {
                harness.wal.release();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        if seq % 7 == 0 {
            harness.wal.hold();
        } else if seq % 7 == 4 {
            harness.wal.release();
        }
        if seq % 3 == 0 {
            node.move_shard(shard, (seq as usize / 3) % 3).await;
        }
        if seq % 5 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    harness.wal.release();
    for (index, client) in clients.iter_mut().enumerate() {
        let mut acks = 0;
        while acks < send_count {
            match expect_reply(client).await {
                Reply::Ack {
                    first_id,
                    producer_seq,
                    ..
                } => {
                    assert!(
                        first_id > 0,
                        "client {index} seq {producer_seq} was acked as committed unwritten, {:?}",
                        harness
                            .wal
                            .stream_entries(&name)
                            .iter()
                            .filter(|entry| entry.producer_id == index as u64 + 1)
                            .map(|entry| entry.producer_seq)
                            .collect::<Vec<_>>()
                    );
                    acks += 1;
                }
                other => panic!("{other:?}"),
            }
        }
        let mut request = publish(&name, &records, send_count + 1, 2);
        request.producer_id = index as u64 + 1;
        client.publish(request).await.unwrap();
        expect_ack(client).await;
    }
    let entries = harness.wal.stream_entries(&name);
    for producer_id in 1..=3 {
        let seqs: Vec<u64> = entries
            .iter()
            .filter(|entry| entry.producer_id == producer_id)
            .map(|entry| entry.producer_seq)
            .collect();
        assert_eq!(seqs, (1..=send_count + 1).collect::<Vec<_>>());
    }
    assert_wal_in_id_order(&harness, &name);
    harness.finish();
}
