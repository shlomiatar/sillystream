//! A stream moving between cores keeps its order and loses nothing (SPEC sections 6.1, 6.4).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use server::{Reply, SealOutcome, shard_of};
use storage::SegmentReader;

/// Ids that go up, with none missing, against the acks.
fn check_ids(records: &[(u64, u64, u64, Vec<u8>)], acks: &[(u64, u64, u32)]) {
    let ids: Vec<u64> = records.iter().map(|record| record.0).collect();
    let acked: Vec<u64> = acks.iter().map(|ack| ack.1).collect();
    assert_eq!(ids, acked);
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn a_stream_moved_mid_load_keeps_order_and_acks_and_shows_two_footer_entries() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    let records = record_with(1, b"payload");
    let shard = shard_of("moving", node.virtual_shard_count());
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

    // The segment has both cores' extents for the stream, and together they are all of it.
    let (path, extents) = match node.seal().await {
        SealOutcome::Uploaded { path, extents, .. } => (path, extents),
        other => panic!("{other:?}"),
    };
    let own: Vec<_> = extents
        .iter()
        .filter(|extent| &*extent.stream == "moving")
        .collect();
    assert_eq!(own.len(), 2, "{extents:?}");
    let store: Arc<dyn object_store::ObjectStore> = harness.store.clone();
    let reader = SegmentReader::new(store, path);
    let top = reader.read_top_index().await.unwrap();
    let in_footer = reader.all_extents(&top).await.unwrap();
    assert_eq!(
        in_footer
            .iter()
            .filter(|extent| &*extent.stream == "moving")
            .count(),
        2
    );
    let stored = reader.read_stream("moving").await.unwrap();
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
    let before = client.snapshot("s").await.unwrap();

    let shard = shard_of("s", node.virtual_shard_count());
    let from = node.placement().await[shard as usize] as usize;
    node.move_shard(shard, 1 - from).await;

    // A snapshot follows the stream even from a stale route.
    assert_eq!(client.snapshot("s").await.unwrap(), before);
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
    let shard = shard_of("s", node.virtual_shard_count());
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
    let shard = shard_of("s", node.virtual_shard_count());
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
                    .filter(|extent| &*extent.stream == "s")
                    .collect();
                own.sort_by_key(|extent| extent.id_first);
                assert!(
                    own.windows(2)
                        .all(|pair| pair[0].id_last < pair[1].id_first),
                    "{own:?}"
                );
                let store: Arc<dyn object_store::ObjectStore> = harness.store.clone();
                let stored = SegmentReader::new(store, path)
                    .read_stream("s")
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
    let shard = shard_of("s", node.virtual_shard_count());
    let from = node.placement().await[shard as usize] as usize;
    let to = 1 - from;
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    expect_ack(&mut client).await;
    assert_eq!(client.route_of("s"), from);
    node.move_shard(shard, to).await;

    // Pipelined across the route change: if any overtook another, the older one would come
    // back with first_id = 0, as a duplicate.
    let mut seq = 2;
    let mut acks = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while client.route_of("s") != to {
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
            let shard = shard_of(stream, node.virtual_shard_count());
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
            shard_of(name, node.virtual_shard_count()) as usize % node.core_count() == core
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
    let shard = shard_of(&name, node.virtual_shard_count());
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
    assert_eq!(client.route_of(&name), 1);
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
    let shard = shard_of(&name, node.virtual_shard_count());
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
            stream: &name,
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
    assert_eq!(client.route_of(&name), 1);
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
    let shard = shard_of(&name, node.virtual_shard_count());
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
