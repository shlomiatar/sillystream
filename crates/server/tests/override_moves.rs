//! Override shards and hashed shards moving in any order: following what cores know always
//! ends at the one owner (SPEC section 6.1, D66).

mod common;

use common::*;
use server::shard_of;

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

/// How long an ack that could arrive has, before calling it lost in a forwarding cycle.
const ACK_WAIT_MS: u64 = 2_000;

/// A stream whose hashed shard starts on `core`, as placement is `shard % core_count`.
fn name_on_core(core: usize, node: &server::Node) -> String {
    (0..)
        .map(|n| format!("s{n}"))
        .find(|name| {
            shard_of(key(name), node.virtual_shard_count()) as usize % node.core_count() == core
        })
        .expect("names go on for ever")
}

async fn stream_count_total(node: &server::Node) -> usize {
    let mut total = 0;
    for core in 0..node.core_count() {
        total += node.stats(core).await.unwrap().stream_count;
    }
    total
}

async fn ack_within(client: &mut server::Client) -> (u64, u64, u32) {
    match client.recv_within(ACK_WAIT_MS).await {
        Some(server::Reply::Ack {
            producer_seq,
            first_id,
            count,
            ..
        }) => (producer_seq, first_id, count),
        other => panic!("expected an ack, got {other:?}"),
    }
}

#[tokio::test]
async fn a_stale_override_entry_cant_make_a_second_owner() {
    let harness = Harness::new(manual_config(4));
    let node = harness.node();
    let name = name_on_core(0, node);
    let hashed = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");

    let own = node.set_override(key(&name), 1).await.unwrap();
    let mut client = node.client(0).await;
    client
        .publish(publish(&name, &records, 5, 2))
        .await
        .unwrap();
    let original = ack_within(&mut client).await;

    node.move_shard(hashed, 2).await;
    node.move_shard(own, 3).await;
    // Core 2 still has core 1 down for the stream's shard, and now hands that to core 1.
    node.move_shard(hashed, 1).await;

    let mut retry = node.client(0).await;
    retry.publish(publish(&name, &records, 5, 2)).await.unwrap();
    assert_eq!(ack_within(&mut retry).await, original);
    assert_eq!(stream_count_total(node).await, 1);
    assert_eq!(node.stats(3).await.unwrap().stream_count, 1);
    assert_eq!(node.stats(3).await.unwrap().duplicate_count, 1);
    assert_eq!(harness.wal.stream_entries(&name).len(), 1);
    harness.finish();
}

#[tokio::test]
async fn a_new_stream_doesnt_bounce_after_its_override_moved_twice() {
    let harness = Harness::new(manual_config(3));
    let node = harness.node();
    let name = name_on_core(0, node);
    let hashed = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");

    node.set_override(key(&name), 1).await.unwrap();
    node.set_override(key(&name), 2).await.unwrap();
    node.move_shard(hashed, 2).await;

    let mut client = node.client(0).await;
    client
        .publish(publish(&name, &records, 1, 1))
        .await
        .unwrap();
    let (_, first_id, _) = ack_within(&mut client).await;
    assert!(first_id > 0);
    assert_eq!(node.stats(2).await.unwrap().stream_count, 1);
    assert_eq!(stream_count_total(node).await, 1);
    harness.finish();
}

#[tokio::test]
async fn a_written_stream_doesnt_bounce_after_its_override_moved_three_times() {
    let harness = Harness::new(manual_config(4));
    let node = harness.node();
    let name = name_on_core(0, node);
    let hashed = shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(1, b"payload");

    let mut first = node.client(0).await;
    first.publish(publish(&name, &records, 1, 1)).await.unwrap();
    ack_within(&mut first).await;
    node.set_override(key(&name), 1).await.unwrap();
    node.set_override(key(&name), 2).await.unwrap();
    node.set_override(key(&name), 3).await.unwrap();
    node.move_shard(hashed, 2).await;

    let mut client = node.client(0).await;
    client
        .publish(publish(&name, &records, 2, 1))
        .await
        .unwrap();
    let (seq, first_id, _) = ack_within(&mut client).await;
    assert_eq!(seq, 2);
    assert!(first_id > 0);
    assert_eq!(node.stats(3).await.unwrap().stream_count, 1);
    assert_eq!(stream_count_total(node).await, 1);
    harness.finish();
}

/// Any order of moves, as the rebalancer could produce. Clients made along the way start
/// from what was true then, so the walk begins from many different cores.
#[tokio::test]
async fn any_order_of_moves_ends_at_one_owner() {
    let core_count = 4;
    let mut random = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move |below: u64| {
        // A plain xorshift: the same cases on every run.
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random % below
    };
    for case in 0..30 {
        let harness = Harness::new(manual_config(core_count));
        let node = harness.node();
        let name = name_on_core(case % core_count, node);
        let hashed = shard_of(key(&name), node.virtual_shard_count());
        let records = record_with(1, b"payload");
        let mut own = None;
        let mut clients = vec![node.client(0).await];
        let mut seq = 1;
        let mut last_id = 0;
        for _ in 0..12 {
            let to = next(core_count as u64) as usize;
            match next(4) {
                0 => own = node.set_override(key(&name), to).await,
                1 => {
                    node.move_shard(hashed, to).await;
                }
                2 => {
                    if let Some(own) = own {
                        node.move_shard(own, to).await;
                    }
                }
                _ => {
                    let client = clients.last_mut().unwrap();
                    client
                        .publish(publish(&name, &records, seq, 1))
                        .await
                        .unwrap();
                    let (_, first_id, _) = ack_within(client).await;
                    assert!(first_id > last_id, "case {case}");
                    last_id = first_id;
                    seq += 1;
                }
            }
            clients.push(node.client(0).await);
        }
        for client in &mut clients {
            client
                .publish(publish(&name, &records, seq, 1))
                .await
                .unwrap();
            let (_, first_id, _) = ack_within(client).await;
            assert!(first_id > last_id, "case {case}");
            last_id = first_id;
            seq += 1;
        }
        assert_eq!(stream_count_total(node).await, 1, "case {case}");
        harness.finish();
    }
}

#[tokio::test]
async fn bad_placement_arguments_are_refused_and_change_nothing() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let shard = server::shard_of(key("s"), node.virtual_shard_count());
    let total = node.virtual_shard_count() + 64;
    let before = node.placement().await;

    // A core that doesn't exist.
    assert!(!node.move_shard(shard, 2).await);
    assert_eq!(node.set_override(key("s"), 99).await, None);
    // A shard that doesn't exist, and an override shard no stream has yet.
    assert!(!node.move_shard(total, 1).await);
    assert!(!node.move_shard(node.virtual_shard_count(), 1).await);
    assert_eq!(node.placement().await, before);

    // Everything still works, including what the panics took down.
    assert_eq!(node.ping(0).await, Some(0));
    assert_eq!(node.ping(1).await, Some(1));
    assert!(matches!(
        node.seal().await,
        server::SealOutcome::Empty { .. }
    ));
    let mut client = node.client(0).await;
    let records = record_with(1, b"hello");
    client.publish(publish("s", &records, 1, 2)).await.unwrap();
    expect_ack(&mut client).await;
    assert!(
        node.move_shard(shard, 1 - before[shard as usize] as usize)
            .await
    );
    let own = node.set_override(key("s"), 1).await.unwrap();
    assert!(node.move_shard(own, 0).await);
    assert!(harness.finish().wal.is_ok());
}
