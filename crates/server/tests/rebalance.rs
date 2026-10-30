//! The periodic check moves shards off an overloaded core, and a hot stream can get its
//! own shard (SPEC section 6.1).

mod common;

use common::*;
use server::{SealOutcome, shard_of};

/// `count` stream names whose shards all sit on `core` at start.
fn names_on_core(core: usize, cores: usize, virtual_shards: u32, count: usize) -> Vec<String> {
    let mut names = Vec::new();
    let mut index = 0;
    while names.len() < count {
        let name = format!("skew.{index}");
        if shard_of(&name, virtual_shards) as usize % cores == core {
            names.push(name);
        }
        index += 1;
    }
    names
}

async fn pump(client: &mut server::Client, names: &[String], records: &[u8], seq: &mut u64) {
    for name in names {
        client
            .publish(publish(name, records, *seq, 1))
            .await
            .unwrap();
        expect_ack(client).await;
    }
    *seq += 1;
}

#[tokio::test]
async fn skewed_load_moves_shards_off_the_overloaded_core_and_loses_nothing() {
    let cores = 4;
    let harness = Harness::new(manual_config(cores));
    let node = harness.node();
    let shards = node.virtual_shard_count();
    let names = names_on_core(0, cores, shards, 16);
    let records = record_with(1, &[3; 512]);
    let mut client = node.client(0).await;
    let mut seq = 1;

    // Nothing moves on a node with no load.
    assert!(node.rebalance_now().await.is_empty());

    for _ in 0..5 {
        pump(&mut client, &names, &records, &mut seq).await;
    }
    let moves = node.rebalance_now().await;
    assert!(!moves.is_empty());
    assert!(
        moves
            .iter()
            .all(|planned| planned.from == 0 && planned.to != 0)
    );

    let placement = node.placement().await;
    for planned in &moves {
        assert_eq!(placement[planned.shard as usize] as usize, planned.to);
    }
    let per_core = |placement: &[u16]| {
        let mut per_core = vec![0; cores];
        for name in &names {
            per_core[placement[shard_of(name, shards) as usize] as usize] += 1;
        }
        per_core
    };
    let spread = per_core(&placement);
    assert!(spread[0] < names.len(), "{spread:?}");
    assert!(spread.iter().all(|count| *count > 0), "{spread:?}");

    // It settles: with the same load spread out, a later check moves little or nothing,
    // and never back onto a core above the others.
    for _ in 0..5 {
        pump(&mut client, &names, &records, &mut seq).await;
        node.rebalance_now().await;
    }
    pump(&mut client, &names, &records, &mut seq).await;
    let settled = node.rebalance_now().await;
    assert!(settled.len() <= 1, "{settled:?}");

    // Each stream's streams live on the core the table says, and nothing was lost.
    let final_placement = node.placement().await;
    let mut total = 0;
    for core in 0..cores {
        total += node.stats(core).await.unwrap().stream_count;
    }
    assert_eq!(total, names.len());
    let last_seq = seq;
    for name in &names {
        client
            .publish(publish(name, &records, last_seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        let entries = harness.wal.stream_entries(name);
        assert_eq!(entries.len() as u64, last_seq);
        let ids: Vec<u64> = entries
            .iter()
            .map(|entry| parse_records(&entry.records)[0].0)
            .collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        let core = final_placement[shard_of(name, shards) as usize] as usize;
        assert!(core < cores);
    }
    assert!(matches!(node.seal().await, SealOutcome::Uploaded { .. }));
    harness.finish();
}

#[tokio::test]
async fn a_hot_stream_gets_its_own_shard_and_its_neighbours_stay() {
    let cores = 2;
    let mut config = manual_config(cores);
    // Few shards, so that it has a neighbour to leave.
    config.virtual_shard_count = 2;
    let harness = Harness::new(config);
    let node = harness.node();
    let names = names_on_core(0, cores, 2, 2);
    let (hot, neighbour) = (names[0].as_str(), names[1].as_str());
    let shard = shard_of(hot, 2);
    assert_eq!(shard, shard_of(neighbour, 2));

    let records = record_with(1, b"data");
    let mut client = node.client(0).await;
    client.publish(publish(hot, &records, 1, 1)).await.unwrap();
    let first = expect_ack(&mut client).await;
    client
        .publish(publish(neighbour, &records, 1, 1))
        .await
        .unwrap();
    expect_ack(&mut client).await;

    let own_shard = node.set_override(hot, 1).await.expect("the table has room");
    assert!(own_shard >= 2);
    assert_eq!(node.placement().await[own_shard as usize], 1);
    // The hashed shard didn't move, and took only the hot stream's name with it.
    assert_eq!(node.placement().await[shard as usize], 0);
    assert_eq!(node.stats(1).await.unwrap().stream_count, 1);
    assert_eq!(node.stats(0).await.unwrap().stream_count, 1);

    // A client with no idea of the override reaches it, with ids and dedupe intact.
    let mut fresh = node.client(0).await;
    fresh.publish(publish(hot, &records, 1, 1)).await.unwrap();
    assert_eq!(expect_ack(&mut fresh).await, first);
    fresh.publish(publish(hot, &records, 2, 1)).await.unwrap();
    assert!(expect_ack(&mut fresh).await.1 > first.1);
    // A second override of the same stream is the same shard.
    assert_eq!(node.set_override(hot, 1).await, Some(own_shard));

    // The hashed shard moves on; the override still works, through its new owner.
    node.move_shard(shard, 1).await;
    node.set_override(hot, 0).await;
    fresh.publish(publish(hot, &records, 3, 2)).await.unwrap();
    assert!(expect_ack(&mut fresh).await.1 > first.1);
    let mut late = node.client(1).await;
    late.publish(publish(hot, &records, 4, 2)).await.unwrap();
    assert!(expect_ack(&mut late).await.1 > first.1);
    assert_eq!(node.stats(0).await.unwrap().stream_count, 1);
    assert_eq!(harness.wal.stream_entries(hot).len(), 4);
    harness.finish();
}

#[tokio::test]
async fn an_override_for_a_stream_with_no_records_yet_applies_when_it_appears() {
    let cores = 2;
    let harness = Harness::new(manual_config(cores));
    let node = harness.node();
    let shard = shard_of("new", node.virtual_shard_count());
    let home = node.placement().await[shard as usize] as usize;
    let own = node.set_override("new", 1 - home).await.unwrap();
    assert_eq!(node.placement().await[own as usize] as usize, 1 - home);
    let mut client = node.client(0).await;
    let records = record_with(1, b"data");
    client
        .publish(publish("new", &records, 1, 1))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    assert_eq!(node.stats(1 - home).await.unwrap().stream_count, 1);
    assert_eq!(node.stats(home).await.unwrap().stream_count, 0);
    harness.finish();
}

#[tokio::test]
async fn the_override_table_is_small_and_says_when_it_is_full() {
    let mut config = manual_config(1);
    config.override_shard_count = 1;
    let harness = Harness::new(config);
    assert!(harness.node().set_override("a", 0).await.is_some());
    assert_eq!(harness.node().set_override("b", 0).await, None);
    harness.finish();
}

#[tokio::test]
async fn the_timer_runs_the_check_and_the_seal_by_itself() {
    let mut config = manual_config(2);
    config.rebalance_period_ms = Some(10);
    // The shortest window section 7.1 allows.
    config.seal_window_ms = Some(1000);
    let harness = Harness::new(config);
    let node = harness.node();
    let names = names_on_core(0, 2, node.virtual_shard_count(), 8);
    let records = record_with(1, &[1; 256]);
    let mut client = node.client(0).await;
    // Taken before any load, since the first check can come at any moment after it.
    let initial = node.placement().await;
    let start = std::time::Instant::now();
    let mut seq = 1;
    // Level 3 only comes back if the window's seal happens, with nothing asking for it.
    for name in &names {
        client
            .publish(publish(name, &records, seq, 3))
            .await
            .unwrap();
    }
    for _ in &names {
        expect_ack(&mut client).await;
    }
    seq += 1;
    assert!(start.elapsed().as_millis() >= 900);
    // Load keeps coming, and sooner or later the check moves something by itself.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(WAIT_MS);
    while node.placement().await == initial {
        assert!(
            std::time::Instant::now() < deadline,
            "the check never moved a shard"
        );
        for name in &names {
            client
                .publish(publish(name, &records, seq, 1))
                .await
                .unwrap();
            expect_ack(&mut client).await;
        }
        seq += 1;
    }
    harness.finish();
}
