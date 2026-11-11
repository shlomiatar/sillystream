//! A hot stream can get its own shard through the override table (SPEC section 6.1), and
//! the seal window's timer runs by itself.

mod common;

use common::*;
use server::shard_of;

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

/// `count` stream names whose shards all sit on `core` at start.
fn names_on_core(core: usize, cores: usize, virtual_shards: u32, count: usize) -> Vec<String> {
    let mut names = Vec::new();
    let mut index = 0;
    while names.len() < count {
        let name = format!("skew.{index}");
        if shard_of(key(&name), virtual_shards) as usize % cores == core {
            names.push(name);
        }
        index += 1;
    }
    names
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
    let shard = shard_of(key(hot), 2);
    assert_eq!(shard, shard_of(key(neighbour), 2));

    let records = record_with(1, b"data");
    let mut client = node.client(0).await;
    client.publish(publish(hot, &records, 1, 1)).await.unwrap();
    let first = expect_ack(&mut client).await;
    client
        .publish(publish(neighbour, &records, 1, 1))
        .await
        .unwrap();
    expect_ack(&mut client).await;

    let own_shard = node
        .set_override(key(hot), 1)
        .await
        .expect("the table has room");
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
    assert_eq!(node.set_override(key(hot), 1).await, Some(own_shard));

    // The hashed shard moves on; the override still works, through its new owner.
    node.move_shard(shard, 1).await;
    node.set_override(key(hot), 0).await;
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
    let shard = shard_of(key("new"), node.virtual_shard_count());
    let home = node.placement().await[shard as usize] as usize;
    let own = node.set_override(key("new"), 1 - home).await.unwrap();
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
    assert!(harness.node().set_override(key("a"), 0).await.is_some());
    assert_eq!(harness.node().set_override(key("b"), 0).await, None);
    harness.finish();
}

#[tokio::test]
async fn the_timer_runs_the_seal_by_itself() {
    let mut config = manual_config(2);
    // The shortest window section 7.1 allows.
    config.seal_window_ms = Some(1000);
    let harness = Harness::new(config);
    let node = harness.node();
    let names = names_on_core(0, 2, node.virtual_shard_count(), 8);
    let records = record_with(1, &[1; 256]);
    let mut client = node.client(0).await;
    let start = std::time::Instant::now();
    // Level 3 only comes back if the window's seal happens, with nothing asking for it.
    for name in &names {
        client.publish(publish(name, &records, 1, 3)).await.unwrap();
    }
    for _ in &names {
        expect_ack(&mut client).await;
    }
    assert!(start.elapsed().as_millis() >= 900);
    harness.finish();
}
