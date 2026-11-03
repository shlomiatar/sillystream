//! Moves against delivery, as fast as they will go. Scheduling isn't scripted here, so these
//! make the races of D83 and D85 likely, and don't prove them absent.

mod common;

use common::*;
use server::{SealOutcome, Subscription};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const STREAM: &str = "stress.stream";

/// An owner on the subscribers' own core delivers directly, which would overtake what the
/// last owner has in that core's delivery channel if nothing put them back in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_delivery_survives_moves_onto_and_off_the_home_core() {
    let harness = Harness::new(manual_config(3));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let shard = server::shard_of(key(STREAM), node.virtual_shard_count());
    let mut connection = node.connection(1).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    subscribed(&mut connection, &mut heard, 1).await;

    let mut count = 0u64;
    let mut ids = Vec::new();
    for round in 0..200usize {
        for _ in 0..20 {
            count += 1;
            let records = record_with(count, &count.to_le_bytes());
            client
                .publish(publish(STREAM, &records, count, 1))
                .await
                .unwrap();
        }
        // Onto the subscriber's core and away again, with the fan-out still arriving.
        assert!(
            node.move_shard(shard, if round % 2 == 0 { 1 } else { 2 })
                .await
        );
    }
    for _ in 0..count {
        ids.push(expect_ack(&mut client).await.1);
    }
    let last_id = *ids.iter().max().unwrap();
    hear_until(&mut connection, &mut heard, |heard| {
        heard
            .records(1)
            .last()
            .is_some_and(|last| last.0 >= last_id)
            || heard.records(1).len() as u64 >= count
    })
    .await;
    let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
    ids.sort_unstable();
    assert_eq!(got, ids, "every record, in id order");
    drop((connection, client));
    harness.finish();
}

/// A batch dropped from memory has to be findable in the extents by whoever reads next, even
/// with the stream on another core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_during_seals_and_moves_miss_nothing() {
    let harness = Harness::new(manual_config(3));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let shard = server::shard_of(key(STREAM), node.virtual_shard_count());
    let mut ids = Vec::new();
    let mut count = 0u64;
    let mut connection = node.connection(2).await.unwrap();
    let mut heard = Heard::default();
    for round in 0..60u32 {
        for _ in 0..5 {
            count += 1;
            let records = record_with(count, &count.to_le_bytes());
            client
                .publish(publish(STREAM, &records, count, 2))
                .await
                .unwrap();
            ids.push(expect_ack(&mut client).await.1);
        }
        assert!(node.move_shard(shard, round as usize % 3).await);
        // A reader starts while the seal is going on.
        connection.subscribe(Subscription::new(round, key(STREAM), 0));
        let seal = node.seal().await;
        assert!(matches!(
            seal,
            SealOutcome::Uploaded { .. } | SealOutcome::Empty { .. }
        ));
        let want = ids.len();
        hear_until(&mut connection, &mut heard, |heard| {
            heard.records(round).len() >= want
        })
        .await;
        let got: Vec<u64> = heard.records(round).iter().map(|record| record.0).collect();
        assert_eq!(got[..want], ids[..], "reader {round}");
        connection.unsubscribe(round);
        heard.events.clear();
    }
    drop((connection, client));
    harness.finish();
}
