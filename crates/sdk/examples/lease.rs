//! A leader lease in the state doc, with the version as the fencing token (PATTERNS.md 7.1).
//!
//! `cargo run -p sdk --example lease -- STREAM HOLDER TTL_MS STEPS STEP_MS`,
//! with SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN set as for `silly`.

mod common;

use ciborium::{Value, cbor};
use common::*;
use sdk::{PublisherConfig, SdkError, StateChange, StateOp};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (stream, me) = (args[0].as_str(), args[1].as_str());
    let ttl: u64 = args[2].parse().unwrap();
    let (steps, step_ms): (u32, u64) = (args[3].parse().unwrap(), args[4].parse().unwrap());
    let (client, http) = connect().await;

    // 1. Acquire: take the lease if it's free, expired or already ours, by CAS on its version.
    let mut fence = loop {
        let (doc, ver, _) = state_get(&http, stream).await;
        // expect_ver 0 is unconditional, so the doc must exist before anyone competes for it.
        assert!(ver != 0, "initialize {stream}'s state first");
        let holder = get(&doc, "holder").and_then(Value::as_text).unwrap_or("");
        let until = get(&doc, "until")
            .and_then(Value::as_integer)
            .map_or(0, |n| u64::try_from(n).unwrap());
        if holder.is_empty() || holder == me || until < now_ms() {
            let lease = cbor!({ "holder" => me, "until" => now_ms() + ttl }).unwrap();
            let change = StateChange {
                expect_ver: ver,
                op: StateOp::Set,
                doc: to_cbor(&lease),
            };
            match client.state_op(stream, change).await {
                Ok(new_ver) => break new_ver,
                Err(SdkError::VersionConflict { .. }) => continue, // someone else moved first
                Err(error) => panic!("{error}"),
            }
        }
        println!("{me}: {holder} holds it until {until}; waiting");
        tokio::time::sleep(Duration::from_millis(until.saturating_sub(now_ms()) + 10)).await;
    };
    println!("{me}: acquired, fence {fence}");

    // 2. Work: every write renews the lease in the same publish, conditional on the fence.
    let mut publisher = client
        .publisher(stream, PublisherConfig::default())
        .unwrap();
    for step in 0..steps {
        tokio::time::sleep(Duration::from_millis(step_ms)).await;
        publisher.add(0, format!("{me} step {step}"));
        let renew = cbor!({ "until" => now_ms() + ttl }).unwrap();
        let change = StateChange {
            expect_ver: fence,
            op: StateOp::Merge,
            doc: to_cbor(&renew),
        };
        match publisher
            .send_with_state(change)
            .await
            .unwrap()
            .remove(0)
            .wait()
            .await
        {
            // The state change is the publish's last id, and that id is the new version.
            Ok(ack) => fence = ack.first_id + u64::from(ack.count) - 1,
            Err(SdkError::VersionConflict { .. }) => {
                println!("{me}: fenced out at step {step}; nothing of it committed");
                return;
            }
            Err(error) => panic!("{error}"),
        }
        println!("{me}: step {step} committed, fence {fence}");
    }

    // 3. Release, still conditional, so a late release can't free a successor's lease.
    let free = cbor!({ "holder" => "", "until" => 0 }).unwrap();
    let change = StateChange {
        expect_ver: fence,
        op: StateOp::Set,
        doc: to_cbor(&free),
    };
    println!(
        "{me}: released {:?}",
        client.state_op(stream, change).await.map(|_| ())
    );
}
