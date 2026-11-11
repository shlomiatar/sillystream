//! Competing workers claiming tasks by CAS on a shared claim cursor and leases (PATTERNS.md 7.2).
//!
//! `cargo run -p sdk --example queue -- STREAM WORKER TTL_MS WORK_MS`,
//! with SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN set as for `silly`.

mod common;

use ciborium::{Value, cbor};
use common::*;
use sdk::{Event, PublisherConfig, SdkError, StateChange, StateOp, Subscribe, WireClause};
use std::time::Duration;

const KIND: u64 = 0xff << 56;
const TASK: u64 = 0x01 << 56;
const RESULT: u64 = 0x03 << 56;

fn int(doc: &Value, key: &str) -> u64 {
    get(doc, key)
        .and_then(Value::as_integer)
        .map_or(0, |n| u64::try_from(n).unwrap())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (stream, me) = (args[0].as_str(), args[1].as_str());
    let (ttl, work_ms): (u64, u64) = (args[2].parse().unwrap(), args[3].parse().unwrap());
    let (client, http) = connect().await;
    let mut results = client
        .publisher(stream, PublisherConfig::default())
        .unwrap();

    // Read tasks from the shared claim cursor on. The CAS below decides who runs each one.
    let (doc, _, _) = state_get(&http, stream).await;
    let mut spec = Subscribe::new(stream, int(&doc, "next"));
    spec.clauses.push(WireClause {
        mask: KIND,
        value: TASK,
        op: 0,
    });
    let mut tasks = client.subscribe(spec).unwrap();

    while let Some(event) = tasks.next().await {
        let Event::Items(items) = event.unwrap() else {
            continue;
        };
        for task in items {
            let key = task.id.to_string();
            // Claim: move "next" to this task and add its lease, if no one has moved it past.
            let claimed = loop {
                let (doc, ver, _) = state_get(&http, stream).await;
                // expect_ver 0 is unconditional, so the doc must exist before anyone claims.
                assert!(ver != 0, "initialize {stream}'s state first");
                if int(&doc, "next") >= task.id {
                    break false; // another worker claimed it
                }
                let claim = cbor!({
                    "next" => task.id,
                    "leases" => { key.as_str() => { "by" => me, "until" => now_ms() + ttl } },
                })
                .unwrap();
                let change = StateChange {
                    expect_ver: ver,
                    op: StateOp::Merge,
                    doc: to_cbor(&claim),
                };
                match client.state_op(stream, change).await {
                    Ok(_) => break true,
                    Err(SdkError::VersionConflict { .. }) => continue,
                    Err(error) => panic!("{error}"),
                }
            };
            if !claimed {
                continue;
            }
            tokio::time::sleep(Duration::from_millis(work_ms)).await; // the work
            // Done: the result and the lease's removal commit together, while the lease is ours.
            loop {
                let (doc, ver, _) = state_get(&http, stream).await;
                let lease = get(&doc, "leases").and_then(|leases| get(leases, &key));
                if lease.and_then(|l| get(l, "by")).and_then(Value::as_text) != Some(me) {
                    println!("{me}: lost the lease on {key}; dropping the result");
                    break;
                }
                results.add(
                    RESULT,
                    format!("{me} did {}", String::from_utf8_lossy(&task.bytes)),
                );
                let done = cbor!({ "leases" => { key.as_str() => null } }).unwrap();
                let change = StateChange {
                    expect_ver: ver,
                    op: StateOp::Merge,
                    doc: to_cbor(&done),
                };
                match results
                    .send_with_state(change)
                    .await
                    .unwrap()
                    .remove(0)
                    .wait()
                    .await
                {
                    Ok(_) => break,
                    Err(SdkError::VersionConflict { .. }) => continue, // another claim moved it
                    Err(error) => panic!("{error}"),
                }
            }
        }
    }
}
