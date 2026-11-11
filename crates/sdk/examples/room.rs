//! Join a room with its state and head in one step, and hand it off atomically (PATTERNS.md 7.5).
//!
//! `cargo run -p sdk --example room -- STREAM ME [handoff NEXT]`,
//! with SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN set as for `silly`.

mod common;

use ciborium::{Value, cbor};
use common::*;
use sdk::{Event, FROM_LIVE, PublisherConfig, SdkError, StateChange, StateOp, Subscribe};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (stream, me) = (args[0].as_str(), args[1].as_str());
    let (client, _) = connect().await;

    // 1. Join: SUBSCRIBED carries the state and `head`, and the records follow from head + 1.
    let mut spec = Subscribe::new(stream, FROM_LIVE);
    spec.with_state = true;
    let mut sub = client.subscribe(spec).unwrap();
    let (ver, doc) = loop {
        if let Some(Ok(Event::Subscribed {
            head,
            state_ver,
            state,
        })) = sub.next().await
        {
            let doc: Value = ciborium::from_reader(&state[..]).unwrap();
            println!("{me}: joined at head {head}, state v{state_ver}: {doc:?}");
            break (state_ver, doc);
        }
    };

    // 2. Hand off: only the owner does, and only from the version it saw.
    if args.get(2).map(String::as_str) == Some("handoff") {
        let next = args[3].as_str();
        assert_eq!(
            get(&doc, "owner").and_then(Value::as_text),
            Some(me),
            "only the owner hands off"
        );
        let mut publisher = client
            .publisher(stream, PublisherConfig::default())
            .unwrap();
        publisher.add(
            0x04 << 56,
            format!("{me} -> {next}: parser done, please review"),
        );
        let change = cbor!({ "owner" => next, "phase" => "review" }).unwrap();
        let change = StateChange {
            expect_ver: ver,
            op: StateOp::Merge,
            doc: to_cbor(&change),
        };
        match publisher
            .send_with_state(change)
            .await
            .unwrap()
            .remove(0)
            .wait()
            .await
        {
            Ok(ack) => println!("{me}: handed off at {}", ack.first_id),
            Err(SdkError::VersionConflict { ver }) => println!("{me}: the room moved on (v{ver})"),
            Err(error) => panic!("{error}"),
        }
    }

    // 3. Follow the room.
    while let Some(event) = sub.next().await {
        if let Event::Items(items) = event.unwrap() {
            for item in items {
                println!(
                    "{me}: {} {}",
                    item.author,
                    String::from_utf8_lossy(&item.bytes)
                );
            }
        }
    }
}
