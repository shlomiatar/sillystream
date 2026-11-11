//! Watch a stream's state doc: a `state_only` subscription gets the doc now and each new one
//! (PATTERNS.md 3.3).
//!
//! `cargo run -p sdk --example watch_state -- STREAM`,
//! with SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN set as for `silly`.

mod common;

use sdk::{Event, FROM_LIVE, Subscribe};

#[tokio::main]
async fn main() {
    let stream = std::env::args().nth(1).expect("STREAM");
    let (client, _) = common::connect().await;
    let mut spec = Subscribe::new(&stream, FROM_LIVE);
    spec.state_only = true;
    let mut sub = client.subscribe(spec).unwrap();
    while let Some(event) = sub.next().await {
        let (ver, doc) = match event.unwrap() {
            Event::Subscribed {
                state_ver, state, ..
            } => (state_ver, state),
            Event::State { ver, doc } => (ver, doc),
            _ => continue,
        };
        let doc: ciborium::Value = ciborium::from_reader(&doc[..]).unwrap();
        println!("v{ver}: {doc:?}");
    }
}
