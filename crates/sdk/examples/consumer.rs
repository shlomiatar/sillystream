//! A durable consumer: a named cursor, committed after the work and on PROGRESS (PATTERNS.md 2).
//!
//! `cargo run -p sdk --example consumer -- STREAM CURSOR [eq:MASK:VALUE]`,
//! with SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN set as for `silly`.

mod common;

use sdk::{Event, FROM_START, Subscribe, WireClause};
use std::time::Duration;

fn hex(text: &str) -> u64 {
    u64::from_str_radix(text.trim_start_matches("0x"), 16).unwrap()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (client, _) = common::connect().await;
    // `from` counts only the first time: once the server holds the cursor, it starts there.
    let mut spec = Subscribe::new(&args[0], FROM_START);
    spec.name = Some(args[1].clone());
    if let Some(filter) = args.get(2) {
        let parts: Vec<&str> = filter.split(':').collect();
        spec.clauses.push(WireClause {
            mask: hex(parts[1]),
            value: hex(parts[2]),
            op: 0,
        });
    }
    let mut sub = client.subscribe(spec).unwrap();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
        match event.unwrap() {
            Event::Items(items) => {
                for item in &items {
                    println!("{} {}", item.id, String::from_utf8_lossy(&item.bytes)); // the work
                }
                // After the work: a crash before this line means these come again.
                sub.commit(items.last().unwrap().id).unwrap();
            }
            // Nothing matches up to `id`, so the cursor can move there too.
            Event::Progress(id) => sub.commit(id).unwrap(),
            Event::Behind { resume_from } => eprintln!("catching up from {resume_from}"),
            _ => {}
        }
    }
    // A commit is one-way: give it a moment to leave before the process ends.
    drop(sub);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
