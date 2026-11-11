//! A log shipper: options from level, service and region, batched, zstd, ack level 1
//! (PATTERNS.md 9).
//!
//! `cargo run -p sdk --example logship -- STREAM SERVICE REGION`, with lines `LEVEL message` on stdin,
//! with SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN set as for `silly`.

mod common;

use sdk::{AckLevel, PublisherConfig};
use std::io::BufRead;

const ERROR_FLAG: u64 = 1 << 63;

fn options(level: u64, service: u64, region: u64) -> u64 {
    let flag = if level >= 4 { ERROR_FLAG } else { 0 };
    flag | level << 56 | service << 40 | region << 24
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (service, region): (u64, u64) = (args[1].parse().unwrap(), args[2].parse().unwrap());
    let (client, _) = common::connect().await;
    // Level 1: acked from memory. A process crash can lose the last few ms of logs.
    let config = PublisherConfig {
        ack: AckLevel::Memory,
        ..PublisherConfig::default()
    };
    let mut publisher = client.publisher(&args[0], config).unwrap();
    let mut lines = std::io::stdin().lock().lines();
    loop {
        // One publish per 1,000 lines here; a real shipper also flushes on a timer.
        for line in lines.by_ref().take(1000) {
            let line = line.unwrap();
            let (level, message) = line.split_once(' ').unwrap();
            let level = match level {
                "debug" => 1,
                "info" => 2,
                "warn" => 3,
                "error" => 4,
                _ => 5,
            };
            publisher.add(options(level, service, region), message.to_owned());
        }
        if publisher.pending_count() == 0 {
            break;
        }
        for ack in publisher.flush().await.unwrap() {
            println!("first_id={} items={}", ack.first_id, ack.count);
        }
    }
}
