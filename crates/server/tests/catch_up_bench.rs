//! How fast one subscription catches up from an object store whose every `GET` waits a set
//! time, and how much of that time a `GET` is in flight. Not a test: run it with
//! `cargo test --release -p server --test catch_up_bench -- --ignored --nocapture`.
//! `BENCH_DELAY_MS` (default 50), `BENCH_MIB` (default 64), `BENCH_OBJECTS` (default 4) and
//! `BENCH_READ_AHEAD` (default the node's) change the run.

mod common;

use std::sync::atomic::Ordering;
use std::time::Instant;

use common::*;
use server::{Event, Subscription};

fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

const STREAM: &str = "bench.stream";
const RECORD_BYTES: usize = 64 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn catch_up_from_a_slow_store() {
    let delay_ms = env_u64("BENCH_DELAY_MS", 50);
    let total_mib = env_u64("BENCH_MIB", 64);
    let object_count = env_u64("BENCH_OBJECTS", 4).max(1);
    let mut config = manual_config(1);
    config.slab_bytes = 4 * 1024 * 1024;
    config.segment_cap_bytes = 1024 * 1024 * 1024;
    config.catch_up_read_ahead =
        env_u64("BENCH_READ_AHEAD", config.catch_up_read_ahead as u64) as usize;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let record_count = total_mib * 1024 * 1024 / RECORD_BYTES as u64;
    let data = vec![7u8; RECORD_BYTES];
    let per_object = record_count.div_ceil(object_count);
    for seq in 1..=record_count {
        client
            .publish(publish(STREAM, &record_with(1, &data), seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        if seq % per_object == 0 || seq == record_count {
            node.seal().await;
        }
    }
    wait_for_memory_to_empty(node, STREAM).await;

    harness.store.get_delay_ms.store(delay_ms, Ordering::SeqCst);
    let busy_before = harness.store.get_busy_nanos();
    let mut connection = node.connection(0).await.unwrap();
    let started = Instant::now();
    connection.subscribe(Subscription::new(1, key(STREAM), 0));
    let mut got = 0u64;
    let mut bytes = 0u64;
    while got < record_count {
        match connection.recv_within(10_000).await {
            Some(Event::Data { records, .. }) => {
                let frame = records.to_vec();
                bytes += frame.len() as u64;
                got += format::Records::new(&frame).count() as u64;
            }
            Some(_) => {}
            None => panic!("stalled at {got} of {record_count}"),
        }
    }
    let elapsed = started.elapsed();
    let busy = harness.store.get_busy_nanos() - busy_before;
    let mib = bytes as f64 / (1024.0 * 1024.0);
    println!(
        "delay {delay_ms} ms, {mib:.0} MiB in {object_count} objects: {:.2} s, {:.1} MiB/s, \
         a GET in flight {:.0}% of the time, at most {} at once",
        elapsed.as_secs_f64(),
        mib / elapsed.as_secs_f64(),
        100.0 * busy as f64 / elapsed.as_nanos() as f64,
        harness.store.max_gets_in_flight.load(Ordering::SeqCst),
    );
    drop((connection, client));
    harness.finish();
}
