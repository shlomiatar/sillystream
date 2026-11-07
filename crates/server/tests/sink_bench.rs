//! How fast a `webhook.deliver` sink drains a stream into a receiver that takes a set time to
//! answer each POST. Not a test: run it with
//! `cargo test --release -p server --test sink_bench -- --ignored --nocapture`.
//! `BENCH_DELAYS_MS` (default `0,5,20`), `BENCH_RECORDS` (default 20000), `BENCH_BYTES`
//! (default 1024) and `BENCH_SECONDS` (default 10) change the runs.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::*;
use server::{SinkOp, SinkSpec, SinkStart};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Answers every POST with 200 after `delay`, counting the records and requests it got.
async fn receiver(delay: Duration, records: Arc<AtomicU64>, posts: Arc<AtomicU64>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let (records, posts) = (Arc::clone(&records), Arc::clone(&posts));
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = vec![0u8; 256 * 1024];
                loop {
                    let head_end = loop {
                        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                            break end + 4;
                        }
                        match socket.read(&mut chunk).await {
                            Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
                            _ => return,
                        }
                    };
                    let head = String::from_utf8_lossy(&buffer[..head_end]).to_ascii_lowercase();
                    let len: usize = head
                        .split("\r\n")
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .and_then(|value| value.trim().parse().ok())
                        .unwrap_or(0);
                    while buffer.len() < head_end + len {
                        match socket.read(&mut chunk).await {
                            Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
                            _ => return,
                        }
                    }
                    let body = &buffer[head_end..head_end + len];
                    let count = format::Records::new(body).count() as u64;
                    buffer.drain(..head_end + len);
                    tokio::time::sleep(delay).await;
                    records.fetch_add(count, Ordering::SeqCst);
                    posts.fetch_add(1, Ordering::SeqCst);
                    let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                    if socket.write_all(reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    format!("http://{address}/hook")
}

async fn run(delay_ms: u64, record_count: u64, data_bytes: usize, seconds: u64) {
    let mut config = manual_config(2);
    config.sink.allow_private_addresses = true;
    config.sink.tick_ms = 2;
    config.sink.request_timeout_ms = 20_000;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, "bench").await;

    let data = vec![7u8; data_bytes];
    let per_publish = 64u64;
    let mut seq = 0;
    let mut published = 0;
    while published < record_count {
        let mut records = Vec::new();
        let count = per_publish.min(record_count - published);
        for _ in 0..count {
            add_record(&mut records, 0, &data, 1);
        }
        seq += 1;
        client
            .publish(publish("bench", &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        published += count;
    }

    let (records, posts) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let delay = Duration::from_millis(delay_ms);
    let url = receiver(delay, Arc::clone(&records), Arc::clone(&posts)).await;
    let spec = SinkSpec {
        kind: "webhook.deliver".to_owned(),
        url: Some(url),
        secret: Some("bench".to_owned()),
        on_expire: "stop".to_owned(),
        ..SinkSpec::default()
    };
    let started = Instant::now();
    client
        .sink_op(
            key("bench"),
            SinkOp::Put {
                sink_id: 1,
                spec: Box::new(spec),
                start: SinkStart::Earliest,
            },
        )
        .await
        .unwrap();
    let limit = Duration::from_secs(seconds);
    while records.load(Ordering::SeqCst) < record_count && started.elapsed() < limit {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let (got, sent) = (records.load(Ordering::SeqCst), posts.load(Ordering::SeqCst));
    println!(
        "| {delay_ms} | {data_bytes} | {got}/{record_count} | {sent} | {:.0} | {:.1} | {:.2} |",
        got as f64 / elapsed,
        got as f64 * data_bytes as f64 / elapsed / 1_048_576.0,
        elapsed,
    );
    drop(client);
    drop(harness.finish());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a benchmark, not a test"]
async fn webhook_deliver_drain_rate() {
    let delays = std::env::var("BENCH_DELAYS_MS").unwrap_or_else(|_| "0,5,20".to_owned());
    let record_count = env_u64("BENCH_RECORDS", 20_000);
    let data_bytes = env_u64("BENCH_BYTES", 1024) as usize;
    let seconds = env_u64("BENCH_SECONDS", 10);
    println!("| reply delay ms | record bytes | delivered | POSTs | records/s | MiB/s | seconds |");
    println!("|---|---|---|---|---|---|---|");
    for delay in delays.split(',').filter_map(|delay| delay.parse().ok()) {
        run(delay, record_count, data_bytes, seconds).await;
    }
}
