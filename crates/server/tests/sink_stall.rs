//! A stalled sink doesn't slow publishing (SPEC section 9), measured against a baseline. Alone in
//! its binary, so that no other test's load falls on one side of the comparison.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use server::{Client, NodeConfig, SinkOp, SinkSpec, SinkStart};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const STREAM: &str = "stall.source";
const RECORD_COUNT: u64 = 30_000;
const RECORD_BYTES: usize = 1024;
const STALLED_SINK_COUNT: u64 = 16;
/// More than each sink's budget, window and queue together, so that the sinks are all stalled
/// before anything is measured, and the baseline's node is in the same state of its log.
const WARM_RECORD_COUNT: u64 = 40_000;
const ROUNDS: usize = 5;

fn config() -> NodeConfig {
    let mut config = manual_config(4);
    config.sink.allow_private_addresses = true;
    config.sink.request_timeout_ms = 3_600_000;
    // A low cap, so that any slab a stalled sink kept would soon stop publishers.
    config.memory_cap_bytes = 64 * 1024 * 1024;
    config.slab_bytes = 1024 * 1024;
    config
}

/// Accepts and reads, and never answers.
async fn stalled_receiver() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut sink = vec![0u8; 64 * 1024];
                while matches!(socket.read(&mut sink).await, Ok(read) if read > 0) {}
            });
        }
    });
    format!("http://{address}/")
}

/// Publishes everything at level 2, as fast as acks return, and gives the rate and the slowest
/// ack in one window of 1,000.
async fn run_publishes(client: &mut Client, first_seq: u64, count: u64) -> (f64, Duration) {
    let records = record_with(1, &vec![9u8; RECORD_BYTES]);
    let started = Instant::now();
    let mut slowest = Duration::ZERO;
    let mut window_start = Instant::now();
    let mut sent = 0;
    let mut acked = 0;
    while acked < count {
        while sent < count && sent - acked < 1_000 {
            client
                .publish(publish(STREAM, &records, first_seq + sent, 2))
                .await
                .unwrap();
            sent += 1;
        }
        expect_ack(client).await;
        acked += 1;
        if acked % 1_000 == 0 {
            slowest = slowest.max(window_start.elapsed());
            window_start = Instant::now();
        }
    }
    (count as f64 / started.elapsed().as_secs_f64(), slowest)
}

struct Run {
    fill_rate: f64,
    rate: f64,
    slowest: Duration,
}

async fn one_run(sink_count: u64, url: &str) -> Run {
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    for sink_id in 1..=sink_count {
        client
            .sink_op(
                key(STREAM),
                SinkOp::Put {
                    sink_id,
                    spec: Box::new(SinkSpec {
                        kind: "webhook.deliver".to_owned(),
                        url: Some(url.to_owned()),
                        secret: Some("k".to_owned()),
                        on_expire: "stop".to_owned(),
                        ..SinkSpec::default()
                    }),
                    start: SinkStart::Earliest,
                },
            )
            .await
            .unwrap();
    }
    let (fill_rate, _) = run_publishes(&mut client, 1, WARM_RECORD_COUNT).await;
    // Let the sinks take what they will. Sealing lets the slabs go if nothing else holds them.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (rate, slowest) = run_publishes(&mut client, 1 + WARM_RECORD_COUNT, RECORD_COUNT).await;
    let info = client.stream_info(key(STREAM)).await.unwrap();
    for (_, _, cursor) in &info.sinks {
        assert_eq!(*cursor, Some(0), "these sinks really are stalled");
    }
    drop(client);
    harness.finish();
    Run {
        fill_rate,
        rate,
        slowest,
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_sink_doesnt_slow_publishing() {
    let url = Arc::new(stalled_receiver().await);
    let (mut baseline, mut stalled) = (Vec::new(), Vec::new());
    let (mut baseline_fill, mut stalled_fill) = (Vec::new(), Vec::new());
    let (mut baseline_slowest, mut stalled_slowest) = (Duration::ZERO, Duration::ZERO);
    for _ in 0..ROUNDS {
        let run = one_run(0, &url).await;
        baseline.push(run.rate);
        baseline_fill.push(run.fill_rate);
        baseline_slowest = baseline_slowest.max(run.slowest);
        let run = one_run(STALLED_SINK_COUNT, &url).await;
        stalled.push(run.rate);
        stalled_fill.push(run.fill_rate);
        stalled_slowest = stalled_slowest.max(run.slowest);
    }
    let baseline_median = median(&mut baseline);
    let stalled_median = median(&mut stalled);
    let ratio = stalled_median / baseline_median;
    println!(
        "SINK_STALL records={RECORD_COUNT} bytes={RECORD_BYTES} rounds={ROUNDS} \
         baseline_rec_s={baseline_median:.0} stalled_rec_s={stalled_median:.0} \
         stalled_sinks={STALLED_SINK_COUNT} ratio={ratio:.3} \
         baseline_slowest_1000_ms={} stalled_slowest_1000_ms={} \
         while_filling_baseline_rec_s={:.0} while_filling_stalled_rec_s={:.0}",
        baseline_slowest.as_millis(),
        stalled_slowest.as_millis(),
        median(&mut baseline_fill),
        median(&mut stalled_fill)
    );
    // While a sink fills its budget it is a subscriber like any other, which costs what fan-out
    // costs, and that is reported above. Once stalled it should cost nothing.
    assert!(
        ratio > 0.7,
        "publishing fell to {ratio:.3} of its baseline: {stalled:?} against {baseline:?}"
    );
    assert!(
        stalled_slowest < Duration::from_secs(5),
        "a window of acks took {stalled_slowest:?}"
    );
}
