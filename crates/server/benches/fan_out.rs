//! Fan-out through real cores: from a publish to every subscriber's connection, with no
//! disk. Bytes per second and ns per subscriber, by subscribers, distinct filters and core
//! count. See docs/BENCH.md.

use std::io::{self, IoSlice};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use format::{Clause, RecordHeader, encode_record};
use object_store::memory::InMemory;
use server::{
    Client, Event, Node, NodeConfig, Publish, Reply, Subscription, SystemClock, shard_of,
};
use storage::{WalFile, WalVolume};

const AUTHOR: NonZeroU64 = NonZeroU64::new(9).unwrap();
const DATA_BYTES: usize = 256;
const SUBSCRIBER_COUNTS: [usize; 3] = [1, 100, 10_000];
const FILTER_COUNTS: [usize; 4] = [1, 10, 100, 10_000];
const CORE_COUNTS: [usize; 2] = [1, 8];
const CONNECTION_COUNT: usize = 16;
const OUTSTANDING_COUNT: u64 = 64;
const SEAL_AT_BYTES: u64 = 256 * 1024 * 1024;

struct NullFile;

impl WalFile for NullFile {
    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        Ok(slices.iter().map(|slice| slice.len()).sum())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct NullVolume;

impl WalVolume for NullVolume {
    type File = NullFile;

    fn create(&mut self, _seq: u64) -> io::Result<NullFile> {
        Ok(NullFile)
    }

    fn remove(&mut self, _seq: u64) -> io::Result<()> {
        Ok(())
    }
}

fn record(options: u64) -> Vec<u8> {
    let header = RecordHeader {
        id: 0,
        options,
        author: 0,
        len: DATA_BYTES as u32,
        count: 1,
        codec: 0,
    };
    let mut out = Vec::new();
    encode_record(&mut out, &header, &[0xAB; DATA_BYTES]).unwrap();
    out
}

/// A stream on a core other than the connections' home whenever there is one.
fn stream_on_core_zero(virtual_shard_count: u32, core_count: usize) -> String {
    (0..)
        .map(|n| format!("fan.{n}"))
        .find(|name| (shard_of(name, virtual_shard_count) as usize).is_multiple_of(core_count))
        .unwrap()
}

struct Rig {
    node: Node,
    client: Client,
    stream: String,
    received_bytes: Arc<AtomicU64>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    seq: u64,
}

/// `filter_count` distinct filters that every record matches, shared evenly.
async fn start(core_count: usize, subscriber_count: usize, filter_count: usize) -> Rig {
    let mut config = NodeConfig::new(core_count);
    config.seal_window_ms = None;
    config.rebalance_period_ms = None;
    config.segment_cap_bytes = u64::MAX;
    config.stream_buffer_bytes = u64::MAX;
    config.max_subscriptions_per_connection = usize::MAX;
    config.subscriber_queue_bytes = u64::MAX / 2;
    config.connection_window_bytes = 1 << 40;
    let node = Node::start(
        config,
        NullVolume,
        Arc::new(InMemory::new()),
        Arc::new(SystemClock),
    )
    .unwrap();
    let mut client = node.client(0).await;
    let stream = stream_on_core_zero(node.virtual_shard_count(), core_count);
    client
        .configure(&stream, server::StreamConfig::default())
        .await;

    let received_bytes = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    let mut handles = Vec::new();
    for index in 0..CONNECTION_COUNT.min(subscriber_count) {
        handles.push((index, node.connection(index % core_count).await.unwrap()));
    }
    let connection_count = handles.len();
    let clauses: Vec<[Clause; 1]> = (0..filter_count)
        .map(|index| {
            [Clause::Eq {
                mask: (index as u64 + 1) << 32,
                value: 0,
            }]
        })
        .collect();
    for sub in 0..subscriber_count {
        let mut subscription = Subscription::new(sub as u32, &stream, u64::MAX);
        subscription.clauses = &clauses[sub % filter_count];
        handles[sub % connection_count].1.subscribe(subscription);
    }
    for (index, mut connection) in handles {
        let want = subscriber_count / connection_count
            + usize::from(index < subscriber_count % connection_count);
        let mut subscribed = 0;
        while subscribed < want {
            if let Some(Event::Subscribed { .. }) = connection.recv().await {
                subscribed += 1;
            }
        }
        let received_bytes = Arc::clone(&received_bytes);
        readers.push(tokio::spawn(async move {
            while let Some(event) = connection.recv().await {
                if let Event::Data { records, .. } = event {
                    received_bytes.fetch_add(records.len_bytes(), Ordering::Relaxed);
                }
            }
        }));
    }
    Rig {
        node,
        client,
        stream,
        received_bytes,
        readers,
        seq: 0,
    }
}

/// Time for `iters` publishes to reach every subscriber, a window at a time so that the
/// publisher doesn't simply run ahead of what is measured.
async fn run(rig: &mut Rig, iters: u64, subscriber_count: u64) -> Duration {
    let records = record(1);
    let per_publish_bytes = records.len() as u64 * subscriber_count;
    let mut elapsed = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let batch = left.min(OUTSTANDING_COUNT);
        let target = rig.received_bytes.load(Ordering::Relaxed) + batch * per_publish_bytes;
        let started = Instant::now();
        for _ in 0..batch {
            rig.seq += 1;
            rig.client
                .publish(Publish {
                    stream: &rig.stream,
                    handle: 1,
                    producer_id: 1,
                    producer_seq: rig.seq,
                    ack_level: Some(1),
                    author: AUTHOR,
                    state: None,
                    records: &records,
                })
                .await
                .unwrap();
        }
        while rig.received_bytes.load(Ordering::Relaxed) < target {
            tokio::task::yield_now().await;
        }
        elapsed += started.elapsed();
        for _ in 0..batch {
            assert!(matches!(rig.client.recv().await, Some(Reply::Ack { .. })));
        }
        left -= batch;
        if rig.client.pinned_bytes() > SEAL_AT_BYTES {
            rig.client.retire_slab();
            rig.node.seal().await;
        }
    }
    elapsed
}

fn fan_out(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_time()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("fan_out");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    for core_count in CORE_COUNTS {
        for subscriber_count in SUBSCRIBER_COUNTS {
            for filter_count in FILTER_COUNTS {
                if filter_count > subscriber_count {
                    continue;
                }
                let mut rig = runtime.block_on(start(core_count, subscriber_count, filter_count));
                let id = format!("{core_count}c/{subscriber_count}s/{filter_count}f");
                group.throughput(Throughput::Bytes(
                    (DATA_BYTES as u64 + 32) * subscriber_count as u64,
                ));
                group.bench_function(BenchmarkId::from_parameter(id), |b| {
                    b.iter_custom(|iters| {
                        runtime.block_on(run(&mut rig, iters, subscriber_count as u64))
                    });
                });
                let Rig {
                    node,
                    readers,
                    client,
                    ..
                } = rig;
                drop(client);
                node.shutdown();
                runtime.block_on(async {
                    for reader in readers {
                        let _ = reader.await;
                    }
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, fan_out);
criterion_main!(benches);
