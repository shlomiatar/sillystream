//! The write path through real cores: publish to durable on the real disk, publish to
//! memory with no disk at all, dedupe lookup, and slab count changes per record. See
//! docs/BENCH.md.

use std::hint::black_box;
use std::io::{self, IoSlice};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use format::{RecordHeader, encode_record, make_id};
use object_store::memory::InMemory;
use server::{DedupeTable, Node, NodeConfig, Publish, Reply, SlabHook, SystemClock, shard_of};
use storage::{DirVolume, WalFile, WalVolume};

const AUTHOR: NonZeroU64 = NonZeroU64::new(9).unwrap();
const DATA_BYTES: usize = 256;
const CORE_COUNTS: [usize; 3] = [1, 4, 16];
const RECORD_COUNTS: [usize; 3] = [1, 100, 10_000];
const WINDOW_COUNT: u64 = 256;
const NO_DISK_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

fn records(record_count: usize) -> Vec<u8> {
    let data = vec![0xAB; DATA_BYTES];
    let mut out = Vec::new();
    for index in 0..record_count {
        let header = RecordHeader {
            id: 0,
            options: index as u64,
            author: 0,
            len: DATA_BYTES as u32,
            count: 1,
            codec: 0,
        };
        encode_record(&mut out, &header, &data).unwrap();
    }
    out
}

/// Discards writes and syncs at once: what's left is the cost of the cores and channels.
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

fn config(core_count: usize) -> NodeConfig {
    let mut config = NodeConfig::new(core_count);
    config.seal_window_ms = None;
    config.rebalance_period_ms = None;
    // This bench measures to durable. Nothing pinned is ever let go in it.
    config.memory_cap_bytes = u64::MAX;
    config.segment_cap_bytes = u64::MAX;
    config.stream_buffer_bytes = u64::MAX;
    config
}

/// One stream name per core, so `core_count` clients each load one core.
fn stream_per_core(node: &Node, core_count: usize, placement: &[u16]) -> Vec<String> {
    let mut names = vec![None; core_count];
    let mut index = 0;
    while names.iter().any(Option::is_none) {
        let name = format!("bench.{index}");
        let core = placement[shard_of(&name, node.virtual_shard_count()) as usize] as usize;
        names[core].get_or_insert(name);
        index += 1;
    }
    names.into_iter().map(Option::unwrap).collect()
}

/// `publish_count` publishes per core, at `ack_level`, with `window` outstanding at a time.
/// Each stream has its own thread, as a connection would.
fn run_load(
    node: &Node,
    core_count: usize,
    bytes: &[u8],
    publish_count: u64,
    window: u64,
    ack_level: u8,
    first_seq: u64,
) -> Duration {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let placement = runtime.block_on(node.placement());
    let names = stream_per_core(node, core_count, &placement);
    let mut clients = Vec::new();
    for core in 0..core_count {
        clients.push(runtime.block_on(node.client(core)));
    }
    let start = Instant::now();
    std::thread::scope(|scope| {
        for (client, name) in clients.into_iter().zip(&names) {
            let mut client = client;
            scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let mut acked = 0;
                    let mut sent = 0;
                    while acked < publish_count {
                        while sent < publish_count && sent - acked < window {
                            client
                                .publish(Publish {
                                    stream: name,
                                    handle: 1,
                                    producer_id: 1,
                                    producer_seq: first_seq + sent,
                                    ack_level: Some(ack_level),
                                    author: AUTHOR,
                                    state: None,
                                    records: bytes,
                                })
                                .await
                                .unwrap();
                            sent += 1;
                        }
                        match client.recv().await.unwrap() {
                            Reply::Ack { .. } => acked += 1,
                            other => panic!("{other:?}"),
                        }
                    }
                });
            });
        }
    });
    start.elapsed()
}

fn scale(duration: Duration, iters: u64, runs: u64) -> Duration {
    duration.mul_f64(iters as f64 / runs as f64)
}

fn bench_dir() -> std::path::PathBuf {
    // In the target dir, so that it's on the disk the build uses and not in tmpfs.
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/bench-wal");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Publish to durable with a real `sync_data`, by outstanding publishes and core count.
fn publish_to_durable(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/publish_to_durable");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(4));
    for core_count in CORE_COUNTS {
        for window in [1, WINDOW_COUNT] {
            let bytes = records(1);
            group.throughput(Throughput::Elements(core_count as u64));
            let id = BenchmarkId::new(format!("window_{window}"), core_count);
            group.bench_with_input(id, &bytes, |b, bytes| {
                let dir = tempfile::tempdir_in(bench_dir()).unwrap();
                let node = Node::start(
                    config(core_count),
                    DirVolume::new(dir.path(), 64 * 1024 * 1024),
                    Arc::new(InMemory::new()),
                    Arc::new(SystemClock),
                )
                .unwrap();
                let mut seq = 1;
                b.iter_custom(|iters| {
                    // Enough for a measurable run and no more: a sync is ms here.
                    let per_core = iters.max(1).min(if window == 1 { 200 } else { 4000 });
                    let took = run_load(&node, core_count, bytes, per_core, window, 2, seq);
                    seq += per_core;
                    scale(took, iters, per_core)
                });
                node.shutdown();
            });
        }
    }
    group.finish();
}

/// The same path with no disk, so the numbers are what the cores and channels cost:
/// 1 record and bigger batches, which must not cost per record beyond the validation walk.
fn publish_no_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/publish_no_disk");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    for core_count in CORE_COUNTS {
        for record_count in RECORD_COUNTS {
            let bytes = records(record_count);
            group.throughput(Throughput::Bytes((bytes.len() * core_count) as u64));
            let id = BenchmarkId::new(format!("records_{record_count}"), core_count);
            group.bench_with_input(id, &bytes, |b, bytes| {
                b.iter_custom(|iters| {
                    // Batches stay in memory, since nothing seals, so a new node each run
                    // lets them go, and the budget bounds them.
                    let cap = (NO_DISK_BUDGET_BYTES / bytes.len() as u64).clamp(1, 20_000);
                    let per_core = iters.max(1).min(cap);
                    let node = Node::start(
                        config(core_count),
                        NullVolume,
                        Arc::new(InMemory::new()),
                        Arc::new(SystemClock),
                    )
                    .unwrap();
                    let took = run_load(&node, core_count, bytes, per_core, WINDOW_COUNT, 2, 1);
                    node.shutdown();
                    scale(took, iters, per_core)
                });
            });
        }
    }
    group.finish();
}

fn dedupe_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/dedupe");
    let now_ms = 1_800_000_000_000;
    for entry_count in [1u64, 10, 100, 1000] {
        let mut table = DedupeTable::new();
        for producer in 0..entry_count {
            table.record(producer * 7, 5, make_id(now_ms, producer as u16), 1, now_ms);
        }
        group.bench_with_input(
            BenchmarkId::new("check", entry_count),
            &entry_count,
            |b, entry_count| {
                let mut at = 0;
                b.iter(|| {
                    at = (at + 1) % entry_count;
                    table.check(black_box(at * 7), black_box(6), now_ms)
                })
            },
        );
    }
    group.finish();
}

#[derive(Default)]
struct CountChanges(AtomicU64);

impl SlabHook for CountChanges {
    fn ref_changed(&self, _slab_id: u64, _delta: i32) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn slab_freed(&self, _slab_id: u64) {}
}

/// Not a time: the count of changes one publish causes all the way through its upload.
fn slab_changes_per_record(_c: &mut Criterion) {
    for record_count in RECORD_COUNTS {
        let hook = Arc::new(CountChanges::default());
        let mut config = config(1);
        let dyn_hook: Arc<dyn SlabHook> = hook.clone();
        config.slab_hook = Some(dyn_hook);
        let node = Node::start(
            config,
            NullVolume,
            Arc::new(InMemory::new()),
            Arc::new(SystemClock),
        )
        .unwrap();
        let bytes = records(record_count);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut client = node.client(0).await;
            let publish = Publish {
                stream: "s",
                handle: 1,
                producer_id: 1,
                producer_seq: 1,
                ack_level: Some(3),
                author: AUTHOR,
                state: None,
                records: &bytes,
            };
            client.publish(publish).await.unwrap();
            node.seal().await;
            client.recv().await.unwrap();
        });
        node.shutdown();
        let changes = hook.0.load(Ordering::Relaxed);
        println!(
            "slab_changes: records={record_count} changes={changes} per_record={:.4}",
            changes as f64 / record_count as f64
        );
    }
}

criterion_group!(
    benches,
    publish_to_durable,
    publish_no_disk,
    dedupe_lookup,
    slab_changes_per_record
);
criterion_main!(benches);
