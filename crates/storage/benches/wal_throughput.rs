//! The WAL writer on the real disk: entries, bytes, fsyncs and group sizes, by entry
//! size and sender count, with real `sync_data`. See docs/BENCH.md.

use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use storage::{
    DirVolume, WalConfig, WalEntry, WalSendError, WalStats, durable_watches, start_wal_writer,
};

const ENTRY_BYTES: [usize; 3] = [256, 4096, 64 * 1024];
const SENDER_COUNTS: [usize; 3] = [1, 4, 16];
const PREALLOCATE_BYTES: u64 = 64 * 1024 * 1024;
const CHANNEL_CAPACITY: usize = 4096;

/// Totals across a benchmark's runs, printed after it, since criterion only reports time.
#[derive(Default)]
struct Totals {
    stats: WalStats,
    elapsed: Duration,
}

fn run_once(entry_bytes: usize, sender_count: usize, entry_count: u64) -> (Duration, WalStats) {
    let dir = tempfile::tempdir_in(bench_dir()).unwrap();
    let (notify, _receivers) = durable_watches(sender_count);
    let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
        DirVolume::new(dir.path(), PREALLOCATE_BYTES),
        notify,
        (),
        WalConfig {
            first_seq: 1,
            closed_seqs: Vec::new(),
            channel_capacity: CHANNEL_CAPACITY,
            segment_cap_bytes: u64::MAX,
            stream_buffer_bytes: u64::MAX,
            carried: Vec::new(),
        },
    )
    .unwrap();
    let records = Bytes::from(vec![0xABu8; entry_bytes]);
    let stream: Arc<str> = Arc::from("bench.stream");
    let per_sender = entry_count.div_ceil(sender_count as u64);
    let start = Instant::now();
    let threads: Vec<_> = (0..sender_count)
        .map(|core| {
            let sender = sender.clone();
            let records = records.clone();
            let stream = Arc::clone(&stream);
            std::thread::spawn(move || {
                for ticket in 1..=per_sender {
                    let mut entry = WalEntry::new(
                        Arc::clone(&stream),
                        core as u64,
                        ticket,
                        records.clone(),
                        core,
                        ticket,
                    )
                    .unwrap();
                    loop {
                        match sender.try_append(entry) {
                            Ok(()) => break,
                            Err(WalSendError::Full(back)) => {
                                entry = back;
                                std::thread::yield_now();
                            }
                            Err(WalSendError::Stopped(_)) => panic!("writer stopped"),
                        }
                    }
                }
            })
        })
        .collect();
    drop(sender);
    for thread in threads {
        thread.join().unwrap();
    }
    let stats = handle.join().unwrap();
    (start.elapsed(), stats)
}

/// A directory on the real disk. The system temp dir is on the same APFS volume here,
/// and `SILLYSTREAM_BENCH_DIR` points it elsewhere.
fn bench_dir() -> std::path::PathBuf {
    std::env::var_os("SILLYSTREAM_BENCH_DIR")
        .map(Into::into)
        .unwrap_or_else(std::env::temp_dir)
}

fn free_running(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_throughput");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));
    for entry_bytes in ENTRY_BYTES {
        for sender_count in SENDER_COUNTS {
            group.throughput(Throughput::Bytes(entry_bytes as u64));
            let id = BenchmarkId::new(format!("{entry_bytes}B"), format!("{sender_count}s"));
            let totals = Arc::new(Mutex::new(Totals::default()));
            let run_totals = Arc::clone(&totals);
            group.bench_function(id, move |b| {
                b.iter_custom(|iters| {
                    let (elapsed, stats) = run_once(entry_bytes, sender_count, iters);
                    let mut totals = run_totals.lock().unwrap();
                    totals.elapsed += elapsed;
                    totals.stats.entry_count += stats.entry_count;
                    totals.stats.record_bytes += stats.record_bytes;
                    totals.stats.sync_count += stats.sync_count;
                    totals.stats.largest_group_count = totals
                        .stats
                        .largest_group_count
                        .max(stats.largest_group_count);
                    elapsed
                })
            });
            let totals = totals.lock().unwrap();
            let seconds = totals.elapsed.as_secs_f64();
            println!(
                "STATS {entry_bytes}B {sender_count}s: {:.0} entries/s, {:.1} MiB/s, {:.0} fsyncs/s, \
                 mean group {:.1}, largest group {}",
                totals.stats.entry_count as f64 / seconds,
                totals.stats.record_bytes as f64 / seconds / (1024.0 * 1024.0),
                totals.stats.sync_count as f64 / seconds,
                totals.stats.entry_count as f64 / totals.stats.sync_count.max(1) as f64,
                totals.stats.largest_group_count,
            );
        }
    }
    group.finish();
}

/// One entry at a time, waiting for its wake-up: a lone publisher's level 2 cost, from
/// `try_append` to the core's `watch` firing.
fn one_at_a_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_durable_wait");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for entry_bytes in ENTRY_BYTES {
        group.throughput(Throughput::Bytes(entry_bytes as u64));
        group.bench_function(format!("{entry_bytes}B"), |b| {
            let dir = tempfile::tempdir_in(bench_dir()).unwrap();
            let (notify, mut receivers) = durable_watches(1);
            let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
                DirVolume::new(dir.path(), PREALLOCATE_BYTES),
                notify,
                (),
                WalConfig {
                    first_seq: 1,
                    closed_seqs: Vec::new(),
                    channel_capacity: 16,
                    segment_cap_bytes: u64::MAX,
                    stream_buffer_bytes: u64::MAX,
                    carried: Vec::new(),
                },
            )
            .unwrap();
            let records = Bytes::from(vec![0xABu8; entry_bytes]);
            let stream: Arc<str> = Arc::from("bench.stream");
            let mut receiver = receivers.pop().unwrap();
            let mut ticket = 0;
            b.iter(|| {
                ticket += 1;
                let entry =
                    WalEntry::new(Arc::clone(&stream), 1, ticket, records.clone(), 0, ticket)
                        .unwrap();
                sender.try_append(entry).unwrap();
                runtime.block_on(async {
                    while *receiver.borrow_and_update() < ticket {
                        receiver.changed().await.unwrap();
                    }
                });
                black_box(ticket)
            });
            drop(sender);
            handle.join().unwrap();
        });
    }
    group.finish();
}

/// Group commit: `publisher_count` publishers, each waiting for its entry to be durable before
/// sending the next, as a level 2 producer with one outstanding does. Each has its own wake-up.
fn group_commit(c: &mut Criterion) {
    const ENTRY_BYTES: usize = 256;
    let mut group = c.benchmark_group("wal_group_commit");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    for publisher_count in [1usize, 4, 16, 64, 256, 1024] {
        group.throughput(Throughput::Elements(1));
        let totals = Arc::new(Mutex::new(Totals::default()));
        let run_totals = Arc::clone(&totals);
        let handle = runtime.handle().clone();
        group.bench_function(format!("{publisher_count}p"), move |b| {
            b.iter_custom(|iters| {
                let dir = tempfile::tempdir_in(bench_dir()).unwrap();
                let (notify, receivers) = durable_watches(publisher_count);
                let (sender, writer) = start_wal_writer::<Bytes, _, _, _>(
                    DirVolume::new(dir.path(), PREALLOCATE_BYTES),
                    notify,
                    (),
                    WalConfig {
                        first_seq: 1,
                        closed_seqs: Vec::new(),
                        channel_capacity: CHANNEL_CAPACITY,
                        segment_cap_bytes: u64::MAX,
                        stream_buffer_bytes: u64::MAX,
                        carried: Vec::new(),
                    },
                )
                .unwrap();
                let records = Bytes::from(vec![0xABu8; ENTRY_BYTES]);
                let stream: Arc<str> = Arc::from("bench.stream");
                let per_publisher = iters.div_ceil(publisher_count as u64);
                let start = Instant::now();
                let tasks: Vec<_> = receivers
                    .into_iter()
                    .enumerate()
                    .map(|(core, mut receiver)| {
                        let sender = sender.clone();
                        let records = records.clone();
                        let stream = Arc::clone(&stream);
                        handle.spawn(async move {
                            for ticket in 1..=per_publisher {
                                let entry = WalEntry::new(
                                    Arc::clone(&stream),
                                    core as u64,
                                    ticket,
                                    records.clone(),
                                    core,
                                    ticket,
                                )
                                .unwrap();
                                assert!(sender.try_append(entry).is_ok());
                                while *receiver.borrow_and_update() < ticket {
                                    receiver.changed().await.unwrap();
                                }
                            }
                        })
                    })
                    .collect();
                drop(sender);
                handle.block_on(async {
                    for task in tasks {
                        task.await.unwrap();
                    }
                });
                let elapsed = start.elapsed();
                let stats = writer.join().unwrap();
                let mut totals = run_totals.lock().unwrap();
                totals.elapsed += elapsed;
                totals.stats.entry_count += stats.entry_count;
                totals.stats.sync_count += stats.sync_count;
                totals.stats.largest_group_count = totals
                    .stats
                    .largest_group_count
                    .max(stats.largest_group_count);
                // Criterion's count is per entry, and rounding up to a whole round sent a few more.
                elapsed.mul_f64(iters as f64 / stats.entry_count.max(1) as f64)
            })
        });
        let totals = totals.lock().unwrap();
        let seconds = totals.elapsed.as_secs_f64();
        println!(
            "GROUP {publisher_count}p: {:.0} entries/s, {:.0} fsyncs/s, mean group {:.1}, \
             largest group {}",
            totals.stats.entry_count as f64 / seconds,
            totals.stats.sync_count as f64 / seconds,
            totals.stats.entry_count as f64 / totals.stats.sync_count.max(1) as f64,
            totals.stats.largest_group_count,
        );
    }
    group.finish();
}

criterion_group!(benches, free_running, one_at_a_time, group_commit);
criterion_main!(benches);
