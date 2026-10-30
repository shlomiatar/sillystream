//! Segment build rate, and footer lookup time and read count on the real disk, by
//! stream count. See docs/BENCH.md.

#[path = "../tests/common/mod.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{CountingStore, put_parts, record};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use storage::{BuiltSegment, SegmentBuilder, SegmentReader};

const STREAM_COUNTS: [u64; 3] = [1, 1_000, 1_000_000];
const FIRST_ID: u64 = 1_800_000_000_000 << 16;

fn names(stream_count: u64) -> Vec<String> {
    (0..stream_count)
        .map(|index| format!("logs.{index}"))
        .collect()
}

fn build(names: &[String], payload: &Bytes) -> BuiltSegment {
    let mut builder = SegmentBuilder::new();
    for name in names {
        builder.add_extent(name, [payload.clone()]).unwrap();
    }
    builder.finish().unwrap()
}

fn build_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_build");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(10));
    let payload = Bytes::from(record(FIRST_ID, 1, 7, &[0xAB; 64]));
    for stream_count in STREAM_COUNTS {
        let names = names(stream_count);
        group.throughput(Throughput::Elements(stream_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(stream_count),
            &names,
            |b, names| b.iter(|| black_box(build(names, &payload))),
        );
    }
    // One stream holding 32 MiB of 4 KiB records: bytes per second, blocks and CRCs.
    let mut wire = Vec::new();
    let mut id = FIRST_ID;
    while wire.len() < 32 * 1024 * 1024 {
        wire.extend(record(id, 1, 7, &[0xCD; 4064]));
        id += 1;
    }
    let wire = Bytes::from(wire);
    group.throughput(Throughput::Bytes(wire.len() as u64));
    group.bench_function("one_stream_32MiB", |b| {
        b.iter(|| {
            let mut builder = SegmentBuilder::new();
            builder.add_extent("logs.big", [wire.clone()]).unwrap();
            black_box(builder.finish().unwrap())
        })
    });
    group.finish();
}

fn lookup(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let local: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let mut group = c.benchmark_group("segment_lookup");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    let payload = Bytes::from(record(FIRST_ID, 1, 7, &[0xAB; 64]));

    for stream_count in STREAM_COUNTS {
        let names = names(stream_count);
        let built = build(&names, &payload);
        let path = Path::from(format!("sealed/{stream_count}.seg"));
        let counting = Arc::new(CountingStore::new(Arc::clone(&local)));
        let store: Arc<dyn ObjectStore> = counting.clone();
        let started = Instant::now();
        runtime.block_on(put_parts(store.as_ref(), &path, &built.parts));
        let put_elapsed = started.elapsed();
        let reader = SegmentReader::new(store, path);
        let middle = &names[names.len() / 2];

        // Read counts, printed once: they are facts, not timings.
        counting.reset_gets();
        let top = runtime.block_on(reader.read_top_index()).unwrap();
        let top_reads = counting.gets();
        counting.reset_gets();
        let extents = runtime.block_on(reader.lookup(&top, middle)).unwrap();
        let page_reads = counting.gets();
        counting.reset_gets();
        runtime.block_on(reader.read_extent(&extents[0])).unwrap();
        let data_reads = counting.gets();
        println!(
            "STATS {stream_count} streams: object {} MiB, PUT {put_elapsed:?}, {} pages, \
             reads: top {top_reads}, page {page_reads}, data {data_reads}",
            built.len_bytes / (1024 * 1024),
            top.page_count(),
        );

        let mut index = 0usize;
        let mut next = move || {
            index = (index + 7919) % names.len();
            names[index].clone()
        };
        group.bench_function(BenchmarkId::new("full", stream_count), |b| {
            b.iter(|| {
                let name = next();
                runtime.block_on(async {
                    let top = reader.read_top_index().await.unwrap();
                    let extents = reader.lookup(&top, &name).await.unwrap();
                    black_box(reader.read_extent(&extents[0]).await.unwrap())
                })
            })
        });
        group.bench_function(BenchmarkId::new("cached_top", stream_count), |b| {
            b.iter(|| {
                let name = next();
                runtime.block_on(async {
                    let extents = reader.lookup(&top, &name).await.unwrap();
                    black_box(reader.read_extent(&extents[0]).await.unwrap())
                })
            })
        });
        group.bench_function(BenchmarkId::new("stored_extent", stream_count), |b| {
            b.iter(|| {
                runtime
                    .block_on(async { black_box(reader.read_extent(&extents[0]).await.unwrap()) })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, build_rate, lookup);
criterion_main!(benches);
