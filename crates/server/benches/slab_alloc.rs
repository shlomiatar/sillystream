//! Section 6.5's choice, measured: records copied into slabs against one allocation per record.
//! Each iteration takes `RECORD_COUNT` records of a size off a "socket", holds them all (as the
//! WAL, the ring and the uploader do), and lets them go.

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use server::{SlabAccount, SlabBytes, SlabPool};

const RECORD_COUNT: usize = 1_000;
const SLAB_BYTES: usize = 4 * 1024 * 1024;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab_alloc");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(RECORD_COUNT as u64));
    for record_bytes in [64usize, 288, 4128] {
        let source = vec![0xABu8; record_bytes * RECORD_COUNT];
        let mut pool = SlabPool::new(SLAB_BYTES, SlabAccount::new(), None, 0);
        let mut held_slab: Vec<SlabBytes> = Vec::with_capacity(RECORD_COUNT);
        let mut held_alloc: Vec<Bytes> = Vec::with_capacity(RECORD_COUNT);

        // What the write path does with a publish of 1,000: one region, one reference.
        group.bench_function(BenchmarkId::new("slab_one_publish", record_bytes), |b| {
            b.iter(|| {
                let frozen = pool.copy_in(black_box(&source)).freeze();
                black_box(&frozen);
            })
        });
        // 1,000 publishes of one record: a region and a reference each.
        group.bench_function(BenchmarkId::new("slab_per_publish", record_bytes), |b| {
            b.iter(|| {
                for record in source.chunks_exact(record_bytes) {
                    held_slab.push(pool.copy_in(black_box(record)).freeze());
                }
                held_slab.clear();
            })
        });
        // The same records, with no slab.
        group.bench_function(BenchmarkId::new("alloc_per_record", record_bytes), |b| {
            b.iter(|| {
                for record in source.chunks_exact(record_bytes) {
                    held_alloc.push(Bytes::copy_from_slice(black_box(record)));
                }
                held_alloc.clear();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
