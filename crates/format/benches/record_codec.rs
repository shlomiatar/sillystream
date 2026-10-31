//! One record encoded into a reused buffer and decoded back, and a walk over many, by data
//! size. Decoding reads headers and slices: the data isn't copied or looked at (SPEC section 2).

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use format::{RecordHeader, Records, decode_record, encode_record};

const DATA_BYTES: [usize; 4] = [0, 64, 4096, 1024 * 1024];

fn header(data_bytes: usize) -> RecordHeader {
    RecordHeader {
        id: 0,
        options: 3,
        author: 0,
        len: data_bytes as u32,
        count: 1,
        codec: 0,
    }
}

fn one_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_codec");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    for data_bytes in DATA_BYTES {
        let data = vec![0xABu8; data_bytes];
        let head = header(data_bytes);
        let mut buffer = Vec::with_capacity(data_bytes + 64);
        group.throughput(Throughput::Bytes(32 + data_bytes as u64));
        group.bench_function(BenchmarkId::new("encode", data_bytes), |b| {
            b.iter(|| {
                buffer.clear();
                encode_record(&mut buffer, black_box(&head), black_box(&data)).unwrap();
                black_box(buffer.len())
            })
        });
        group.bench_function(BenchmarkId::new("decode", data_bytes), |b| {
            b.iter(|| black_box(decode_record(black_box(&buffer)).unwrap().0.header.len))
        });
    }
    group.finish();
}

fn walk(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_walk");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    for record_count in [12u64, 1_000, 100_000] {
        let data = [0xABu8; 64];
        let mut bytes = Vec::new();
        for _ in 0..record_count {
            encode_record(&mut bytes, &header(data.len()), &data).unwrap();
        }
        group.throughput(Throughput::Elements(record_count));
        group.bench_function(BenchmarkId::from_parameter(record_count), |b| {
            b.iter(|| black_box(Records::new(black_box(&bytes)).count()))
        });
    }
    group.finish();
}

criterion_group!(benches, one_record, walk);
criterion_main!(benches);
