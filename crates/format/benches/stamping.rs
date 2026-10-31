//! ns per publish, by records per publish and data size. Validation and stamping
//! must be linear in records and independent of data size (SPEC section 3).

use std::hint::black_box;
use std::num::NonZeroU64;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use format::{RecordHeader, encode_record, validate_publish};

const RECORD_COUNTS: [u64; 5] = [1, 12, 100, 1_000, 10_000];
const DATA_BYTES: [usize; 3] = [8, 256, 4096];
const AUTHOR: NonZeroU64 = NonZeroU64::new(7).unwrap();
const FIRST_ID: u64 = 1_800_000_000_000 << 16;

fn publish(record_count: u64, data_bytes: usize) -> Vec<u8> {
    let data = vec![0xABu8; data_bytes];
    let header = RecordHeader {
        id: 0,
        options: 3,
        author: 0,
        len: data_bytes as u32,
        count: 1,
        codec: 0,
    };
    let mut bytes = Vec::with_capacity((32 + data_bytes) * record_count as usize);
    for _ in 0..record_count {
        encode_record(&mut bytes, &header, &data).unwrap();
    }
    bytes
}

/// Lengths from 0 to 4,095 bytes, so that the walk's next offset can't be predicted.
fn mixed_publish(record_count: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    for _ in 0..record_count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let data = vec![0xABu8; (state % 4096) as usize];
        let header = RecordHeader {
            id: 0,
            options: 3,
            author: 0,
            len: data.len() as u32,
            count: 1,
            codec: 0,
        };
        encode_record(&mut bytes, &header, &data).unwrap();
    }
    bytes
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("stamping");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    for record_count in RECORD_COUNTS {
        group.throughput(Throughput::Elements(record_count));
        let uniform = DATA_BYTES.map(|data_bytes| {
            (
                format!("{record_count}_records/{data_bytes}_bytes"),
                publish(record_count, data_bytes),
            )
        });
        let mixed = (
            format!("{record_count}_records/mixed_bytes"),
            mixed_publish(record_count),
        );
        for (label, mut bytes) in uniform.into_iter().chain([mixed]) {
            group.bench_function(BenchmarkId::new("validate", &label), |b| {
                b.iter(|| {
                    black_box(
                        validate_publish(black_box(&mut bytes))
                            .unwrap()
                            .item_count(),
                    )
                })
            });
            let mut validated = validate_publish(&mut bytes).unwrap();
            let mut first_id = FIRST_ID;
            group.bench_function(BenchmarkId::new("stamp", &label), |b| {
                b.iter(|| {
                    validated.stamp(black_box(first_id), AUTHOR);
                    first_id += record_count;
                    black_box(validated.as_bytes().as_ptr());
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
