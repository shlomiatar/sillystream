//! Decode and encode rate of `PUBLISH`, `DATA` and `SUBSCRIBE`, by body size.
//! Decoding never touches `records`, so its time should not depend on their size;
//! encoding copies them, unless the sender uses `data_prefix`.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use proto::{
    AckLevel, ClientFrame, DEFAULT_MAX_FRAME_BYTES, Publish, ServerFrame, Subscribe, WireClause,
    WireClauses, data_prefix, decode_client, decode_server,
};

const RECORD_BYTES: [usize; 4] = [64, 4096, 65_536, 1 << 20];

fn publish_frame(records: &[u8]) -> ClientFrame<'_> {
    ClientFrame::Publish(Publish {
        handle: 5,
        producer_id: 7,
        producer_seq: 42,
        ack: AckLevel::WalFsync,
        state: None,
        records,
    })
}

fn subscribe_frame(clause_count: usize, stream: &str) -> ClientFrame<'_> {
    let clause = WireClause {
        mask: 0xFF00,
        value: 0x1200,
        op: 0,
    };
    ClientFrame::Subscribe(Subscribe {
        sub: 1,
        from: 0,
        linger_ms: 5,
        author: None,
        clauses: WireClauses::new(&vec![clause; clause_count]).unwrap(),
        stream,
        name: None,
        with_state: false,
        state_only: false,
        include_system: false,
    })
}

fn bench_sized(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_codec");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    for size in RECORD_BYTES {
        let records = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));

        let frame = publish_frame(&records);
        let mut encoded = Vec::new();
        frame.encode(&mut encoded).unwrap();
        group.bench_function(BenchmarkId::new("publish_decode", size), |b| {
            b.iter(|| decode_client(black_box(&encoded), DEFAULT_MAX_FRAME_BYTES).unwrap())
        });
        let mut out = Vec::with_capacity(encoded.len());
        group.bench_function(BenchmarkId::new("publish_encode", size), |b| {
            b.iter(|| {
                out.clear();
                black_box(&frame).encode(&mut out).unwrap();
                black_box(out.len())
            })
        });

        let frame = ServerFrame::Data {
            sub: 1,
            records: &records,
        };
        let mut encoded = Vec::new();
        frame.encode(&mut encoded).unwrap();
        group.bench_function(BenchmarkId::new("data_decode", size), |b| {
            b.iter(|| decode_server(black_box(&encoded), DEFAULT_MAX_FRAME_BYTES).unwrap())
        });
        let mut out = Vec::with_capacity(encoded.len());
        group.bench_function(BenchmarkId::new("data_encode", size), |b| {
            b.iter(|| {
                out.clear();
                black_box(&frame).encode(&mut out).unwrap();
                black_box(out.len())
            })
        });
        group.bench_function(BenchmarkId::new("data_prefix", size), |b| {
            b.iter(|| data_prefix(black_box(1), black_box(size)).unwrap())
        });
    }
    group.finish();
}

fn bench_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_codec_subscribe");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));
    let long_stream = "s".repeat(1024);
    for (label, clause_count, stream) in [
        ("0_clauses_short_stream", 0, "logs.payments.3"),
        ("4_clauses_short_stream", 4, "logs.payments.3"),
        ("4_clauses_1kib_stream", 4, long_stream.as_str()),
    ] {
        let frame = subscribe_frame(clause_count, stream);
        let mut encoded = Vec::new();
        frame.encode(&mut encoded).unwrap();
        group.bench_function(BenchmarkId::new("decode", label), |b| {
            b.iter(|| decode_client(black_box(&encoded), DEFAULT_MAX_FRAME_BYTES).unwrap())
        });
        let mut out = Vec::with_capacity(encoded.len());
        group.bench_function(BenchmarkId::new("encode", label), |b| {
            b.iter(|| {
                out.clear();
                black_box(&frame).encode(&mut out).unwrap();
                black_box(out.len())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sized, bench_subscribe);
criterion_main!(benches);
