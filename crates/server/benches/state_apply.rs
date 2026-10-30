//! Merge and incr by document size, up to the 64 KB limit. See docs/BENCH.md.

use std::hint::black_box;

use bytes::Bytes;
use ciborium::value::Value;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use server::{MAX_STATE_BYTES, StateOp, apply_op};

const DOC_BYTES: [usize; 4] = [64, 1024, 16 * 1024, MAX_STATE_BYTES - 64];

fn cbor(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).unwrap();
    out
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

/// A flat map of counters and short strings, about `target_bytes` when encoded, which is
/// what section 10 expects state to look like: `count` is at the front.
fn doc(target_bytes: usize) -> Vec<u8> {
    let mut pairs = vec![(text("count"), Value::Integer(1.into()))];
    let mut index = 0;
    while cbor(&Value::Map(pairs.clone())).len() < target_bytes {
        pairs.push((text(&format!("key.{index}")), text("value.value")));
        index += 1;
        if index % 256 == 0 && cbor(&Value::Map(pairs.clone())).len() + 4096 < target_bytes {
            // Grow faster for the big ones. The measured doc is still flat.
            for _ in 0..200 {
                pairs.push((text(&format!("key.{index}")), text("value.value")));
                index += 1;
            }
        }
    }
    cbor(&Value::Map(pairs))
}

fn bench(c: &mut Criterion) {
    let merge = cbor(&Value::Map(vec![
        (text("status"), text("running")),
        (text("key.0"), Value::Null),
    ]));
    let incr = cbor(&Value::Map(vec![
        (text("path"), Value::Array(vec![text("count")])),
        (text("n"), Value::Integer(1.into())),
    ]));
    let mut group = c.benchmark_group("state_apply");
    for target_bytes in DOC_BYTES {
        let current = Bytes::from(doc(target_bytes));
        group.throughput(Throughput::Bytes(current.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("merge", current.len()),
            &current,
            |b, current| {
                b.iter(|| apply_op(black_box(current), StateOp::Merge, black_box(&merge)).unwrap())
            },
        );
        group.bench_with_input(
            BenchmarkId::new("incr", current.len()),
            &current,
            |b, current| {
                b.iter(|| apply_op(black_box(current), StateOp::Incr, black_box(&incr)).unwrap())
            },
        );
        group.bench_with_input(
            BenchmarkId::new("set", current.len()),
            &current,
            |b, current| b.iter(|| apply_op(&[], StateOp::Set, black_box(current)).unwrap()),
        );
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
