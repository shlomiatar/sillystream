# Benchmarks

Numbers come from runs, and are never adjusted or estimated. Anything not run is listed as not run.

## Environment

- **Machine:** Apple M4 Max, 16 cores. **OS:** macOS 27.0.1. **Toolchain:** cargo 1.98.1, `--release` via `cargo bench`.
- **Commit:** the working tree on top of `a858b38`, committed together with this file. The filter, stamping and id code is unchanged from `a858b38`.
- **Load:** a shared laptop with a load average around 3 during the runs. No thread pinning (not available on macOS).

## M1: `filter_scan`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p format --bench filter_scan
```

(Numbers below came from `cargo bench -p format`, which runs both suites in turn.)

Headers per second, in millions, over 64 blocks of 1,024 records with 32 data bytes each (65,536 records, about 64 KiB per block). Each cell is `full scan / block skip`. Every run first asserts that both methods return the expected match count. One clause can match; the others never do.

`clustered` puts the matching records in as few blocks as possible, as sorting by options tends to. `scattered` spreads them evenly, the worst case for skipping.


**clustered**

| match rate | 1 clause | 2 clauses | 3 clauses | 4 clauses |
|---|---|---|---|---|
| 0.1% | 334 / 21,837 | 318 / 19,489 | 300 / 18,097 | 274 / 17,287 |
| 1% | 337 / 19,863 | 316 / 19,255 | 297 / 18,848 | 280 / 18,384 |
| 10% | 329 / 20,253 | 304 / 20,042 | 309 / 19,009 | 289 / 17,621 |
| 100% | 319 / 369,240 | 325 / 359,240 | 332 / 358,790 | 331 / 369,320 |

**scattered**

| match rate | 1 clause | 2 clauses | 3 clauses | 4 clauses |
|---|---|---|---|---|
| 0.1% | 332 / 333 | 312 / 314 | 299 / 292 | 281 / 271 |
| 1% | 331 / 335 | 312 / 304 | 301 / 300 | 277 / 280 |
| 10% | 331 / 331 | 316 / 320 | 300 / 297 | 277 / 258 |
| 100% | 318 / 359,060 | 334 / 355,850 | 325 / 355,890 | 323 / 350,470 |

Reading these:

- A full scan runs at roughly 270–340 M headers/s, or about 3 ns per record, and clauses cost little.
- Skipping pays only when summaries can decide. Clustered at 10% or below it is about 60× faster. Scattered at 0.1–10% every block holds a match and it gains nothing, but costs nothing measurable either.
- At 100% skipping reports about 350,000 M/s, because the `All` decision settles each block without opening it. This counts matches only. A real reader still has to send those bytes, so this isn't a delivery rate.

## M1: `stamping`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p format --bench stamping
```

Time per publish, then time per record. `validate` walks headers and padding, and `stamp` writes `id` and `author`. Records have `count = 1`. `mixed` has pseudo-random data lengths from 0 to 4,095 bytes.

| records | data per record | validate | validate per record | stamp | stamp per record |
|---|---|---|---|---|---|
| 1 | 8 B | 1.5 ns | 1.48 ns | 1.0 ns | 0.99 ns |
| 1 | 256 B | 1.5 ns | 1.49 ns | 1.0 ns | 1.00 ns |
| 1 | 4096 B | 1.5 ns | 1.49 ns | 1.0 ns | 0.99 ns |
| 1 | mixed | 1.7 ns | 1.73 ns | 1.0 ns | 1.00 ns |
| 100 | 8 B | 78.7 ns | 0.79 ns | 61.1 ns | 0.61 ns |
| 100 | 256 B | 91.3 ns | 0.91 ns | 62.1 ns | 0.62 ns |
| 100 | 4096 B | 87.1 ns | 0.87 ns | 63.0 ns | 0.63 ns |
| 100 | mixed | 221.1 ns | 2.21 ns | 207.9 ns | 2.08 ns |
| 10,000 | 8 B | 7.3 µs | 0.73 ns | 5.8 µs | 0.58 ns |
| 10,000 | 256 B | 14.4 µs | 1.44 ns | 14.1 µs | 1.41 ns |
| 10,000 | 4096 B | 16.4 µs | 1.64 ns | 14.8 µs | 1.48 ns |
| 10,000 | mixed | 78.1 µs | 7.81 ns | 76.5 µs | 7.65 ns |

Reading these:

- **Linear in records:** the walk does constant work per record, and with uniform lengths the cost per record stays flat from 100 to 10,000 records. With `mixed` lengths it rises as the publish outgrows the cache (see below).
- **Independent of data size:** the code never touches data, but the cost does vary with how far apart headers are. At 10,000 records, a stride beyond the cache costs about 2.5× the 8 B case, and that is memory access, not work on the data.
- **Uniform lengths are a best case.** Each record's offset depends on the previous header's `len`, and this CPU appears to predict repeated values. With `mixed` lengths the cost is 2 ns per record at 100 records and about 8 ns at 10,000. Use the `mixed` rows for real publishes.
- **Run-to-run variance:** an earlier run of the same code had `stamp` at 10,000 records with 256 B and 4,096 B at 6.8 µs and 7.0 µs, against about 14 µs here, so large publishes can differ by about 2× between runs. Criterion's narrow intervals don't capture this.

## Caveats

- One laptop, under whatever else it was running. Treat differences under about 2× as noise.
- These are single-threaded CPU measurements with no I/O, so no latency figure in SPEC.md section 7.2 is confirmed or refuted by them.

## Not run yet

`frame_codec` (M2), `wal_throughput` and `segment` (M3), `write_path` and `state_apply` (M4), `fan_out` (M5), and all load generator numbers (M6, M10).
