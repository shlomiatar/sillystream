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

## M2: `frame_codec`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p proto --bench frame_codec
```

Commit: the working tree on top of `bc0ddcb`, committed together with this file. Same machine, load average about 3.7. Time per frame; the record bytes are a fill pattern, which is fine because the codec doesn't read them.

`PUBLISH` has no state. `encode` appends to a reused, already sized `Vec`, so it measures no allocation. `data_prefix` builds only the 10 bytes before the records, which is what a sender passing the original bytes to the socket pays.

| records | `PUBLISH` decode | `PUBLISH` encode | `DATA` decode | `DATA` encode | `data_prefix` |
|---|---|---|---|---|---|
| 64 B | 8.2 ns | 5.4 ns | 3.1 ns | 4.1 ns | 1.4 ns |
| 4 KiB | 8.3 ns | 40.5 ns | 3.1 ns | 37.9 ns | 1.4 ns |
| 64 KiB | 8.3 ns | 545 ns | 3.1 ns | 554 ns | 1.4 ns |
| 1 MiB | 8.4 ns | 12.1 µs | 3.1 ns | 11.9 µs | 1.5 ns |

`SUBSCRIBE`, time per frame:

| clauses and stream id | decode | encode |
|---|---|---|
| 0 clauses, 15 B | 35.8 ns | 11.1 ns |
| 4 clauses, 15 B | 38.7 ns | 13.7 ns |
| 4 clauses, 1 KiB | 84.7 ns | 45.2 ns |

Reading these:

- **Decode doesn't depend on record size:** 64 B to 1 MiB stays within about 3% for `PUBLISH` and 2% for `DATA`, since it only slices. Criterion also prints GiB/s for these rows, and those numbers mean nothing because no record byte is touched.
- **Encode is a `memcpy`:** about 80–110 GiB/s, or 12 µs per MiB. `data_prefix` avoids that cost on the delivery path. Whether the delivery path gets the bytes to the socket without a copy is M5's to show.
- **`SUBSCRIBE`:** clauses add about 3 ns to decode, and a 1 KiB stream id adds about 46 ns over a 15 B one. It is a per-subscription frame, so none of this is on the record path.

## M3: `wal_throughput`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p storage --bench wal_throughput
```

Commit: the working tree on top of `58c6402`, committed together with this file. Same machine, load average about 3. The real writer thread, real `sync_data` and the internal SSD (APFS, in the system temp dir). Each run starts a writer on a new 64 MiB `set_len` file, with a channel of 4,096 commands. Senders are real threads retrying `try_append` with `yield_now` on a full channel, and the clock stops when every entry is durable and the writer has exited. Each cell is the total over all of that bench's runs.

| record bytes | senders | entries/s | MiB/s | fsyncs/s | mean group | largest group |
|---|---|---|---|---|---|---|
| 256 | 1 | 752,710 | 183.8 | 187 | 4,035 | 4,097 |
| 256 | 4 | 725,936 | 177.2 | 186 | 3,913 | 4,097 |
| 256 | 16 | 734,334 | 179.3 | 187 | 3,917 | 4,097 |
| 4,096 | 1 | 456,268 | 1,782 | 115 | 3,979 | 4,097 |
| 4,096 | 4 | 443,592 | 1,733 | 118 | 3,756 | 4,097 |
| 4,096 | 16 | 400,657 | 1,565 | 106 | 3,780 | 4,097 |
| 65,536 | 1 | 62,636 | 3,915 | 18 | 3,414 | 4,097 |
| 65,536 | 4 | 61,591 | 3,850 | 23 | 2,672 | 4,097 |
| 65,536 | 16 | 60,920 | 3,808 | 23 | 2,648 | 4,097 |

`wal_durable_wait`, one entry at a time with one sender: `try_append` to the core's `watch` firing, so one group of one per sync. Mean of the criterion estimate.

| record bytes | time per entry |
|---|---|
| 256 | 4.13 ms |
| 4,096 | 4.30 ms |
| 65,536 | 4.87 ms |

Reading these:

- **Group commit is what scales:** the group fills to the channel's capacity in every case, because senders refill it during each sync. A lone entry costs about 4 ms, and in a group costs about 1.3 µs.
- **At 256 B the limit is the channel, not the disk.** About 187 syncs per second × 4,096 entries matches the 750,000 entries/s measured, so a bigger channel might raise it. Sender count makes little difference, since one thread writes.
- **At 64 KiB the limit is the disk,** about 3.9 GiB/s, with groups of about 170–210 MiB.
- **Section 7.2's level 2 row:** `wal_durable_wait` is 4–5 ms, against its estimates of 1–4 ms on EBS and 0.1–2 ms on NVMe. That is `F_FULLFSYNC` on this SSD, not either of those, so it confirms or refutes neither. Both rows stay unmeasured estimates.

## M3: `segment`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p storage --bench segment
```

Same commit and machine. Each stream has one record with 64 data bytes.

Build, in memory, with no I/O: walking headers, block CRCs, the summary table and the footer. Record bytes aren't copied.

| streams | time | rate |
|---|---|---|
| 1 | 232 ns | 4.3 M streams/s |
| 1,000 | 112 µs | 9.0 M streams/s |
| 1,000,000 | 164 ms | 6.1 M streams/s |
| 1 stream, 32 MiB of 4 KiB records | 1.24 ms | 25 GiB/s (CRC speed) |

Reads per lookup, counted through the wrapper store, and one measured `PUT` through `LocalFileSystem`:

| streams | object | pages | top index reads | page reads | data reads | one `PUT` |
|---|---|---|---|---|---|---|
| 1 | under 1 MiB | 1 | 1 | 1 | 2 | 333 µs |
| 1,000 | under 1 MiB | 1 | 1 | 1 | 2 | 2.8 ms |
| 1,000,000 | 221 MiB | 977 | 1 | 1 | 2 | 2.93 s |

Lookup time on `LocalFileSystem`, on a rotating set of streams, reading the record back with all CRCs checked:

| streams | full lookup | cached top index | stored extent |
|---|---|---|---|
| 1 | 104 µs | 78 µs | 52 µs |
| 1,000 | 115 µs | 81 µs | 52 µs |
| 1,000,000 | 115 µs | 82 µs | 52 µs |

Reading these:

- **Lookup doesn't depend on stream count:** 1K to 1M streams changes nothing measurable. Each read costs about 26 µs. Where that time goes wasn't profiled.
- **These reads hit the page cache,** since the object was just written. They count reads, and don't measure a disk or S3.
- **The 2.93 s `PUT` has about 2M small parts,** two per stream. It wasn't profiled, and S3 wasn't run.

## M4: `write_path` and `state_apply`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p server --bench write_path
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p server --bench state_apply
```

Commit: the working tree on top of `fa77830`, committed together with this file. Load average about 2 during the runs.

`write_path` runs real cores, the real WAL writer and the real client messages, with one client thread per core publishing to one stream on it. Each publish is one record of 256 data bytes (288 on the wire) unless stated, and producer ids dedupe as in production. Numbers are wall time per publish on each core, so the node's rate is `cores / time`. Runs are capped in size to bound memory, and criterion's count is scaled from the capped run. With 16 cores, the 16 client threads and the writer share the 16 hardware threads with the cores, so that column is pessimistic.

**Publish to durable, on the real disk with `sync_data`, level 2:**

| cores | one outstanding: per publish | node rate | 256 outstanding: per publish | node rate |
|---|---|---|---|---|
| 1 | 4.24 ms | 236 /s | 36.1 µs | 27,700 /s |
| 4 | 8.68 ms | 461 /s | 34.0 µs | 117,500 /s |
| 16 | 10.3 ms | 1,559 /s | 41.9 µs | 382,000 /s |

**The same with no disk** (a WAL file that discards writes and syncs at once), which is what cores and channels cost, 256 outstanding:

| cores | 1 record | 100 records | 10,000 records |
|---|---|---|---|
| 1 | 915 ns | 1.63 µs (16.5 GiB/s) | 129 µs (20.9 GiB/s) |
| 4 | 1.06 µs | 4.73 µs (22.7 GiB/s) | 500 µs (21.4 GiB/s) |
| 16 | 3.64 µs | 25.0 µs (17.2 GiB/s) | 2.26 ms (19.0 GiB/s) |

**Dedupe lookup**, by entries in the table: 3.1 ns at 1, 3.4 ns at 10, 5.5 ns at 100, 7.9 ns at 1,000.

**Slab reference changes** for one level 3 publish, from arrival through upload, counted by the hook:

| records in the publish | changes | per record |
|---|---|---|
| 1 | 8 | 8.0 |
| 100 | 8 | 0.08 |
| 10,000 | 8 | 0.0008 |

Each change is two atomic operations (D74).

**`state_apply`**, one op on an encoded document, by its size. The doc is a flat map of short strings with a counter at the front:

| doc bytes | merge | incr | set |
|---|---|---|---|
| 80 | 999 ns | 1.02 µs | 500 ns |
| 1,025 | 5.93 µs | 5.69 µs | 4.39 µs |
| 16,400 | 72.7 µs | 69.0 µs | 58.2 µs |
| 65,491 | 267 µs | 254 µs | 213 µs |

With the hand-written CBOR reader and writer (D81), short runs (2 s measuring) merge 645 ns, 4.41 µs, 58.8 µs, 218 µs; incr 623 ns, 4.17 µs, 55.5 µs, 206 µs; set 326 ns, 3.16 µs, 45.9 µs, 173 µs, by the same doc sizes. That is 18–37% faster than the table, measured on the same machine in one session, though the table's run was longer.

After D78 and D79 changed the core loop, `publish_no_disk` with 1 record on 1 core measured 941 ns in a short run, no significant change from the previous run. Only that cell was repeated.

Rechecked after the fixes on top of `1fd96cd` (D65, D66, D76, D77), load average about 2. `state_apply` is within 3% of this table in every cell, and the no-disk `write_path` runs are within 6% except 1 core with 100 records, 15% faster, which is noise under the caveats below. The real-disk runs weren't repeated. A merge or an `incr` on an existing number doesn't re-parse its result. An `incr` that adds a key does, which isn't in this table: parsing every result was measured at about 78% slower at 64 KB, and is why only that case does it (D76).

Reading these:

- **Level 2 with one outstanding is the fsync:** 4.2 ms is `F_FULLFSYNC` on this laptop, in line with the WAL's own numbers. It says nothing about EBS or Linux NVMe, and section 7.2's level 2 row stays unconfirmed.
- **Group commit is what scales it:** with 256 outstanding, one core does 117 times the single-outstanding rate on the same fsyncs. Going from 1 to 16 cores multiplies the node's rate by 13.8, on one laptop with the clients on the same cores.
- **Batches cost little beyond memcpy.** With no disk, a 10,000-record publish takes about 140 times a one-record one at 10,000 times the records. The rate is around 20 GiB/s, which is mostly the client's copy into a slab plus the validation and stamping walks. Nothing here is a socket: M6 adds those.
- **`no disk` isn't an ingest figure.** Nothing is sealed or uploaded in that run, and a discarded write isn't a WAL.
- **State ops cost a full decode and encode,** so about 4 µs per KiB: one core could do about 3,700 merges per second on a full 64 KB doc. Section 10 already says state isn't for high-frequency updates. `set` also decodes, to validate.

## Caveats

- `LocalFileSystem` doesn't fsync objects, so its `PUT` isn't a durability figure, and neither it nor these numbers say anything about S3.
- `sync_data` on macOS is `F_FULLFSYNC`, which behaves differently from `fdatasync` on Linux NVMe or EBS.
- One laptop, under whatever else it was running. Treat differences under about 2× as noise.
- M1 and M2 are single-threaded CPU measurements with no I/O, and no latency figure in SPEC.md section 7.2 is confirmed or refuted by any number here.

## Not run yet

`fan_out` (M5), and all load generator numbers (M6, M10).
