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

Merge after D237, short runs (2 s measuring), small patch, scan path: 706 ns, 4.82 µs, 63.5 µs, 237 µs by the same doc sizes, in line with the hand-written reader's figures above. A patch of 16,000 integer keys merged into the same 64,003-byte document takes the index path: 3.3 ms in release, measured once by a test's timer, not criterion, against about 600 ms before.

Rechecked after the fixes on top of `1fd96cd` (D65, D66, D76, D77), load average about 2. `state_apply` is within 3% of this table in every cell, and the no-disk `write_path` runs are within 6% except 1 core with 100 records, 15% faster, which is noise under the caveats below. The real-disk runs weren't repeated. A merge or an `incr` on an existing number doesn't re-parse its result. An `incr` that adds a key does, which isn't in this table: parsing every result was measured at about 78% slower at 64 KB, and is why only that case does it (D76).

Reading these:

- **Level 2 with one outstanding is the fsync:** 4.2 ms is `F_FULLFSYNC` on this laptop, in line with the WAL's own numbers. It says nothing about EBS or Linux NVMe, and section 7.2's level 2 row stays unconfirmed.
- **Group commit is what scales it:** with 256 outstanding, one core does 117 times the single-outstanding rate on the same fsyncs. Going from 1 to 16 cores multiplies the node's rate by 13.8, on one laptop with the clients on the same cores.
- **Batches cost little beyond memcpy.** With no disk, a 10,000-record publish takes about 140 times a one-record one at 10,000 times the records. The rate is around 20 GiB/s, which is mostly the client's copy into a slab plus the validation and stamping walks. Nothing here is a socket: M6 adds those.
- **`no disk` isn't an ingest figure.** Nothing is sealed or uploaded in that run, and a discarded write isn't a WAL.
- **State ops cost a full decode and encode,** so about 4 µs per KiB: one core could do about 3,700 merges per second on a full 64 KB doc. Section 10 already says state isn't for high-frequency updates. `set` also decodes, to validate.

## M5: `fan_out`

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench -p server --bench fan_out
```

Commit: `db473b0`. A publish of one 288-byte record at level 1, on a WAL file that discards writes and syncs at once, until every subscriber's connection has been handed it and a reader has taken it. Up to 64 publishes are outstanding, so these are rates, not latencies. There are 16 connections (fewer with fewer subscribers), on 8 tokio threads that are also the publisher. Every filter matches every record, so 100 filters means 100 checks per record. The stream's owner is core 0, and connections are spread over the cores. Each cell is time per publish, ns per subscriber, and bytes delivered per second.

| cores | subscribers | 1 filter | 10 filters | 100 filters |
|---|---|---|---|---|
| 1 | 1 | 1.69 µs, 1,692 ns, 162 MiB/s | not applicable | not applicable |
| 1 | 100 | 19.0 µs, 190 ns, 1.41 GiB/s | 19.2 µs, 192 ns, 1.40 GiB/s | 28.6 µs, 286 ns, 961 MiB/s |
| 1 | 10,000 | 1.99 ms, 199 ns, 1.35 GiB/s | 1.47 ms, 147 ns, 1.82 GiB/s | 1.29 ms, 129 ns, 2.08 GiB/s |
| 8 | 1 | 1.72 µs, 1,717 ns, 160 MiB/s | not applicable | not applicable |
| 8 | 100 | 28.3 µs, 283 ns, 971 MiB/s | 27.9 µs, 279 ns, 986 MiB/s | 43.8 µs, 438 ns, 627 MiB/s |
| 8 | 10,000 | 397 µs, 40 ns, 6.75 GiB/s | 326 µs, 33 ns, 8.24 GiB/s | 427 µs, 43 ns, 6.28 GiB/s |

Reading these:

- **Fan-out per subscriber is about 150 ns on one core,** including the reader's channel and counting, and doesn't grow with subscribers. A byte count in GiB/s here counts each subscriber's share of the same memory, since nothing is copied: it isn't memory bandwidth, and nothing here has been through a socket, which M6 adds.
- **N cores spread it,** as section 6.1 says: 3 to 5 times the rate at 10,000 subscribers on 8 cores. At 100 subscribers cross-core messages cost more than they save, about 50% slower than one core.
- **Filter count hardly shows at 10,000 subscribers** (one record, 100 checks, is small next to 10,000 enqueues), and the differences there are under the 2× noise floor below. At 100 subscribers on 1 core, 100 filters cost about 50% more than 1.
- **A write among catch-ups.** D101's effect, as a one-off in a release build at `db473b0` and not a criterion bench: 1 core, 4,000 publishes of 200 records of 32 B held in memory, K subscribers from 0 with a filter matching nothing, then time one level 1 ack. K=1: 23 µs, K=100: 1.9 ms, K=1,000: 28 ms, K=5,000: 66 ms, against 3.5 ms, 333 ms, 3.34 s and 16.6 s before. The catch-ups themselves took 1.4 ms, 119 ms, 1.16 s and 17.1 s. What remains in the ack is the burst of subscribe commands the core reads first, with one read on each turn.
- **Not measured:** real disks, catch-up rate (M10's load generator), and delivery with non-matching filters. One filter per connection isn't varied, since connections are fixed at 16.

## M6: `sillystream bench`

The real server binary as a child process, a client in another process, TLS 1.3 and WebSocket over loopback, through the SDK. Run at commit `0d8ceb5`, on the machine above with a load average around 3.

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo build --release -p server -p cli
target/release/sillystream bench all --count 5000 --seconds 10 --idle-count 10000 --cores 4
```

### Latency, 256-byte record, one publisher, 4 cores

| level | measures | samples | p50 µs | p99 µs | p99.9 µs |
|---|---|---|---|---|---|
| 0 | publish to delivery | 5000 | 37 | 137 | 217 |
| 1 | ack, closed loop | 5000 | 34 | 45 | 54 |
| 2 | ack, closed loop, local disk | 5000 | 4578 | 7807 | 75056 |
| 3 | ack at 50 per second, `LocalFileSystem`, 1 s window | 1000 | 507916 | 996214 | 1008570 |

### Throughput, one stream, 4 cores, 10 s each

| ack | connections | record data bytes | publishes/s | MB/s of data |
|---|---|---|---|---|
| 1 | 1 | 1024 | 320381 | 328.1 |
| 1 | 1 | 65536 | 13326 | 873.3 |
| 1 | 4 | 1024 | 509079 | 521.3 |
| 1 | 4 | 65536 | 25561 | 1675.2 |
| 2 | 1 | 1024 | 195262 | 199.9 |
| 2 | 1 | 65536 | 7482 | 490.3 |
| 2 | 4 | 1024 | 476923 | 488.4 |
| 2 | 4 | 65536 | 13022 | 853.4 |

### Memory per idle connection, 4 cores

| connections | server RSS before | server RSS after | bytes per connection |
|---|---|---|---|
| 10000 | 10223616 | 225263616 | 21504 |

- **Level 0** has no ack to time, so its row is publish to delivery on a subscriber of the stream, closed loop.
- **Levels 1 and 2** are closed loop: each ack before the next publish, so they include the SDK and TLS on both sides and one loopback round trip. Level 2 is on this laptop's SSD with `F_FULLFSYNC`. Section 7.2's NVMe and EBS figures are neither confirmed nor refuted by it.
- **Level 3** goes out at 50 per second so that the spread of a 1 s seal window shows, and its target is `LocalFileSystem`, which doesn't fsync objects. It measures the wait for the window, not S3.
- **Throughput** rows are one stream, so one owning core, whatever `--cores` says. The window is 8 MiB unanswered per connection, or 16 records if larger. Each row has its own server. Rows include the client's own cost on the same machine, so the server may not be the limit.
- **Memory per idle connection** is the server's RSS after minus before over the connection count, from `ps`. It counts what the process itself holds for a connection, such as the rustls state, the task and its channels, and not the kernel's socket buffers. The read buffer isn't in it, which `crates/server/tests/idle_connections.rs` asserts on the pool's counters: 10,000 connections, `held_buffer_count = 0`.
- **Not measured here:** real S3 and EBS (unmeasured estimates remain as section 7.2's table has them). The other load generator numbers are under M10 below.

## Caveats

- `LocalFileSystem` doesn't fsync objects, so its `PUT` isn't a durability figure, and neither it nor these numbers say anything about S3.
- `sync_data` on macOS is `F_FULLFSYNC`, which behaves differently from `fdatasync` on Linux NVMe or EBS.
- One laptop, under whatever else it was running. Treat differences under about 2× as noise.
- M1 and M2 are single-threaded CPU measurements with no I/O, and no latency figure in SPEC.md section 7.2 is confirmed or refuted by those two.

## Later re-runs

### Throughput after D121

The same throughput command, re-run after frames that span reads gained a copy (D121), on the same machine with a load average around 4. One run each, so differences of this size are within what a re-run moves, and this shows no large cost rather than no cost.

| ack | connections | record data bytes | publishes/s | MB/s of data |
|---|---|---|---|---|
| 1 | 1 | 1024 | 340293 | 348.5 |
| 1 | 1 | 65536 | 14069 | 922.0 |
| 1 | 4 | 1024 | 453805 | 464.7 |
| 1 | 4 | 65536 | 25586 | 1676.8 |
| 2 | 1 | 1024 | 187784 | 192.3 |
| 2 | 1 | 65536 | 7259 | 475.7 |
| 2 | 4 | 1024 | 486434 | 498.1 |
| 2 | 4 | 65536 | 13759 | 901.7 |

### Throughput after D129 to D133

`sillystream bench throughput --seconds 3 --cores 4`, release build, re-run after the command gate (D131) and the changes to `wsconn`. One run each at 3 s, where the table above used a longer run, so this shows no large cost rather than no cost. The publisher has no subscriber here, so it doesn't exercise D132's release of `DATA` chunks, and it publishes over WebSocket, so D130's extra copy on HTTP publishes wasn't measured.

| ack | connections | record data bytes | publishes/s | MB/s of data |
|---|---|---|---|---|
| 1 | 1 | 1024 | 338870 | 347.0 |
| 1 | 1 | 65536 | 24632 | 1614.3 |
| 1 | 4 | 1024 | 448522 | 459.3 |
| 1 | 4 | 65536 | 34103 | 2235.0 |
| 2 | 1 | 1024 | 190548 | 195.1 |
| 2 | 1 | 65536 | 6586 | 431.6 |
| 2 | 4 | 1024 | 468264 | 479.5 |
| 2 | 4 | 65536 | 13788 | 903.6 |

## M9: a stalled sink against publishing

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo test -p server --test sink_stall -- --nocapture
```

Working tree on top of `270514e`, committed together with this file. Same machine as above. This is a test, in a debug build, with a scripted WAL file, so it compares two runs and says nothing about absolute speed. No criterion bench was added for M9.

30,000 records of 1,024 bytes over 5 rounds, measured after a 40,000 record warm-up. The receivers of all 16 sinks hold their responses.

| | No sinks | 16 stalled sinks |
|---|---|---|
| Steady, records/s | 128,107 | 104,436 (0.815) |
| Slowest 1,000 records, ms | 11 | 13 |
| While sinks fill their budgets, records/s | 129,812 | 67,489 (0.52) |

Caveats: one run on a shared laptop. The test asserts a steady ratio above 0.7, a bound picked for noise. The steady cost isn't explained (see BUILD_NOTES), and filling costs about half while it lasts. A real fsync would likely hide some of this, which wasn't measured.

## M10: the load generator, complete

All of it at commit `0681e76`, release build, the real server binary as a child process and a client in another process, TLS 1.3 and WebSocket over loopback, through the SDK. Machine as above (Apple M4 Max, 16 cores, macOS 27.0.1, cargo 1.98.1), a shared laptop, load average between 2 and 6. One run of each command, except that each catch-up tier is read three times.

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo build --release -p server -p cli
target/release/sillystream bench latency --count 5000 --cores 4
target/release/sillystream bench throughput --seconds 10 --cores 4
target/release/sillystream bench idle --idle-count 10000 --cores 4
target/release/sillystream bench catch-up --cores 4 --catch-up-mib 8
target/release/sillystream bench catch-up --cores 4 --catch-up-mib 256
target/release/sillystream bench many-streams --cores 16 --seconds 10
target/release/sillystream bench puts --cores 4
target/release/sillystream bench idle-streams --cores 4 --idle-streams 20000
```

#### Latency, 256-byte record, one publisher, 4 cores

| level | measures | samples | p50 µs | p99 µs | p99.9 µs |
|---|---|---|---|---|---|
| 0 | publish to delivery | 5000 | 33 | 144 | 297 |
| 1 | ack, closed loop | 5000 | 34 | 47 | 81 |
| 2 | ack, closed loop, local disk | 5000 | 4694 | 12845 | 66714 |
| 3 | ack at 50 per second, `LocalFileSystem`, 1 s window | 1000 | 508994 | 995747 | 1009146 |

#### Throughput, one stream, 4 cores, 10 s each

| ack | connections | record data bytes | publishes/s | MB/s of data |
|---|---|---|---|---|
| 1 | 1 | 1024 | 334770 | 342.8 |
| 1 | 1 | 65536 | 12122 | 794.4 |
| 1 | 4 | 1024 | 449716 | 460.5 |
| 1 | 4 | 65536 | 23753 | 1556.7 |
| 2 | 1 | 1024 | 188602 | 193.1 |
| 2 | 1 | 65536 | 5683 | 372.4 |
| 2 | 4 | 1024 | 473112 | 484.5 |
| 2 | 4 | 65536 | 12137 | 795.4 |

#### Memory per idle connection, 4 cores

| connections | server RSS before | server RSS after | bytes per connection |
|---|---|---|---|
| 10000 | 12664832 | 227721216 | 21506 |

#### Catch-up rate, 2048 records of 4096 bytes, 4 cores

| tier | run | MB/s of data | bytes from memory | bytes from local | bytes from object store |
|---|---|---|---|---|---|
| memory | 1 | 319 | 8454144 | 0 | 0 |
| memory | 2 | 319 | 8454144 | 0 | 0 |
| memory | 3 | 353 | 8454144 | 0 | 0 |
| local disk | 1 | 1813 | 0 | 8454144 | 0 |
| local disk | 2 | 2459 | 0 | 8454144 | 0 |
| local disk | 3 | 2538 | 0 | 8454144 | 0 |
| `LocalFileSystem` object store | 1 | 1743 | 0 | 0 | 8454144 |
| `LocalFileSystem` object store | 2 | 2141 | 0 | 0 | 8454144 |
| `LocalFileSystem` object store | 3 | 2068 | 0 | 0 | 8454144 |

#### Catch-up rate, 65536 records of 4096 bytes, 4 cores

| tier | run | MB/s of data | bytes from memory | bytes from local | bytes from object store |
|---|---|---|---|---|---|
| memory | 1 | 488 | 100000800 | 170531808 | 0 |
| memory | 2 | 2386 | 0 | 270532608 | 0 |
| memory | 3 | 2545 | 0 | 270532608 | 0 |
| local disk | 1 | 2522 | 0 | 270532608 | 0 |
| local disk | 2 | 2507 | 0 | 270532608 | 0 |
| local disk | 3 | 2517 | 0 | 270532608 | 0 |
| `LocalFileSystem` object store | 1 | 2075 | 0 | 0 | 270532608 |
| `LocalFileSystem` object store | 2 | 2071 | 0 | 0 | 270532608 |
| `LocalFileSystem` object store | 3 | 2062 | 0 | 0 | 270532608 |

#### Throughput, many streams, 16 cores, 10 s each

| ack | streams | record data bytes | publishes/s | MB/s of data |
|---|---|---|---|---|
| 1 | 1 | 1024 | 261960 | 268.2 |
| 1 | 16 | 1024 | 408276 | 418.1 |
| 1 | 64 | 1024 | 460702 | 471.8 |
| 2 | 1 | 1024 | 164303 | 168.2 |
| 2 | 16 | 1024 | 364602 | 373.4 |
| 2 | 64 | 1024 | 399761 | 409.4 |

#### Objects per seal, 4 cores, 1 s window, every stream written in every window

| streams | windows | new objects in `sealed/` per window | other new objects per window |
|---|---|---|---|
| 10 | 8.0 | 1.00 | 1.00 |
| 1000 | 8.0 | 1.00 | 1.00 |
| 10000 | 8.0 | 0.87 | 0.87 |

#### Memory per idle stream, 4 cores

| streams | active limit | server RSS before | server RSS after | bytes per stream |
|---|---|---|---|---|
| 20000 | none | 13172736 | 201703424 | 9427 |
| 20000 | 1000 | 13139968 | 201932800 | 9440 |

#### Against section 7.2

| level | spec's estimate | measured here (p50 / p99 / p99.9) | target |
|---|---|---|---|
| 1 | under 0.1 ms, slow case under 1 ms | 34 µs / 47 µs / 81 µs | memory |
| 2, local NVMe | 0.1–2 ms, slow case about 5 ms | not comparable: 4.7 ms / 12.8 ms / 66.7 ms with `F_FULLFSYNC` on this laptop's SSD | macOS, not Linux NVMe |
| 2, EBS | 1–4 ms, slow case 10+ ms | **unmeasured** | not exercised |
| 3, real S3 | half a window + 20–100 ms, slow case one window + 200 ms | **unmeasured** | not exercised |
| 3, `LocalFileSystem` | none | 509 ms / 996 ms / 1009 ms, 1 s window | not a durability figure |

#### Catch-up from real S3 and PUTs on real S3

**Unmeasured.** The object store row is `LocalFileSystem` on the same SSD, whose reads come from the page cache, so it says nothing about S3 latency or cost.

#### What to make of these

- **Memory is the slowest tier in these runs, and that isn't explained.** 319 to 353 MB/s from memory against 1,743 to 2,538 MB/s from the other two, on the same records. The 256 MiB run's first read, mostly local with 100 MB from memory, is also the slow one at 488 MB/s. Nothing here measured why. Recorded in BUILD_NOTES as a finding, not fixed.
- **Tier columns come from the server's own trace** (`--trace-catch-up`, D213). The trace was on in all of these runs, at one line on stderr per read. The bench tempdir is on the same SSD as everything else.
- **The 256 MiB run's "memory" rows aren't all memory:** a stream that fills its buffer gets a dedicated object before the window ends (section 7.3), so its records were already on disk even with a 120 s window. The 8 MiB run is the one that reads only memory. Records read are through the SDK, which copies them out, so these are client-side rates.
- **Many streams:** the client shares the machine and uses one Tokio runtime, and 64 streams on 16 cores gives 1.2 to 2.4 times one stream, so this shows the client or the machine as much as section 6.1's limit. Maximum ingest across 16 cores wasn't reached, and this can't say what it is. The one-stream rows here run with 16 cores and the throughput table's with 4, and that difference is one run each, inside this file's 2x noise.
- **Objects per seal:** one new file in `sealed/` per window at 10, 1,000 and 10,000 streams, and one other (a checkpoint). A new file on `LocalFileSystem` stands for one PUT. Overwrites aren't counted. The 10,000 row reads 0.87 because publishing 10,000 streams takes up part of a window, so windows were missed in that count, which has no seal counter behind it, and the count isn't exact.
- **Memory per idle stream** counts RSS with one 64-byte record per stream, after the records sealed, with 20,000 producers' dedupe entries among it. The active limit made no difference to RSS in this run. Whether the streams left RAM wasn't asserted here (M7's tests do that), and an allocator doesn't have to give memory back, so this isn't evidence for or against eviction.
- All of section 7.2's caveats and this file's Caveats apply. Criterion suites weren't re-run at this commit: each table above names the commit it came from, and the code they measure may have changed since.
