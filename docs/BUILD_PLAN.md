# Build plan

Ordered plan for implementing `docs/SPEC.md` on a single node. Each milestone builds on the last. Multi-node takeover (section 8.2) is deferred and has no work here, but shards are built. Spec gaps and open questions are in `docs/BUILD_NOTES.md`, referred to as D*n* and Q*n*.

The docs `DECISIONS.md` and `ROADMAP.md` exist only under `docs/old/` and are not binding.

## Ground rules

**Definition of done for every milestone.** All of these pass, then a local commit on `spec-rewrite`:

```
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo test --workspace
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo clippy --workspace --all-targets -- -D warnings
CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target cargo bench --workspace --no-run
```

A failure that can't be fixed is reported as a failure. Tests are never weakened, skipped or deleted.

**Environment.** macOS has no `timeout` command, so long commands use `perl -e 'alarm N; exec @ARGV' cargo ...`. Tests that spawn processes kill them on drop, and leave none behind.

**Not exercised, and stated in the final report:** real S3 and R2, EBS, power loss, machine loss, and Linux. `kill -9` proves process-crash recovery only, since the page cache survives it, so fsync ordering gets its own test in M3.

## Crates

| crate | contents | notes |
|---|---|---|
| `format` | record, ids, filters, summaries, CRC | no I/O |
| `proto` | WebSocket frames | |
| `auth` | token encode, sign, verify, pattern match | |
| `storage` | WAL writer, segments, object store, uploader, metadata store | no codec |
| `server` | cores, write path, delivery, state, APIs, recovery, compaction, sinks; binary `sillystream-server` | no codec |
| `sdk` | Rust client, batching, zstd | only crate with zstd |
| `cli` | binary `silly`: load generator, tokens, config | |
| `e2e` | black-box suite | starts the real binaries |

Vectors live in `tests/vectors/` of `format` and `proto`.

## Benchmarks

Benchmarks are a deliverable, not an afterthought. SPEC.md says its latency figures are estimates to be benchmarked, and this is where they get measured.

**Criterion suites.** Each one is added in the milestone that builds the code, and each milestone adds its results to `docs/BENCH.md` in the same commit:

| milestone | bench | measures | varied by |
|---|---|---|---|
| M1 | `filter_scan` | headers per second | 1–4 clauses, match rate 0.1–100%, with and without block skipping |
| M1 | `stamping` | ns per publish | 1, 100, 10,000 records; must stay linear in records and independent of data size |
| M2 | `frame_codec` | decode and encode rate of `PUBLISH`, `DATA`, `SUBSCRIBE` | body size |
| M3 | `wal_throughput` | entries and bytes per second, fsyncs per second, group size, on the real disk | 256 B, 4 KiB, 64 KiB; 1, 4, 16 senders |
| M3 | `segment` | build rate; footer lookup time and read count | 1, 1K, 1M streams |
| M4 | `write_path` | publish to durable on one core; dedupe lookup; slab atomics per record | 1 and N cores |
| M4 | `state_apply` | merge and incr | doc size up to 64 KB |
| M5 | `fan_out` | bytes per second and ns per subscriber | 1, 100, 10,000 subscribers; 1, 10, 100 distinct filters; 1 and N cores |

**Load generator numbers,** through the real server over TLS, from `silly bench` (M6) and finished in M10:

- Ack latency p50, p99 and p99.9 at each level 0–3, against the table in section 7.2, marked as local disk and `LocalFileSystem`. The S3 and EBS rows stay marked as unmeasured estimates.
- Maximum ingest throughput for one stream on one core (the hard limit of section 6.1), and across many streams on 16 cores.
- Catch-up rate from memory, disk and object store.
- Memory per idle connection and per idle stream.
- Object PUTs per seal, confirming one per node regardless of stream count.

**`docs/BENCH.md` format:** machine, OS, commit, exact command, table of results, and caveats. Known caveats to state: Rust's `sync_data` on macOS uses `F_FULLFSYNC`, which behaves differently from `fdatasync` on Linux NVMe or EBS; thread pinning isn't available; and criterion numbers are from one laptop under whatever else it is running. Numbers are never adjusted or estimated: anything not run is listed as not run.

## Milestones

### M1. Workspace and record format

**Scope.** Cargo workspace with all crates. `.DS_Store` ignored. Record header (section 2), id assignment with ms borrowing (D15), publish validation, stamping in place, filter clauses, block summaries and the skip rules (section 4), and CRC helpers.

**Acceptance.**
- Byte-exact vectors in `format/tests/vectors/` for records with `count` 1 and greater, a padded record and a system record, and all fields decode little-endian at the offsets in section 2.
- Property tests: encode/decode round trip; random bytes never panic the decoder.
- Validation rejects each case in section 3 (lengths don't add up, `count = 0`, non-zero `id`, `author`, or padding), and a test shows it reads no data bytes by passing identical results over random data.
- Property test: for random options and clauses, `none match` and `all match` never disagree with evaluating each record. This includes D3's system-record rule.
- Ids strictly increase across 1M allocations with a frozen clock, a backward clock and full-counter records (`count = 65,535`), and the high 48 bits borrow forward.
- `filter_scan` and `stamping` run, with results in `docs/BENCH.md`.

### M2. Frames and tokens

**Scope.** Every frame in section 11.1 (including `REDIRECT` encoding only, since it is deferred), error codes, and flag-dependent fields. Token format and verification (D25), and patterns.

**Acceptance.**
- A byte-exact vector for each client and server frame, including every optional-field and flag combination of `SUBSCRIBE` and `PUBLISH`.
- Property round-trip test for every frame; decoders return an error and never panic on truncated, oversized or random input.
- Tokens: a test each for a valid token, wrong signature, expired, `author = 0`, tampered byte, and exact and `logs.*` matching (with `logsx` not matching `logs.*`). Verification touches no I/O.
- `frame_codec` runs, with results in `docs/BENCH.md`.

### M3. Storage

**Scope.** WAL writer thread with group commit, crossbeam channels and wake-ups (section 6.3, D8, D9). Segment builder and reader with two-level footer, summaries and CRCs (section 7.3, D10). Object store access through `object_store` with real S3 only behind config. Uploader thread producing one object per seal (section 6.4, D12). Fjall metadata store (D19). Local segment cache with eviction (D11).

**Acceptance.**
- Fsync ordering: with an injected file recording calls, no durable notification comes before the `sync_data` covering it, and a sync error stops notifications.
- Group commit: with slow sync, entries queued during a sync all land in the next one (1 sync, not N), with no timer in the code path.
- `try_send` on a full channel returns at once: a test with a stalled writer shows the sender never blocks.
- Recovery reader: for a WAL truncated at every byte of its last entries and with a flipped bit in any position, replay returns exactly the valid prefix. A reused file's stale entries are not returned (D8).
- Segment: property round trip; reading a stream across 1M synthetic streams makes one top index read, one page read and one read of its region, counted through a wrapper store. A cached top index or stored extent drops the footer reads. A stream added twice is one region in id order. Index hash collisions are resolved by the full key hash. A flipped bit fails the CRC.
- Record bytes in a segment equal the wire bytes exactly.
- Uploader: N sections over many streams produce exactly one PUT per seal, and a dedicated object for a stream filling its buffer, at the section 7.3 path. Store failures retry without losing references.
- Everything runs on `LocalFileSystem` and `InMemory`. A test proves the S3 config builds a client without connecting.
- `cargo tree` for `storage` and `server` contains no compression codec (zstd, lz4, flate2, miniz, snappy, brotli), enforced by a test.
- `wal_throughput` and `segment` run, with results in `docs/BENCH.md`.

### M4. Cores, write path and state

**Scope.** Cores (one tokio current-thread runtime per thread), stream → shard → core placement with override table, per-shard counters, periodic rebalancing through message handoff (section 6.1). Slabs with per-slab reference counts (section 6.5). Write path (section 6.2), dedupe (section 3), ack levels 0–3 (section 7.2), backpressure (D7), seal and handoff to the uploader, and who enforces the segment size cap of section 7.3 (Q18). State: set, merge and incr, `expect_ver`, atomic publish, system records, and snapshot with head (section 10).

Tested through in-process message APIs, without sockets. Shard moves and crash-related cases get deterministic tests with an injected clock and scripted WAL and store.

**Acceptance.**
- Acks: with a held fsync, level 2 never acks; with a held upload, level 3 never acks; level 1 acks without either; level 0 sends nothing; below-minimum publishes are raised.
- Dedupe: a retry of the latest publish returns the original `ACK` and writes nothing; an older one gets `first_id = 0`; a rejected publish doesn't use up its seq; an entry expires at 24 hours idle; the 1,001st producer evicts the oldest; entries stay 28 bytes.
- Consecutive ids across a publish, in SDK sort order. A stable-order property test covers many publishes per stream.
- Backpressure: with a full WAL channel, a core keeps serving other work, such as a ping, delivery and non-publish messages (asserted by a test with a stalled writer), no publish is dropped, and order is preserved on release.
- Slabs: a counting hook shows reference count changes per slab per batch, not per record, and a slab is freed exactly once all references are gone. Any `unsafe` has a comment and a test.
- A stream moved between cores mid-load keeps its ids in order with none lost, pending acks arrive, and the seal holds one region for it, in id order.
- Rebalancing: under a skewed scripted load, the check moves shards off the overloaded core. A hot stream can get its own shard through the override table.
- State: each op, including nested merge deletes by null, incr creation and failure on non-numbers, 64 KB limit (over limit rejected and state unchanged), `expect_ver` mismatch gives version conflict, and `version` equals the system record's id. Property test: replaying the system records gives the same state as applying them live.
- Atomic publish: a failed `expect_ver` commits no records and no ids are visible; on success ids are consecutive with the state record last (D5). The WAL holds it as one entry.
- `write_path` and `state_apply` run, with results in `docs/BENCH.md`.

### M5. Delivery and readers

**Scope.** Filter table (section 5.2), ordered delivery with the visibility rule, linger, `PROGRESS`, bounded queues and `BEHIND`, fair sharing across subscriptions on a connection, cross-core fan-out, catch-up through memory → local → object store and the switch to live, cursors including named ones and `COMMIT`.

Visibility: delivery advances in id order. A level 0–1 record is visible at once unless an earlier record in the stream is still waiting for the WAL, and a level 2–3 record is visible once it is in the WAL.

**Acceptance.**
- Filter table: 10,000 subscribers over 3 distinct filters check each record exactly 3 times (counted), and every subscriber gets exactly the records a naive check picks, in a property test.
- Each subscriber's received bytes are byte-identical to the stored record, and pointer checks confirm memory delivery uses shared slab bytes.
- With a held fsync, level 2 records aren't delivered. Level 3 is delivered before the upload finishes. A level 1 record behind a pending level 2 record waits, keeping id order.
- Catch-up: starting from each of `from = 0`, an id mid-record (whole record sent), `u64::MAX`, and each tier, ids arrive strictly in order with no gap or duplicate across each tier switch and the switch to live, while publishing continues. A property test varies the positions.
- Filtered catch-up uses block skipping: a rare filter reads fewer blocks than a full scan, counted.
- `PROGRESS` arrives after skipped records, at most once per interval, and on reaching head. Resuming from it delivers no missed match.
- A slow subscriber hits its byte bound and gets `BEHIND`, resumes with no loss, and other subscribers and the publisher are unaffected.
- Fairness: on one connection, a heavy subscription doesn't starve a light one (bounded share in a test).
- Named cursors move forward only; a lower `COMMIT` is ignored; reset can rewind; cursors with `sink:` are refused.
- Reading before the start of retention by explicit id gives error 7.
- `fan_out` runs, with results in `docs/BENCH.md`.

### M6. WebSocket and HTTP APIs, SDK and CLI

**Scope.** Server binary with TLS 1.3 (D22), WebSocket reader into slabs with in-place unmasking (D23), idle connections holding no read buffer (section 6.5), all frames wired up, auth on every operation, all routes in section 11.2 plus D21 and D26, stream auto-create, and stream config (section 13). Rust SDK: stable sort by options, batching and per-record zstd, cursors, retries with `(producer_id, producer_seq)`, the wait on `expect_ver`, skipping by id, reconnection. CLI: `sillystream` with token issue, stream and sink admin, publish, tail, and `bench`.

**Acceptance.**
- The server interoperates with `tokio-tungstenite` as well as the SDK, including fragmented and maximum-size frames. A frame over `max_frame` gives error 6. Client certificates aren't needed, and TLS 1.2 is refused. Test certificates come from `rcgen`.
- Auth: for each permission and operation, the right permission passes and every other gets error 1. Expired tokens and wrong-pattern streams fail, and a token expiring mid-connection fails on the next operation. Clients can't set `author`.
- Every HTTP route has a test for success and each error it can return. The simple publish form from section 11.2 works with `curl`-equivalent headers. `If-Match` conflict gives a version conflict. Hex `X-Options` parses.
- SDK: published records round trip through zstd, with the server's dependency tree still free of codecs. Ordering across publishes holds, and a `expect_ver` publish blocks later ones from that producer until acked.
- Idle: 10,000 idle connections hold no read buffer (asserted on pool counters), and memory per connection is recorded in `docs/BENCH.md`.
- `silly bench` produces the level 0–3 latency tables and single-stream throughput in `docs/BENCH.md`.

### M7. Recovery, ownership and stream metadata

**Scope.** Startup replay (section 8.1) with the generation record (D14, D15), empty-WAL warning, reader rule on last valid id, state and dedupe recovery, seal-time metadata written to the local store and S3 checkpoints (D18), idle eviction LRU, load on access, rebuild from checkpoint, delete tombstones (D20).

**Acceptance.**
- Crash matrix, using `test-hooks` crash points (D32) on the real binary: before WAL write, after WAL write before fsync, after fsync before ack, during seal, after segment upload before checkpoint, after checkpoint before WAL recycle, during recycle, during ownership write. After each: no WAL-durable record is lost, nothing is duplicated, and ids stay above all earlier ones.
- Restart restores state to the same version and bytes, restores dedupe so a retry of the latest publish returns its original ack, and restores cursors.
- Empty WAL: warning logged only when a generation already exists, and never on a new node. Each case is tested.
- Generation: two processes starting at once produce one winner. An object uploaded late by a former generation is accepted up to the last valid id and ignored above it, including at record boundaries, with a test on each side of it.
- Idle metadata: with a low limit, cold streams leave RAM oldest first, and streams with a live subscriber never do. Access reloads with identical metadata. With the local store deleted, the stream is rebuilt from its checkpoint, including after a long gap needing older files.
- Checkpoints: PUT count per seal doesn't change between 10 and 100,000 changed streams.
- Deleted streams stay deleted across restart and rebuild, and a re-created one has higher ids and can't read old records.

### M8. Retention and compaction

**Scope.** Seal manifests in the system stream (Q3, Q4), compactor with one worker per class and named cursors (section 7.5), compacted prefixes, extent updates through the owner core, delete job with its compaction check (section 7.4).

Time-based tests use an injected clock, or `test-hooks` on the binary.

**Acceptance.**
- Each seal publishes one manifest, and the manifest stream yields none for itself.
- Compaction output has identical record bytes in the same order, checked by comparing every record to the source, and the source is not deleted. A read during and after sees no gap or duplicate. Readers follow the updated extents, and one holding a stale extent recovers.
- The `1d` class is never compacted. Each longer class lands in its own prefix.
- Compaction restart: after a crash mid-merge, the cursor resumes with no loss, and duplicates of a partial output are never readable. A former generation's records above last valid id are dropped (D17).
- Deletion: a wrapper store records every delete, and proves only whole expired hour prefixes are ever deleted. A stalled compactor blocks `sealed/` expiry until it catches up (the key test from section 7.4).
- After records expire, state, cursors, ids and config remain, and `from = 0` starts at the new earliest record.

### M9. Sinks

**Scope.** Sink runtime in a pool separate from the write path, with per-sink budgets (section 9). Destinations: `webhook.notify`, `webhook.deliver`, `forward`, `archive`. Failure handling, `on_expire`, full lifecycle, sinks moving with their shard, private address blocking (D13, D27–D30).

**Acceptance.**
- At-least-once: a sink killed between delivery and cursor update redelivers, with the same `{sink_id, first_id, last_id}`.
- Ordered: one delivery in flight, shown by a receiver that holds its response; what arrives meanwhile goes in the next batch (D290).
- Retryable errors back off exponentially (scripted clock); permanent errors or too many failures pause the sink.
- `webhook.deliver`: the HMAC verifies, and bytes equal the stored records. Private addresses (IPv4, IPv6 and DNS names resolving to private ranges) are blocked by default, and are allowed only with the node setting.
- `webhook.notify` sends `{stream, head, unread}` and honours its rate limit under a burst.
- Forward: killing it at each step gives no duplicate in the target (property test), with `producer_id = sink_id`, `producer_seq = source first_id`, and each record's original author. System records aren't forwarded. A fan-in of several streams into one works.
- Archive: objects hold byte-identical records on `LocalFileSystem`.
- Lifecycle: create at `now`, `earliest` and an id; update finishes in-flight work and keeps the cursor; pause, resume, reset, delete; a restart restores all of them. A sink moves with its shard.
- `on_expire`: warning before expiry, then `stop` halts it, and `skip` continues and records the gap.
- A stalled sink doesn't slow publishing, measured against a baseline.

### M10. End-to-end suite

**Scope.** Crate `e2e` starts the real `sillystream-server` and CLI binaries as child processes with TLS certificates from `rcgen`, a temp dir, and `LocalFileSystem` as the object store. All clients use real sockets: TLS WebSocket and HTTP. Tests don't call server internals. Only retention and crash-point tests use a binary with `test-hooks`, and everything else uses the normal build. Real S3 and EBS aren't exercised, and the report says so.

**Acceptance.** Each is a named test that passes on `cargo test -p e2e`:
1. `publish_to_live`: published records reach live subscribers, byte-identical and in id order, over WebSocket, with HTTP publish too.
2. `filters`: `eq`, `any`, OR-ed clauses, author and id start deliver exactly the expected sets, live and in catch-up, and `INCLUDE_SYSTEM` gives system records.
3. `catch_up_memory`, `catch_up_disk`, `catch_up_object_store`: each reads from `from = 0` with the tier asserted through server tracing output, with the local cache emptied for the object-store case, and no gaps. The HTTP read route gives the same bytes.
4. `state_cas`: `expect_ver` success and conflict over WebSocket and HTTP, and snapshot plus read misses nothing under concurrent writes.
5. `atomic_publish`: records and state change commit together, and a conflict commits nothing, seen by both a subscriber and a read.
6. `token_rejection`: bad signature, expired token, wrong stream and missing permission are each rejected, on both WebSocket and HTTP, with no side effects.
7. `kill9_mid_load`: several producers publish at level 2 and `kill -9` hits the server at random points, for at least 5 rounds. After each restart and producer retries: every acked record is present exactly once, no id appears twice, ids strictly increase per stream, and state matches its operations.
8. `sink_webhook_outage`: the receiver goes down mid-stream, then returns, and every matching record arrives in full id order at the destination with verified signatures. The test also kills the server during the outage.
9. `sink_forward`: with a target and server restarts during forwarding, the target has each source record exactly once, with original authors.
10. `retention_and_compaction`: with a clock offset, `7d` data is compacted and readable after `sealed/` expires, `1d` data is gone, whole prefixes alone were deleted, and state, cursors and ids remain. A blocked compactor keeps data.
11. Clean-up: after the suite, no server processes remain, checked by the suite itself.

**Also finished here:**
- The whole suite passes 3 consecutive runs, and any flake is reported rather than retried away.
- `docs/BENCH.md` is complete as specified above.
- A final report lists what works, every Q*n* still open, and what wasn't exercised (real S3 and R2, EBS, power loss, machine loss, Linux).
