# Roadmap

Work in order. Each milestone ends with its acceptance checks passing in CI.

## M0: workspace and formats

- Cargo workspace with the crates from CLAUDE.md (empty where not yet needed).
- `format`: batch header, columns, payload offsets, SPARSE slicing, crc32c (with `first_id` and crc zeroed), id generation (48-bit ms + 16-bit seq, ms borrowing), filters, summaries (OR/AND/min/max), the none/all/maybe decision.
- `proto`: all frames from SPEC §6, encode/decode.
- `hash`: xxh3_64 + jump consistent hash.

**Accept when:**
- [ ] Byte-exact test vectors in `tests/vectors/` for batches, frames and group hashing
- [ ] Round-trip property tests and fuzz targets for every decoder
- [ ] Criterion benchmarks: filter scan over 1,000 options, stamp a batch, SPARSE slice

## M1: single-node, in-memory server

- WebSocket server (rustls, TLS 1.3, ECDSA), HELLO/WELCOME, token auth stub.
- Lanes (one per core), shard → lane routing, cross-lane SPSC forwarding.
- OPEN/PUBLISH/ACK with dedupe on `(producer_id, seq)`; ack levels 0–1.
- SUBSCRIBE with filters, positions, multiple subscriptions per connection, fair scheduling, linger, BEHIND.
- Hot ring per stream; full and SPARSE delivery.
- State: set/merge/cas/incr on CBOR, `_sys` separation, atomic message + state, STATE frames.
- Minimal Rust SDK: batch builder, per-message zstd (dictionary optional), resume token.
- `cli` load generator.

**Accept when:**
- [ ] End-to-end tests for publish → live delivery, filters, SPARSE, linger, BEHIND, state CAS conflicts, atomic message + state
- [ ] Load test numbers recorded in `docs/BENCH.md` (publishes/s, delivered frames/s, connections)

## M2: durability and storage

- Per-lane WAL with group commit; ack level 2 after fsync.
- Seal by size/age, build segments sorted by stream from the WAL index, footer index + summaries; one upload per node per seal via `object_store` (MinIO in tests).
- Hybrid dedicated objects for busy streams.
- Extent lists; catch-up reads from memory, local segments, S3 range reads.
- Ack level 3 (after upload) and the logs profile (WAL on local NVMe).
- Retention: `1d` class, hour prefixes, delete job.

**Accept when:**
- [ ] Crash-recovery test: kill the process at random points, restart, no acked message lost, no duplicate ids
- [ ] Catch-up from each tier returns the same messages in order
- [ ] PUT count per node is independent of stream count

## M3: tiers, checkpoints, cursors

- Metadata KV (fjall or RocksDB): parked streams, extents, dedupe, cursors.
- Warm and parked tiers, eviction under memory and disk pressure, watches on parked streams.
- Checkpoint store per shard in S3 with latest-wins compaction; hibernation.
- Named cursors with COMMIT; watermarks.
- Config: templates (`_sys/templates`), `_sys` references and overrides, PATCH API, `sillystream apply`.

**Accept when:**
- [ ] 10M parked streams on one test node; wake latency p50/p99 recorded
- [ ] Hibernate → wake on another node restores state, last id, cursors and dedupe exactly

## M4: cluster

- Virtual shards, node lease in the bucket, CAS shard claims with epochs, epoch in object paths.
- Routing table + REDIRECT; SDK reconnect with jitter; TLS ticket keys shared across nodes.
- Failover: claim, attach WAL volume (or local test equivalent), replay, then accept writes.

**Accept when:**
- [ ] Deterministic simulation tests in `crates/sim`: node crash, network partition, zombie owner, clock skew, with no lost acked writes, no duplicate ids, no writes accepted by a stale epoch
- [ ] Measured failover time for 100k streams

## M5: sinks, retention classes, compaction

- Sink runtime: pool, budgets, cursor `sink:<id>`, retries, pause, `on_expire`, timer index, status streams.
- Plugins: webhook.notify, webhook.deliver (HMAC, SSRF guard, HTTP/1.1 + HTTP/2, ticket cache), forward, archive.
- Sink lifecycle operations.
- Retention classes (`7d`, `30d`, `365d`) with a segment stream per class.
- Compaction as a consumer group on `_sys/seals/<class>`, catalog per `(class, bucket)`, watermark W, levels.

**Accept when:**
- [ ] A webhook receiver that fails for an hour receives everything afterwards, in order, with dedupable keys
- [ ] Forward retries never duplicate messages in the target
- [ ] Reading a 30-day stream after compaction uses a bounded number of GETs

## M6: groups and consumer groups

- Sealing, stale-version check (error 9).
- Group SDK: jump hash placement, resize by CAS, moved markers, ordering hold, retirement.
- Consumer groups: shared cursor + pending list with deadlines and redelivery.

**Accept when:**
- [ ] Grow 4 → 8 → 5 partitions under load: no lost lines, per-producer order preserved
- [ ] A worker crash in a consumer group leads to redelivery within the deadline

## Later

kTLS + io_uring thread-per-core runtime; Parquet/Iceberg and Pipelines sinks; QUIC/WebTransport; the block codec for high-volume logs; compact single-message frames; a cross-AZ peer option.
