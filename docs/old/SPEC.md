# Sillystream specification

Status: design, pre-implementation. All performance figures are estimates to be benchmarked.

## 1. Model

- **Stream**: an address (UTF-8 path, for example `inbox/reviewer`, `logs/payments/3`) + an ordered log of messages + a state (≤ 64 KB user state, plus a small `_sys` part).
- **Producers** publish messages. **Subscribers** hold a cursor and read live or catch up. **Named cursors** are server-stored positions.
- **Access is all-or-nothing per stream** (read / publish / admin permissions). Anything private goes to its own stream.
- **Message retention** is set by the stream's retention class (`1d` default; `7d`, `30d`, `365d`). State, cursors, ids and config are kept until the stream is deleted.
- **Creation**: implicit on first publish or first state write, configured from the matching template; or explicit `create`.

## 2. Message

Logical view of one message:

| field | type | notes |
|---|---|---|
| id | u64 | high 48 bits: ingest ms; low 16 bits: sequence within that ms. Assigned by the owner. If a ms overflows, borrow the next ms; ids only need to increase. |
| options | u64 | filter metadata, app-defined layout (see 2.1) |
| author | u64 | keyed hash of the writer identity (who wrote it) |
| event_time_ms | u64 | producer clock; computed as `base_ms + event_delta` |
| payload | bytes | opaque to the server; max 1 MB; raw or zstd-compressed on its own |

`author` (who) is distinct from `producer_id` (one sending instance, used only for idempotency).

### 2.1 Options layout

`options` belongs to the application. Convention: put the most-filtered fields in the high bits (they sort and summarize best), for example for logs:

```
bit 63      error flag
bits 56–62  level
bits 40–55  service
bits 24–39  region
bits 0–23   app-defined
```

A stream references a named, versioned layout (`logs-v1`) in its config so consumers know the meaning of the bits.

### 2.2 Filters

A filter is up to 4 clauses OR-ed together, plus an optional `author` equality.

- `eq` clause: `(options & mask) == value`
- `any` clause: `(options & mask) != 0`

### 2.3 Summaries

For any group of messages (batch, segment block, compacted file row group), store `options_or`, `options_and`, `id_first`, `id_last`, `count`, `bytes`, `event_min_ms`, `event_max_ms`, `author` (or "mixed"). For an `eq` clause with `need1 = mask & value`, `need0 = mask & !value`:

```
none match  if (need1 & !OR) != 0  or  (need0 & AND) != 0
all match   if (need1 & !AND) == 0 and (need0 & OR) == 0
else maybe
```

For `any`: none if `OR & mask == 0`; all if `AND & mask != 0`.

## 3. Batch format (wire and storage)

A batch is a list of independent messages sent together. It never changes message semantics.

```
offset size field
0      1    ver            (1)
1      1    flags          bit0 HAS_AUTHOR_COL, bit1 SPARSE, bit2 SYSTEM, bit3 HAS_STATE_OP
2      1    codec          0 none, 1 zstd-msg (each payload compressed separately)
3      1    reserved       0
4      4    count          u32
8      8    first_id       u64, 0 from producer; stamped by owner
16     8    producer_id    u64
24     8    producer_seq   u64
32     8    base_ms        u64, event-time base
40     8    watermark_ms   u64, producer has sent everything up to here
48     8    author         u64, default author for all messages
56     4    dict_id        u32, 0 = no dictionary
60     4    crc32c         u32, over the whole batch with first_id and crc32c set to 0
64          columns, each `count` entries, each column padded to 8 bytes:
            options      [u64]
            len          [u32]   stored payload bytes
            event_delta  [u32]   ms after base_ms
            author       [u64]   only if HAS_AUTHOR_COL
            index        [u32]   only if SPARSE: original positions in the source batch
            payloads     concatenated; offset of message i = sum of len[0..i]
            state_op     only if HAS_STATE_OP (see 5.2), after the payloads
```

- Message i has id `first_id + i` (or `first_id + index[i]` when SPARSE).
- `SYSTEM` batches carry system entries (state changes, group markers). Subscribers do not receive them unless they ask (`include_system`).
- Max batch (and frame) size: 4 MB by default, announced in `WELCOME`.
- Dictionaries are trained by producers or the SDK, published as blobs, referenced by `dict_id`, and opaque to the server.
- A future block codec (frames of several payloads, sorted by `(options, time)`, per-frame summaries) is reserved as a new `codec` value.

## 4. Reads and delivery

- **Positions**: `from = 0` start of retention; `u64::MAX` live only; otherwise "after this id".
- Every subscriber is a cursor. The server sends everything from the cursor to the head that matches the filter. Live subscribers sit at the head; catching-up subscribers read from memory, NVMe or S3 and become live when they reach the hot ring.
- **Delivery is batches.** Full match → the stored batch bytes (zero copy). Partial match → a SPARSE batch: matching column entries + `index` + their payload bytes, sliced. No match → nothing. Summaries skip whole batches.
- **Guarantees**: batches arrive in id order; an unfiltered subscriber never sees part of a batch; resuming inside a batch returns a slice from that index.
- **Linger**: per subscription `linger_ms` (default from profile). Pending batches are sent when the timer fires or the queue reaches max frame bytes, in one write. With 0, natural batching still happens under load.
- **Slow subscribers**: bounded queues in bytes. On overflow, `BEHIND {resume_from}` for that subscription only; it falls back to catch-up.
- **Many subscriptions per connection**, each with its own sub id, filter and cursor. Fair scheduling by per-subscription byte budget (deficit round-robin).
- **Watermark**: the stream publishes the minimum `watermark_ms` over producers active in the last 30 s.

### 4.1 Cursors

| kind | where | for |
|---|---|---|
| client-held | in the client's resume token | live UIs, tailing from now |
| named | server: `(stream, name) → id` | agents, workers, scheduled jobs, inboxes |
| consumer group (later) | server: shared cursor + pending list with deadlines | several workers sharing a stream with redelivery |

Named cursors move only forward via `COMMIT` (at-least-once); rewinding is an explicit reset. A name belongs to one credential; one active instance at a time.

## 5. State

State is how things stand now, consistent with the log. A joiner gets `(snapshot, version)` and streams from `version + 1`. It is the only part of a stream that outlives message retention.

- CBOR, plaintext to the server. User part ≤ 64 KB logical; typical < 4 KB. Stored compressed with server-trained dictionaries.
- `version` = id of the last state change. Each change is also a SYSTEM log entry.
- `_sys` part, server-managed, ≤ 4 KB, separate from the user budget (see 9).

### 5.1 Operations

| op | code | meaning |
|---|---|---|
| set | 1 | replace user state |
| merge | 2 | RFC 7396-style merge patch over CBOR maps |
| incr | 3 | add n to a numeric path (optional) |

Any op may carry `expect_ver` (0 = unconditional); a mismatch fails with "version conflict".

### 5.2 Atomic message + state

A batch with `HAS_STATE_OP` ends with one state op. The messages and the state change commit together, or neither does.

### 5.3 Usage guidance

Use for: plans and status of agent sessions, task views, leases (state version = fencing token), fleet config, folded long-term memory, room metadata. Not for: growing lists, large blobs (store pointers), high-frequency updates, per-user data in shared streams, secrets.

## 6. Protocol (WebSocket, binary)

One WebSocket binary message = one frame: `type u8 | flags u8 | body`. Little-endian. `str` = u16 length + UTF-8. `bytes` = u32 length + data. No WebSocket compression. SSE is a read-only fallback (event id = message id).

```
client → server
0x01 HELLO        ver u16 | token str | resume bytes
0x02 SUBSCRIBE    sub u32 | from u64 | linger_ms u16 | author u64 (0 = any) |
                  nfilt u8 | nfilt × (mask u64 | value u64 | op u8) | address str |
                  [name str if NAMED_CURSOR]
                  flags: 0x01 WITH_STATE  0x02 STATE_ONLY  0x04 NAMED_CURSOR  0x08 INCLUDE_SYSTEM
0x03 UNSUBSCRIBE  sub u32
0x04 OPEN         handle u32 | address str
0x05 PUBLISH      handle u32 | producer_seq u64 | ack u8 | batch bytes
0x06 STATE_OP     handle u32 | req u32 | expect_ver u64 | op u8 | doc bytes
0x07 COMMIT       sub u32 | id u64
0x08 PING         nonce u32

server → client
0x81 WELCOME      session u64 | max_frame u32 | server_ms u64 | resume bytes
0x82 SUBSCRIBED   sub u32 | head u64 | state_ver u64 | state bytes
0x83 DATA         sub u32 | batch bytes
0x84 ACK          handle u32 | producer_seq u64 | first_id u64 | count u32
0x85 STATE        sub u32 | ver u64 | doc bytes
0x86 BEHIND       sub u32 | resume_from u64
0x87 WATERMARK    sub u32 | event_ms u64
0x88 STATE_RESULT req u32 | ok u8 | ver u64
0x89 REDIRECT     address str | host str
0x8E ERROR        ref u32 | code u16 | msg str
0x8F PONG         nonce u32
```

- Ack levels: 0 none, 1 memory, 2 replicated (WAL fsync), 3 durable (in S3).
- Error codes: 1 unauthorized, 2 not found, 3 version conflict, 4 rate limited, 5 too many subscriptions, 6 frame too large, 7 retention expired, 8 sealed, 9 stale group version.
- Auth: one token per connection listing capabilities; each subscribe / publish / state op is checked against the stream.
- Version negotiated in HELLO; an unknown frame type is an error.

### 6.1 TLS

- TLS 1.3 via rustls; ECDSA P-256 certificates.
- Session tickets with ticket keys shared across the cluster and rotated, so clients resume on any node after failover.
- 0-RTT allowed only for HELLO, SUBSCRIBE, PING, and PUBLISH carrying `producer_seq` (replays are deduped). STATE_OP and COMMIT wait for the full handshake.
- SDK reconnects use jittered exponential backoff.
- Later: kTLS (and NIC offload where available); QUIC/WebTransport as an optional transport for mobile and browsers.

## 7. Node architecture

- Addresses hash to virtual shards; each core runs a **lane** that owns a set of shards and everything about their streams.
- **Write path per lane**:
  1. Dedupe on `(producer_id, seq)`, stamp `first_id`, append to the active buffer; the hot ring holds a slice (no copy).
  2. Group commit every ~2 ms or ~64 KB: one WAL write + fsync, then ack the group.
  3. At seal, build the sorted segment from the WAL index and upload. One uploader per node merges all lanes into one object per seal.
  4. The local copy stays as a read cache until disk pressure evicts it.
- **Read path**: each resident stream keeps an extent list `id range → Mem | Local(seg, off, len) | Remote(obj, off, len)`. Data below the compaction watermark is found through the catalog (see 8.4).
- **Lane loop priorities**: ingest, then live fan-out, then catch-up under a byte budget. Cold history can be served by any node (sealed data is immutable).
- **Connections across lanes**: a single-stream connection moves to its owning lane after the first message. A multi-subscription connection stays; owning lanes forward one `Bytes` per destination core through SPSC rings.

### 7.1 Stream tiers

| tier | where | RAM per stream | wake cost |
|---|---|---|---|
| hot | RAM: state, hot ring, subscribers | ~5 KB | none |
| warm | metadata in RAM, state compressed or on NVMe | ~1 KB | one NVMe read |
| parked | NVMe metadata KV | ~50–100 B index | 1–2 NVMe reads |
| hibernated | checkpoint store in S3 | 0 | one range GET |

- Streams with live connections are never shed. Parking costs no S3 requests; streams leave NVMe only under disk pressure, oldest first.
- A subscription to a parked stream records a watch instead of waking it.
- Sinks of a parked or hibernated stream don't run: new data (the only thing that gives them work) wakes the stream. A small timer index handles time-driven sink work (backoff retries, expiry warnings).

## 8. Storage

### 8.1 WAL

Per lane, unsorted, group-committed. Read only during crash recovery. Deleted after the covering segment is uploaded. On AWS: a small gp3 EBS volume (survives instance loss, reattachable). On GCP: regional persistent disk (also survives zone loss). Logs profile (S3-acked): WAL on local NVMe.

### 8.2 Segments

- Built at seal, **sorted by stream**, with a footer index `stream → (offset, len, id range)` and per-batch summaries. Readers never scan: one range read per segment.
- **Hybrid placement**: each stream has a private buffer; if it reaches the size cap within the window, it becomes a dedicated object (busy streams, log partitions); otherwise it spills into the shared segment.
- **Seal by size or age**: 32–128 MB, or the window (30–60 s for WAL-acked streams; 1–5 s for S3-acked streams). Seal early on hibernate, drain, or WAL pressure.
- **One segment stream per retention class**, under its own prefix.
- **Path**: `<class>/<org>/<ingest_hour>/<stream-or-shard>/<epoch>-<first_id>.seg`.
- One seal manifest per node per seal, also published to `_sys/seals/<class>`. No S3 LIST on the read path.

### 8.3 Retention

Expire whole hour prefixes per class (own delete job; S3 lifecycle rule per class prefix as a safety net). Retention is by ingest time (id). Deleting a stream's messages early is done by the next compaction pass of the files that contain them.

### 8.4 Compaction (classes longer than 1 day)

- Compaction is a **consumer group on `_sys/seals/<class>`**.
- When a `(class, bucket, window)` is complete or large enough, a worker reads each input segment once, routes each stream's batch byte ranges to output builders, and writes files **sorted by (stream, id)** with footer indexes; busy streams get dedicated files. Batches are copied as is; payloads are never decoded.
- Output names are deterministic from inputs (retries overwrite). Commit = upload outputs, CAS the `(class, bucket)` catalog object with the new file list and **compaction watermark W**, publish `compacted` to `_sys/compactions`, commit the cursor.
- Inputs are deleted after a grace period (~1 h).
- Levels: hourly → daily → monthly, each fed by the previous level's `compacted` messages. The `1d` class is never compacted.
- Reads below W use the catalog plus file footers; above W, extent lists and sealed segments.

### 8.5 Checkpoint store

Per-shard keyed files in S3, latest version per stream wins, compacted periodically, kept until the stream is deleted. Holds hibernated streams' state, `_sys`, last id, producer dedupe table, named cursors, extents above W. One small manifest per shard per seal maps stream → checkpoint location.

### 8.6 Local metadata KV

fjall or RocksDB on NVMe: parked streams, extents, producer dedupe, cursors, sink timers. A cache, rebuildable from S3. Segments and the WAL are plain files, never stored in the KV.

## 9. Configuration

Stream configuration lives in `_sys` state (CAS-updated, every change is a SYSTEM log entry, watchable).

```
_sys = {
  template:  "logs-default@3",     // pinned, immutable template version
  overrides: { retention: "30d" }, // only what differs
  layout:    "logs-v1",            // options schema by name
  group:     { partitions, version, retired: [{p, final_id}] },  // group streams only
  sealed:    false,
  sinks:     [ ... ]               // only streams with sinks
}
```

- **Templates** match address prefixes (`logs/**`), most specific wins, applied at creation by pinning `template@version`. Templates are versioned and stored in `_sys/templates`; every node caches all live versions. `apply --to-existing` re-pins.
- Settings: `profile` (bus, logs, ephemeral), `retention` (new data only), `ack` default/minimum, `layout`, `limits`, `linger`, `group`, `sealed`, `sinks`.
- Set via templates, the API (`PATCH /streams/{address}/config` with `expected_version`), or declarative files (`sillystream apply config.yaml`). Requires the admin permission.
- Typical `_sys` size: 30–60 bytes.

## 10. Sinks

A sink = source stream (+ filter, linger, max bytes) + destination plugin (+ config, secret by reference) + named cursor `sink:<id>` + delivery policy. Status lives in the system stream `_sys/sinks/<id>` (state = current status, log = events).

- Run on the node owning the source stream, in a separate sink pool (not in lanes), with per-sink byte and concurrency budgets. They move with the shard on failover.
- **At-least-once**: the cursor advances after `deliver` succeeds; destinations dedupe with `DeliveryKey {sink_id, first_id, last_id}`.
- **Ordered by default** (one delivery in flight); concurrency is an explicit opt-in.
- Retryable errors back off exponentially; permanent errors or too many failures pause the sink. Before unread data expires: warn, then `on_expire: stop | skip` (gap recorded).
- **Lifecycle**: create (validate, test request, cursor at `now` | `earliest` | id), update (CAS new spec version; finish in-flight with the old spec, switch at the next batch boundary, keep the cursor), pause/resume, reset (move the cursor), delete. Filter and destination changes apply forward only. Type change = delete + create. Secret rotation with overlapping keys and a key id header.
- **Plugins**:
  - v1: `webhook.notify` (`{stream, head, unread}`, rate-limited; wake-up triggers), `webhook.deliver` (binary batches by default, JSON optional, HMAC-signed, private addresses blocked), `forward` (into another stream with `producer_id = sink_id`, `producer_seq = source first_id`, so retries dedupe for free), `archive` (raw batches or metadata + base64 payload to S3/R2).
  - v2: Parquet/Iceberg, Cloudflare Pipelines, Kafka, Kinesis.
- **HTTP client**: HTTP/1.1 and HTTP/2 over TLS 1.3; per-host pools with keep-alive and multiplexing; a session ticket cache for resumption; no 0-RTT; per-node connection limits; no HTTP/3.
- Any export that reads payload contents (field extraction, JSONL of payloads) runs as a client-side worker holding the keys.

## 11. Groups and patterns (SDK conventions)

No server-side wildcards. Server primitives used: state with CAS, sealing, version check on writes.

- **Group**: the group stream's `_sys.group` = `{partitions, version, retired}`. Members are `<group>/0 … /N-1`.
- Producers pick `jump_hash(xxh3_64(host), partitions)`.
- Consumers subscribe to the group stream with state; the SDK opens every partition on one connection behind one cursor.
- Resize by CAS. A moving producer first writes a `moved` marker (SYSTEM entry) in its old partition. The SDK holds a moved producer's new lines until it has seen its marker or read the old partition to its final id.
- Shrinking retires the last partition: seal with a final id, record it in `retired`, keep it readable until retention.
- Writers on a stale group version get error 9.
- The exact hash, marker format and record handling have test vectors in `crates/hash`.
- **Instead of wildcards**: partitioned stream → group; feed across many streams → fan-in via forward sink into an aggregate stream; discovery → a registry stream (`created`/`deleted` messages) with `subscribeAll(registry)` in the SDK; org-wide audit → forward or export sink.

## 12. Ownership, durability, failover

- Each node renews one lease record in the bucket and owns its shards while alive. Taking a shard = conditional write with `epoch + 1`. The epoch is in every object path; readers ignore superseded epochs.
- Clients locate owners through a cached routing table and follow REDIRECT.

| profile | ack after | ack latency | survives node loss | survives AZ loss |
|---|---|---|---|---|
| bus (agents, chat) — default | WAL fsync on EBS | ~0.5–1 ms | yes | loses ≤ seal window |
| logs | segment in S3 | ~20–100 ms | yes | yes |
| ephemeral | memory | ~0 | no | no |

**Failover**:
1. The node lease expires.
2. A same-AZ node claims the shards with `epoch + 1`.
3. It attaches the WAL volume and replays everything after the last sealed segment.
4. Then it accepts writes; streams with live subscribers come first, parked ones wake on demand.
5. Clients resume from cursors; producers retry with the same `(producer_id, seq)`.

## 13. Capacity targets (one 4-vCPU, 16 GB node with local NVMe, e.g. m7gd.xlarge)

| resource | target |
|---|---|
| hot streams | ~2M |
| warm streams | ~10M |
| parked streams | ~50–60M |
| idle WebSocket connections | 100–250k |
| unbatched publishes | 300–800k msgs/s |
| delivered frames | 0.5–1M/s (network-bound) |
| log ingest | 150–200 MB/s compressed |
| full TLS handshakes | 5–10k/s per core (more with resumption) |
| sink pool (1 core) | thousands of webhooks; ~20–50k HTTP requests/s |

Cost traps: hibernation churn for streams that wake within minutes (park on NVMe instead), cross-AZ replication, one S3 object per stream, EBS WAL for high-volume logs, internet egress.
