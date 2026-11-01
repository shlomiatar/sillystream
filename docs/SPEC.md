# Sillystream specification

Status: being rewritten from the basics, pre-implementation. Mechanisms are brought over from `docs/old/` on purpose; anything not listed here does not apply. All latency figures are estimates to be benchmarked.

## 1. What it is

An extremely simple system for working with streams. It is a primitive for agents synchronizing with each other, event buses and log aggregators.

A stream has an id, an ordered log of records, cursors, sinks, and a state of up to 64 KB.

Stream ids are UTF-8 names without `/`, such as `inbox.reviewer` or `logs.payments.3`.

One owner assigns ids for each stream, so a stream has a total order without consensus on the write path.

## 2. Record

A record is one group of one or more items that share the same `options` and `author`. A single message is simply a record with `count = 1`, so there is one format for both.

```
offset size field
0      8    id        u64, the first item's id; item i has id + i
8      8    options   u64, app-defined, used for filtering
16     8    author    u64, 0 = system
24     4    len       u32, data bytes
28     2    count     u16, items in this record, 1–65,535
30     2    codec     u16, set by the client, ignored by the server: 0 raw, 1 zstd
32          data, padded to 8
```

All fields are little-endian and naturally aligned.

- **Id:** high 48 bits are ingest time in ms, and low 16 bits are a counter within that ms. The id is both the item's position and its timestamp. The owner assigns it. If a ms runs out of counter values, the owner borrows the next ms, because ids only need to increase. Ids are never reused.
- **Author:** a non-zero id for the source, such as an agent id or `hash(host)`. A hash that comes out as 0 is remapped to 1. The token issuer chooses it and the token carries it. Producers can't choose it; the owner writes it when stamping.
- **Data:** opaque to the runtime, with one exception (section 2.2). Producers set it, and it can be compressed, encrypted or anything else. The runtime never parses or decompresses it, which lets it move data by copying bytes.
- **Items:** with `count = 1`, data is the item's bytes. With `count > 1`, data is client-owned item offsets and item bytes, usually compressed together. Any per-item details, such as event time, also live inside the data.

### 2.1 Options layout

`options` belongs to the application. For example, for logs:

```
bit 63      error flag
bits 56–62  level
bits 40–55  service
bits 24–39  region
bits 0–23   app-defined
```

Records group on an exact `options` value, so high-cardinality fields such as request ids shrink groups to one item and lose the compression. They belong inside the data.

### 2.2 System records

A record with `author = 0` is a system record. **The server reads a record's data only when `author = 0`; everything else is opaque.**

- Clients can't write these directly, because authors come from the token and are never 0. The server writes them.
- On a system record, `options` belongs to the server and holds the entry type: 1 state set, 2 state merge, 3 state incr.
- Data is `actor u64 | doc`, where `actor` is the author who made the change.
- Subscribers receive system records only if they ask, using `INCLUDE_SYSTEM`.

## 3. Publishing

A publish is one or more records, and it commits as one unit with consecutive ids.

- **Order:** the SDK sorts stably by options, keeping producer order within each options value. Ids follow that order. Order across publishes is preserved.
- **Stamping:** the owner walks the record headers, writes `id` and `author`, and never re-encodes anything.
- **Idempotency:** `(producer_id, producer_seq)`, carried on the publish frame and in the WAL entry, not in records. Duplicates are dropped and never re-stamped. `producer_seq` must increase per stream, and gaps are allowed.
- **Dedupe entries:** one per `(stream, producer_id)`, about 28 bytes, holding the highest `producer_seq` with its `first_id` and `count`. Entries expire after 24 hours idle, with a cap of 1,000 per stream that evicts the oldest first. A producer retrying after its entry has expired can create a duplicate.
- **Retries:** a retry of the latest publish gets the original `ACK`. A retry of an older one is dropped and acked with `first_id = 0`, meaning it is already committed and its id wasn't kept.
- **Validation:** the owner rejects the whole publish with error 8 if the record lengths don't add up exactly to the frame, `count = 0`, `id` or `author` isn't 0, padding isn't 0, or a state doc is over 64 KB or isn't valid CBOR. This never reads a record's data. Forward sinks may set `author`, because they keep the original.
- **Rejection:** a rejected publish gets a `NACK`, commits nothing and doesn't use up its seq. The SDK waits for the ack on a publish with `expect_ver` before sending later ones from that producer, because a later seq succeeding would make the retry look like a duplicate. Other publishes can be sent in parallel.
- **Compression:** lives entirely in the client. Similar items compress together much better, which is why grouping by options is effective. A dictionary id, if used, goes in a prefix inside the data.

## 4. Filtering

Filters use only `options`, time (through the id) and `author`. The server decides on each record's header and never opens its data, and a match is exact because all items in a record share the same `options`.

- Time: an id range, from any point to any point.
- Author: equality on `author`.
- Options: up to 4 clauses OR-ed together, where each clause is one of:
  - `eq`: `(options & mask) == value`
  - `any`: `(options & mask) != 0`

### 4.1 Summaries

Every segment block stores `options_or`, `options_and`, `id_first`, `id_last`, `count`, `bytes` and `author` (or "mixed"). Readers use them to skip whole blocks without looking at individual records.

For an `eq` clause with `need1 = mask & value` and `need0 = mask & !value`:

```
none match  if (need1 & !OR) != 0  or  (need0 & AND) != 0
all match   if (need1 & !AND) == 0 and (need0 & OR) == 0
else maybe
```

For `any`: none match if `OR & mask == 0`; all match if `AND & mask != 0`.

## 5. Readers

A reader is a cursor plus a filter. Multiple readers can each read from any point to any point. There are two kinds, which differ in who holds the cursor:

| reader | who holds the cursor | how it works |
|---|---|---|
| connected client | the client, or the server for a named cursor | reads while connected and resumes from its cursor after reconnecting |
| sink (section 9) | the server | the server pushes to a destination that can't hold a cursor, such as a webhook |

### 5.1 Cursors

- Positions: `from = 0` is the start of retention, `u64::MAX` is live only, and anything else means "after this id".
- Reads come from memory, then the local disk, then S3. A reader catching up becomes live when it reaches recent data in memory.
- A client-held cursor needs nothing on the server.
- A named cursor is stored on the server as `(stream, name) → id`. It moves forward only, through `COMMIT`, which gives at-least-once delivery. Rewinding is an explicit reset.

### 5.2 Delivery to connected clients

- **Records are the unit of delivery.** The server sends the original bytes of each matching record, with no copy and no re-encoding.
- **Resuming inside a record:** the server sends the whole record, and the client skips ahead by id.
- **Filter table:** each stream keeps a table of its distinct filters, with subscribers listed under each. A record is checked once per distinct filter, and all matching subscribers share the result.
- **Visibility:** a record is delivered once it is in the WAL, and delivery never waits for S3. Streams that use only ack levels 0–1 deliver immediately, and accept that a crash can erase a record someone already saw.
- **Ordering:** records arrive in id order.
- **Linger:** each subscription has a `linger_ms`. Pending records go out when the timer fires or the queue reaches the maximum frame size.
- **Progress:** after skipping records, the server sends `PROGRESS` at most every few seconds and when the subscriber catches up to the head, so a rare filter doesn't rescan from its old position after a reconnect. It means nothing matches up to that id, and a named cursor can commit it.
- **Slow clients:** queues are bounded in bytes. On overflow, that subscription gets `BEHIND {resume_from}` and falls back to catching up.
- **Many subscriptions per connection:** each has its own id, filter and cursor, and bytes are shared fairly between them.

## 6. Node architecture

A node has stream-owning cores, one WAL writer per volume and one uploader. They share no locks and communicate only through messages.

### 6.1 Stream-owning cores

Each core exclusively owns a set of streams, including their ids, buffers and subscribers. This is a design requirement, because it determines the system's throughput limits.

- **Placement:** stream → virtual shard → core. A shard is a bucket of streams, so moving one moves all its streams with one small update. A small override table can give a very hot stream its own shard.
- **Counters per shard:** ingest bytes and records, fan-out bytes, subscribers and catch-up bytes, as moving averages.
- **Rebalancing:** a periodic check moves shards off overloaded cores through a message handoff.
- **Hard limit:** one stream can't ingest more than one core can handle. Fan-out can spread further, because other cores forward the shared bytes to their own connections.

### 6.2 Write path

1. The owning core reads the publish into a slab (section 6.5), dedupes on `(producer_id, producer_seq)` and stamps the records in place.
2. It passes a reference to the bytes to the WAL writer, without copying.
3. The WAL writer appends and fsyncs, then tells the cores what is durable. The cores send the acks and deliver to subscribers.
4. At seal, each core hands the uploader its references, sorted by stream, and the uploader uploads one segment.
5. The local copy stays as a read cache until disk pressure evicts it.

Local disks are caches, except for the unsealed WAL. The source of truth is the WAL until a segment is sealed, and S3 after that.

### 6.3 WAL writer

One preallocated append-only file, written by one thread per volume, using crossbeam channels. It is read only during crash recovery and recycled once the segments covering it are uploaded.

```rust
loop {
    let first = rx.recv()?;                    // wait for work
    let batch = once(first).chain(rx.try_iter());
    write_all(batch);                          // writev of the queued bytes
    file.sync_data()?;
    notify_cores(durable_positions);
}
```

- **Group commit needs no timer:** whatever queued during the last fsync goes in the next one, which gives one group commit across the whole node.
- **Entries:** `stream | producer_id | producer_seq | len | crc32c`, then the record bytes. The CRC detects torn writes during recovery.
- **Backpressure:** the channel is bounded and cores use `try_send`. A full queue becomes backpressure on publishers and never blocks a core.
- **Waking cores:** cores can't block on a crossbeam `recv`, so the writer wakes them through a mechanism they can await.

### 6.4 Uploader

One thread per node, following the same pattern as the WAL writer. It produces **one object per node per seal**, so a node's PUT count doesn't depend on its stream count.

A stream lives on one core, so the cores' sections don't overlap. The uploader writes the sections one after another from the references it received, already sorted by stream, and combines their footer indexes. It doesn't sort or copy data. A stream that moved cores during the window appears in two sections, so the footer allows two entries for it.

### 6.5 Memory

- **Slabs:** each core reads into large shared slabs, so records need no individual allocation. The WAL, the recent-record ring and the uploader all refer to the same memory as `(slab, offset, len)`, so a record is allocated once on its way from the socket to S3.
- **Reference counts:** per slab rather than per record, so the hot path has no per-record atomic operations.
- **Idle connections:** a connection holds no read buffer while idle. It waits until the socket is readable, borrows a buffer from a pool and returns it afterwards.

## 7. Storage and durability

### 7.1 Durability requirements

The operator chooses two node settings, which together decide what a level 2 ack guarantees:

- **`wal_dir`:** the directory for the WAL. To the server it is just a path; the guarantee comes from what the operator mounts there.
- **Seal window:** 1–120 s, 10 s by default. All streams on a node share it, which is what lets them share one object per seal.

What to mount at `wal_dir`:

| mounted | ack latency | process or kernel crash | machine loss | AZ loss |
|---|---|---|---|---|
| EBS | 1–4 ms | safe | safe | up to one seal window lost |
| local NVMe | 0.1–2 ms | safe | up to one seal window lost | up to one seal window lost |

An acked record survives machine loss only if `wal_dir` is on EBS. The default path doesn't guarantee this, so deployment docs must say to mount EBS there.

A shorter window loses less and makes level 3 acks faster, but costs more PUTs: about 2.6M a month per node at 1 s, 260K at 10 s, and 22K at 120 s.

Sealed data is safe in S3. Surviving the loss of a region is out of scope.

### 7.2 Ack levels

Each publish asks for an ack level, and the ack is sent only after that level is met. The default is level 2.

| level | ack after | typical | slow case | lost on |
|---|---|---|---|---|
| 0 | nothing | none | none | anything |
| 1 | memory | under 0.1 ms | under 1 ms | process crash |
| 2 | WAL fsync | 1–4 ms on EBS, 0.1–2 ms on NVMe | 10+ ms on EBS, about 5 ms on NVMe | see section 7.1 |
| 3 | segment in S3 | half a window + 20–100 ms | one window + 200 ms | regional S3 failure |

### 7.3 Segments

- Written every seal window, or earlier when the size cap is reached (32–128 MB).
- Sorted by stream within each core's section, with per-block summaries and CRCs. Readers never scan.
- **Two-level footer:** the stream index `stream → (offset, len, id range)` would be about 32 MB at 1M streams, so it is split into fixed-size pages sorted by stream hash, with a small top index over the pages. A reader fetches the top index once and caches it, then makes one small range read for the right page and one for the data. A reader with a stored extent skips the footer entirely.
- There are no per-stream S3 objects on the write path, except that a stream filling its own buffer within the window gets a dedicated object.
- Path: `sealed/<ingest_hour>/<node-or-stream>/<generation>-<first_id>.seg`.
- Plain files. Segment bytes are the same as wire bytes.

### 7.4 Retention and deletion

Retention applies to records only, by ingest time, at a per-stream class (`1d` by default, then `7d`, `30d` or `365d`). State, cursors, ids and config last until the stream is deleted.

Deletion is purely retention: whole hour prefixes expire, and nothing else deletes data.

- New segments go to the `sealed/` prefix, which expires after about 2 days. The `1d` class is served from here and is never compacted.
- Longer classes are copied by compaction into their own prefix, which expires by that class.
- The delete job checks how far compaction has progressed before expiring anything, so a stalled compactor can't destroy data. An S3 lifecycle rule alone can't do that.

### 7.5 Compaction

Seal manifests are published to a system stream. A compactor follows it with a named cursor, with one worker per retention class, and merges small segments into larger ones sorted by stream. It copies records without decoding them, then updates the index readers use to find them. It deletes nothing.

Unlike a normal sink, it must update the read index. The manifest stream also can't produce manifests for its own segments, so it needs a special rule.

### 7.6 Stream metadata

A stream is in one of two states, and S3 backs both:

- **Active:** in RAM. Anything with a live subscriber stays active.
- **Idle:** evicted from RAM, least recently used first, to a local key-value store on NVMe, and loaded on the next access.
- **Backup:** if the local copy is gone, the stream is rebuilt on demand from its S3 checkpoint.

The metadata is last id, the highest expired id, state, config, cursors, dedupe entries and extents (`id range → memory, local or S3 location`). A position below the highest expired id gets error 7, including after a restart.

At each seal, changed metadata is written to the local store and to S3 checkpoints. Checkpoints go into shared files, so PUTs don't grow with stream count, and they are separate from segments because they must outlive record retention. The key-value store stays out of the write path.

## 8. Ownership and recovery

The first version runs on a single node. Shards stay, because cores need them for placement and rebalancing.

### 8.1 Single node

- **Startup:** the server replays whatever is in `wal_dir`, then carries on from S3. The code is the same in every case below.
- **Process crash:** the process restarts and replays the WAL.
- **Machine loss:** something external, such as an auto-scaling group, starts a replacement. With EBS, that automation reattaches the volume, and force-detaching it stops the old machine's WAL writes. With local NVMe, the WAL is gone.
- **Empty WAL check:** if `wal_dir` is empty but the ownership record already has a generation, the server logs a warning that the WAL was lost and that unsealed records may be gone. It can't tell how many. A new node has no generation yet, so it doesn't warn.
- **Clients:** resume from cursors, and producers retry with the same `(producer_id, producer_seq)`.

A replacement could start while the old machine is still running, and the old machine could still upload a segment. So one ownership record in the bucket holds a `generation` and the last valid id of the previous generation. A new process takes ownership with a conditional write that increments the generation. The generation is in every object path, and readers accept an older generation's data only up to that last valid id.

### 8.2 Multiple nodes (designed, deferred)

Not part of the first version. The record format and storage layout don't need to change to add it.

- **Ownership:** each node renews a lease record in the bucket and owns its shards while it is alive.
- **Routing:** clients find owners through a routing table and follow `REDIRECT`.
- **Epochs:** each shard has an epoch, a counter that goes up each time its ownership changes. It replaces the single generation of section 8.1. The segment footer records the epoch for each shard it contains, because one object holds many shards.
- **Takeover:**
  1. A node's lease expires.
  2. A node in the same AZ claims its shards with a conditional write of `epoch + 1`, recording the last valid id of the previous epoch.
  3. If the dead node's WAL is on a volume that can move, it attaches the volume and replays everything after the last sealed segment. Otherwise it recovers from S3 alone. This needs a setting naming the volume, added with this design.
  4. It starts accepting writes.
- **Readers:** accept an older epoch's data up to its last valid id, and ignore anything later, which is what a stalled former owner could have written.

## 9. Sinks

A sink is a source stream, a filter, a destination, and a named cursor `sink:<id>`. When a matching record arrives, the sink sends it on. Unlike a connected client, it runs on the server and retries.

- **Destinations:**
  - `webhook.notify`: sends `{stream, head, unread}`, rate-limited, for wake-ups.
  - `webhook.deliver`: sends records, HMAC-signed, with private addresses blocked.
  - `forward`: writes into another stream.
  - `archive`: writes records to S3 or R2.
- **At-least-once:** the cursor advances after delivery succeeds. Destinations dedupe with `{sink_id, first_id, last_id}`.
- **Ordered by default:** one delivery is in flight at a time. Concurrency is opt-in.
- **Failures:** retryable errors back off exponentially. Permanent errors or too many failures pause the sink. Before unread data expires, the sink warns, then follows `on_expire: stop | skip`, and a skip records the gap.
- **Lifecycle:** create (cursor at `now`, `earliest` or an id), update (in-flight work finishes first, the new spec starts at the next record boundary, the cursor stays), pause and resume, reset the cursor, delete.
- **Isolation:** sinks run on the node that owns the source stream, in a pool separate from the write path, with per-sink byte and concurrency budgets. They move with their shard.
- **Reading data:** any export that needs to read data contents runs as a client-side worker holding the keys.

### 9.1 Forwarding between streams

A `forward` sink publishes with a `producer_id` the server derives from the source stream and `sink_id`, with the top bit set, and `producer_seq = source first_id`. A publisher that sets the top bit is rejected with error 8, so forwards have a dedupe key space of their own. Retries therefore dedupe through normal idempotency and never duplicate records in the target. It keeps each record's original author.

There are no server-side wildcards. A feed across many streams is a fan-in: forward sinks into one aggregate stream.

## 10. State

State is how things stand now, consistent with the log. It is the one part of a stream's content that outlives record retention.

- CBOR, plaintext to the server, so the server can apply merges. At most 64 KB.
- Good for plans and status of agent sessions, rolling counters, leases (the version is the fencing token) and room metadata. Not for growing lists, large blobs (store pointers), high-frequency updates or secrets.

| op | code | meaning |
|---|---|---|
| set | 1 | replace state |
| merge | 2 | RFC 7396-style merge patch over CBOR maps |
| incr | 3 | add n to a numeric path |

- **In the log:** each change is a system record (section 2.2) holding the operation, and `version` is that record's id. The current state is the result of applying the operations. Retention can delete old entries, so the snapshot is stored separately too.
- **Conditional changes:** any op can carry `expect_ver` (0 = unconditional). A mismatch fails with a version conflict.
- **Atomic changes:** a publish can include one state change. Its records and the state change commit together with consecutive ids, or neither does.
- **Snapshot + read:** the owner returns the state and `head` as a single consistent step, and the client reads from `head + 1`.
- **Storage:** active streams keep state in RAM. A change goes into the WAL, is applied in RAM, and is then acked. At each seal, changed states go to the local key-value store and the S3 checkpoint.
- **Recovery:** load the last snapshot and replay state operations from the WAL.

## 11. API

### 11.1 WebSocket

Binary. One WebSocket message is one frame: `type u8 | flags u8 | body`. Little-endian. `str` is a u16 length + UTF-8, and `bytes` is a u32 length + data. No WebSocket compression. TLS 1.3.

```
client → server
0x01 HELLO        ver u16 | token str
0x02 SUBSCRIBE    sub u32 | from u64 | linger_ms u16 | [author u64 if HAS_AUTHOR] |
                  nfilt u8 | nfilt × (mask u64 | value u64 | op u8) | stream str |
                  [name str if NAMED_CURSOR]
                  flags: 0x01 WITH_STATE  0x02 STATE_ONLY  0x04 NAMED_CURSOR
                         0x08 INCLUDE_SYSTEM  0x10 HAS_AUTHOR
0x03 UNSUBSCRIBE  sub u32
0x04 OPEN         handle u32 | stream str
0x05 PUBLISH      handle u32 | producer_id u64 | producer_seq u64 | ack u8 |
                  [expect_ver u64 | op u8 | doc bytes if HAS_STATE] | records bytes
                  flags: 0x01 HAS_STATE
0x06 STATE_OP     handle u32 | req u32 | expect_ver u64 | op u8 | doc bytes
0x07 COMMIT       sub u32 | id u64
0x08 PING         nonce u32

server → client
0x81 WELCOME      session u64 | max_frame u32 | server_ms u64
0x82 SUBSCRIBED   sub u32 | head u64 | state_ver u64 | state bytes
0x83 DATA         sub u32 | records bytes
0x84 ACK          handle u32 | producer_seq u64 | first_id u64 | count u32
0x85 STATE        sub u32 | ver u64 | doc bytes
0x86 BEHIND       sub u32 | resume_from u64
0x87 PROGRESS     sub u32 | id u64
0x88 STATE_RESULT req u32 | ok u8 | ver u64
0x89 REDIRECT     stream str | host str   (multiple nodes only, deferred)
0x8A NACK         handle u32 | producer_seq u64 | code u16 | msg str
0x8E ERROR        ref u32 | code u16 | msg str
0x8F PONG         nonce u32
```

Error codes: 1 unauthorized, 2 not found, 3 version conflict, 4 rate limited, 5 too many subscriptions, 6 frame too large, 7 retention expired, 8 malformed.

### 11.2 HTTP

```
GET   /v1/stream/<id>                stream info and head
POST  /v1/stream/<id>/publish        publish records
GET   /v1/stream/<id>/read?from=&limit=&filter=
GET   /v1/stream/<id>/state
PATCH /v1/stream/<id>/state          If-Match for expect_ver
PUT   /v1/stream/<id>/cursor/<name>
```

Binary records are the standard format for publish and read. Publish also has a simple form that needs no SDK or JSON, where the body is one item's bytes and the rest goes in headers:

```
POST /v1/stream/logs.app/publish?ack=2
X-Options: 0x8000000000000000
X-Producer-Id: 7
X-Producer-Seq: 42
```

Reads are binary only for now.

## 12. Auth

A signed token grants access to streams. It lists stream id patterns such as `logs.*`, permissions, an author id and an expiry.

- The server verifies it without a lookup.
- Revocation is handled by short expiry.
- It goes in `HELLO` on WebSocket and as a Bearer token on HTTP.
- Each subscribe, publish and state op is checked against the stream. Access is per stream, so anything private goes in its own stream.

| permission | allows |
|---|---|
| `read` | subscribe, read, commit a cursor |
| `publish` | publish records |
| `state` | change state, including in a publish |
| `admin` | config, sinks, delete a stream |

## 13. Configuration

- **Per node:** `wal_dir` and seal window (section 7.1).
- **Per stream, set when it is created or changed later:** retention class, default and minimum ack level, and sinks. Streams are created on first publish or first state write, or explicitly.

## 14. Open questions

None at the moment.

## 15. Not brought over

These are in `docs/old/` and don't apply unless we bring them back: config templates and option layouts by name, TLS resumption and 0-RTT, SSE, capacity targets, and the roadmap.

Decided against: partitioned groups, consumer groups, event time and watermarks in the server (they can live inside record data), and the warm, parked and hibernated tiers (replaced by section 7.6). Registry streams are out of scope because they can be composed on top.
