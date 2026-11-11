# Read cache for catch-up: design note

Status: a proposal for discussion. Pieces 3 and 4 are done (SPEC.md section 5.1); the rest is not in SPEC.md yet. Once decided, the parts we keep move into section 7.3 and this note goes to `docs/old/`.

Pieces 3 and 4, measured locally (`crates/server/tests/catch_up_bench.rs`: in-memory store, every GET waits 50 ms, one stream, 4 MiB reads, 4 in flight):

| | before | after |
|---|---|---|
| 256 MiB in 1 object | 71.7 MiB/s | 232 MiB/s |
| 64 MiB in 4 objects | 46.8 MiB/s | 86.9 MiB/s |
| 64 MiB in 32 objects | 18.4 MiB/s | 18.4 MiB/s |

Before, a GET was in flight 98–100% of the time and never more than one data read at once: the serial reads were the cost. Read-ahead keeps 4 in flight within a region. Moving to the next object still waits for its region lookup (top index, page, region: 3 GETs in a row), and the owner names only the next object, so streams with small regions per object gain little. Naming more of the list ahead would fix that.

## Why

A catch-up read that misses the local segment directory makes ranged GETs against the object store: one 4 MiB (`catch_up_read_bytes`) read at a time per subscription, serial. Every pass over history pays the bucket again.

`86b1262` tried whole-object fill: on a miss, the uploader copied the entire object into `cached/`. It was reverted because its cost grows with the number of streams:

- A sealed object holds every stream the node sealed in that window. A reader of one stream pulled all of them; with 4 streams the bucket served about twice the bytes on the first pass.
- Four copies at a time, and extra requests were dropped. Under many readers, which objects got cached was luck.
- At 1M streams with many independent catch-ups, almost all of each copied object is data nobody asked for.

Measured on GCS, 2026-11-08 (docs/BENCH.md), 4 streams, 9.9 GB:

| | first pass | second pass |
|---|---|---|
| no fill (2026-11-07) | 33–34 s | 33–34 s |
| whole-object fill | 39.6 s | 16.5 s (all local) |

The goal: a first pass no slower than no fill, and a second pass as fast as whole-object fill, without fetching bytes no reader asked for.

## Design targets

- 1M streams per node, most of them live and idle, any number catching up independently and in different streams.
- Popular history (many readers of the same stream and range) costs the bucket once, like a CDN.
- Bucket reads in flight are bounded per node and shared fairly; one heavy reader can't starve the others.
- Memory is bounded independent of the number of subscriptions.
- The runtime still never looks at record data; the cache stores bytes by object and range.

## Proposed pieces

1. **Range cache.** Keep the bytes a catch-up read already fetched, keyed by (object id, aligned block). No extra GETs: the cache holds exactly what readers fetched. Block alignment (for example 4 MiB) lets overlapping readers share entries.
2. **Folded misses.** Concurrent reads of the same block wait on one in-flight GET instead of each issuing their own. Ten readers of the same stream from the start cost one pass over the bucket.
3. **Node-wide read limit.** (Done: `object_reads_in_flight`, FIFO, and `read_ahead_bytes`.) One semaphore for bucket GETs in flight on the node, handed out round-robin per subscription, plus a byte bound on buffered blocks. Replaces "one outstanding read per subscription" as the thing that protects the bucket and memory.
4. **Read-ahead within a stream.** (Done: `catch_up_read_ahead`, 4.) A subscription that is behind may hold up to N blocks in flight (within the node limit), and may start on the stream's region in the next object before finishing this one. This is what makes the first pass faster than today, not just the second.
5. **Storage for many small entries.** At 1M streams, entries are many and small. Options:
   - append-only cache files with an in-memory index (object, block) → (file, offset), evicted a whole file at a time, oldest first;
   - the metadata KV store, for small blocks only.
   The first is simpler and keeps the cap in section 7.3 as one number.
6. **Compaction** (section 7.5) lowers the object count per stream, so a range cache sees fewer, larger, mostly single-stream ranges over time. Not new, but it changes how well the cache works.

## Open questions

- Block size: 4 MiB matches the read size today, but small streams waste most of a block. Cache only the requested range, and align only the GET?
- Should the cache count against the same cap as `cached/`, or have its own?
- Is the page cache enough for hot blocks, so the cache only needs to survive on disk for second passes after eviction?
- Fairness unit: per subscription, per stream, or per connection?
- Prefetch the next object's region: worth it, or does read-ahead within one object already hide the latency?

## How to measure

The same GCS layout as BENCH.md, plus:

- the 4-stream first and second pass, compared to the table above;
- 64 readers of one stream from the start (folding);
- 10,000 streams with small backlogs and 1,000 concurrent catch-ups (fairness, memory, bucket GETs per second).
