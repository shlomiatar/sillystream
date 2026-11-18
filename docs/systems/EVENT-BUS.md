# Event bus on Sillystream

Status: draft for review, 2026-11-12, not built. It sits on top of `docs/SPEC.md` and adds nothing to the server. `docs/PATTERNS.md` section 8 has the short version, run against a real server; this document fixes the names, the event envelope, and the ways a consumer can process events.

## 1. What it is

A bus where services publish typed events and other services consume them, each in the way that suits it: holding a connection, receiving HTTP pushes, being woken, or having events routed into a queue of their own. Producers and consumers don't know about each other.

Scope: many producers and tenants, tens of event types per domain, consumers that are services, serverless functions or agents. Out of scope: schemas enforced by the server (it never reads data), exactly-once delivery (everything is at-least-once and consumers dedupe by id), and ordering across streams.

## 2. Streams

| stream | written by | read by | retention |
|---|---|---|---|
| `bus.<domain>.in.<producer>` | one producer or tenant, with a token for this stream only | nobody directly | `1d` |
| `bus.<domain>` | `forward` sinks from every `in` stream | consumers | per domain: `7d` by default |
| `bus.<domain>.q.<consumer>` | a `forward` sink with the consumer's filter | one consumer group | `7d` |
| `bus.<domain>.dlq.<consumer>` | the consumer | operators and agents | `30d` |

- **Ingress streams** exist because access is per stream: a tenant's token can't write another tenant's events, and the token's `author` survives the forward, so `author:N` on the aggregate still says who produced an event.
- **The aggregate** is the bus proper. One stream per domain keeps an order within the domain and lets a consumer subscribe once.
- **Queue streams** are optional, per consumer (section 5.4).
- A single-producer domain can skip ingress and publish straight to `bus.<domain>`.

A domain is one stream, so it is bounded by one core's ingest (SPEC section 6.1). A domain that outgrows that is split by key into `bus.<domain>.<shard>`; ordering then holds per shard.

## 3. Events

### 3.1 Options

```
bits 56–63  entity     within the domain: 0x01 order, 0x02 refund, …
bits 48–55  event      within the entity: 0x01 created, 0x02 paid, 0x03 cancelled, …
bits 44–47  schema     major version of this event's payload
bits 40–43  class      one-hot: bit 40 fact, 41 command, 42 audit, 43 test
bits 32–39  priority   one-hot: bit 32 high, 33 bulk; 0 normal
bits 8–31   app-defined
bits 0–7    partition  hash(key) mod 256, where key is the entity id
```

A type is hierarchical, so a prefix is a filter:

| want | filter |
|---|---|
| every `order` event | `eq:0xff00000000000000:0x0100000000000000` |
| `order.paid` only | `eq:0xffff000000000000:0x0102000000000000` |
| `order.paid`, schema 2 | `eq:0xfffff00000000000:0x0102200000000000` |
| `order.paid` or `refund.created` | two `eq` clauses |
| no test traffic | not expressible as "not"; consumers that care filter on the class they want: `any:0x0000030000000000` (fact or command) |
| partition 7 of 16 | not one clause: partitions are matched by value, so a worker takes `eq:0x0f:0x07` over the low 4 bits |

A filter has at most 4 OR-ed clauses (SPEC section 4). A consumer wanting more than 4 unrelated types takes a wider prefix and drops the rest itself, or uses a queue stream fed by several sinks.

Each domain keeps its entity and event numbers in one registry file in its repository. The server knows nothing about it.

### 3.2 Envelope

One event per item. Producers batch events of the same type into one record (`count > 1`), which is where compression comes from. The item is a JSON object:

```json
{"id":"01JC…","key":"order-8812","t":1762650000123,"trace":"…","data":{…}}
```

| field | meaning |
|---|---|
| `id` | the producer's event id, unique per producer. Consumers dedupe on `(author, id)` |
| `key` | the entity the event is about. Events with one key are published by one producer in order |
| `t` | event time in ms. The record id is ingest time |
| `trace` | optional correlation id |
| `data` | the payload, shaped by entity, event and schema version |

**Schema changes:** adding a field keeps the version. A breaking change takes the next version in bits 44–47 and the producer writes both for a while. An old consumer keeps filtering on the version it knows.

**Sensitive payloads** use the encrypted codec (CLIENTS.md 2.4). Options stay readable, so types and partitions still filter.

### 3.3 Publishing

- Level 2 acks. A producer keeps one `producer_id` per process and its own increasing seq, so a retry after a lost ack is dropped (SPEC section 3).
- **Outbox:** a producer that must not lose an event writes it to its own database in the same transaction as the change, and a relay publishes from there with `producer_seq` = the outbox row number. A relay restart then republishes from its last acked row and the server drops what it already has, while the dedupe entry lives (24 h idle).
- **Event + state together:** a domain can keep a small summary in the stream's state doc (for example counts by status) and update it in the same publish as the event (SPEC section 10). It is the only cross-record invariant the bus can give.

## 4. Delivery guarantees

- **At-least-once** everywhere: a consumer may see an event again after a crash, a sink retry, or a lost dedupe entry. Every consumer keeps "last id handled" or dedupes on `(author, id)`.
- **Order:** by record id within a stream. Across a forward, each ingress stream's order is kept; two ingress streams interleave in arrival order.
- **Replay:** a consumer can re-read any range still in retention, with a new cursor name.

## 5. Processing options

Each consumer picks one. They can be mixed on one bus.

| option | consumer is | mechanism | cursor held by | parallelism |
|---|---|---|---|---|
| 5.1 subscriber | a long-running service | WebSocket subscription with a filter | the server (named cursor) | one reader per cursor name |
| 5.2 push | an HTTP service | `webhook.deliver` sink | the sink | one POST in flight |
| 5.3 wake | a serverless function or an agent | `webhook.notify`, then a read | the server (named cursor) | one reader per cursor name |
| 5.4 queue | a pool of workers | `forward` to a queue stream, then claims | the queue's readers | many, by claims or partitions |
| 5.5 archive | an analytics store | `archive` sink | the sink | n/a |
| 5.6 state view | a UI or a cache | `STATE_ONLY` subscription to a summary doc | nobody | any |

### 5.1 Subscriber

`SUBSCRIBE` with the filter and `NAMED_CURSOR` named after the consumer (`billing`). Process a `DATA` frame, then `COMMIT` its last id. On `BEHIND`, resubscribe from `resume_from`. `PROGRESS` ids may be committed too, so a rare filter doesn't rescan after a reconnect.

Only one process should use a cursor name at a time. For standby, the active one holds a lease (AGENT-COORDINATION.md section 6).

### 5.2 Push

A `webhook.deliver` sink with the filter posts batches of stored records, HMAC-signed, with `X-First-Id` and `X-Last-Id`, optionally compressed. The service returns 2xx once it has stored or handled the batch, and drops records at or below the last id it took from that sink. Failures back off; a permanent failure pauses the sink (SPEC section 9). The receiver has to decode the record format and item packing, so it uses a client library.

### 5.3 Wake

A `webhook.notify` sink posts `{sink_id, head, unread}` at most about once a second. The function reads with `GET /read?from=<cursor>&filter=…`, handles the batch, and moves its cursor with `PUT /cursor/<name>`. It loops until it reaches `head`, since a wake that arrives while it runs may be coalesced.

### 5.4 Queue

A `forward` sink with the consumer's filter copies matching events into `bus.<domain>.q.<consumer>`. Workers then share that stream by either:

- **claims** on the queue's state doc (AGENT-COORDINATION.md section 4), when any worker can take any event; or
- **partitions:** worker *i* of *N* subscribes with `eq` over the low partition bits. Events for one key always go to one worker, in order. A lease per partition says which worker owns it.

The queue has its own retention and its own backlog, so a slow consumer never holds the bus back, and the queue's `unread` is that consumer's lag.

Forward drops the source record's id (PATTERNS gap 6). The envelope's `id` and `author` identify the event on both sides.

### 5.5 Archive

An `archive` sink writes matching records to S3 or R2 as objects named by first id. It keeps events past the stream's retention for analytics, read back with a client library.

### 5.6 State view

A domain summary in the state doc, updated atomically with events, read by `STATE_ONLY` subscribers. They get the current doc at subscribe and each change, and never the events.

## 6. Failed events

A consumer that can't process an event after its own retries publishes it to `bus.<domain>.dlq.<consumer>` with the original item plus `{"err": "...", "src_id": <record id>, "tries": n}`, then commits past it. Options are copied from the original, with the app-defined bit 31 set as "dead-lettered".

Push consumers (5.2) can't do that from inside the sink: a batch that keeps failing pauses the sink. So a push receiver accepts the batch (2xx) and dead-letters the bad event itself.

A `webhook.notify` sink on a DLQ stream is how an operator or an investigating agent is woken (AGENT-HARNESS.md).

## 7. Operations

- **Lag per consumer:** `head` minus the named cursor for 5.1 and 5.3; the sink's `unread` for 5.2; the queue stream's unread for 5.4.
- **Adding a consumer** never touches producers: a new cursor name, or a new sink.
- **Adding a tenant:** create its ingress stream, then its forward sink, then issue its token.
- **Retention:** the bus class is how long a consumer may be down and still catch up. A sink warns before unread data expires and follows `on_expire: stop | skip`.

## 8. What this needs from Sillystream

Works today. Rough edges, all already listed in PATTERNS section 10:

- a sink can't be created before its stream exists, so setup order matters (gap 8);
- no HTTP `GET` for a named cursor, so lag over HTTP uses the `PUT` with 0 trick (gap 4);
- forward drops the source id (gap 6);
- the item packing a push receiver must decode isn't in SPEC yet (gap 7; being written down).

## 9. Open questions

1. **Ingress streams per tenant, or per producer service?** Per tenant gives isolation; per service gives fewer streams and sinks.
2. **Partition count:** 8 bits (256) is assumed. Enough?
3. **Where the type registry lives** and whether a small tool should generate filter constants from it for each client language.
4. **A "not" filter** (for example "everything except test") doesn't exist. Is the class field's one-hot workaround enough, or is this worth raising for the spec?
5. **DLQ replay:** a tool that republishes DLQ items to the queue stream, or a manual step?
