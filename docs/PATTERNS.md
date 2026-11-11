# Sillystream patterns

How to build agent coordination, event buses and log aggregation out of Sillystream's few parts: streams, `options`, filters, named cursors, the 64 KB state doc, ack levels and sinks. `docs/SPEC.md` is the source of truth. This file only shows how the parts combine, and adds no mechanism.

Every command and snippet here was run against a local `sillystream-server` built from `spec-rewrite`, first at `e71b3d9`. The Rust examples were run again at `9cebc93`. Section 10 lists the gaps that came up while writing it.

## 0. Running the examples

### A local server

```bash
mkdir -p ~/silly-demo && cd ~/silly-demo
# A leaf certificate: rustls refuses a CA certificate used as the server's own.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout key.pem -out cert.pem -days 30 -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost,IP:127.0.0.1 -addext basicConstraints=critical,CA:FALSE
openssl rand -hex 32 > secret       # signs tokens
openssl rand -hex 32 > stream-key   # the deployment's stream identity key; never changes
```

```bash
target/release/sillystream-server --listen 127.0.0.1:7443 \
  --wal-dir ~/silly-demo/wal --data-dir ~/silly-demo/data \
  --cert ~/silly-demo/cert.pem --key ~/silly-demo/key.pem \
  --token-secret-file ~/silly-demo/secret --stream-key-file ~/silly-demo/stream-key \
  --object-store local:$HOME/silly-demo/objects --sink-archive-store local:$HOME/silly-demo/archive \
  --sink-allow-private-addresses true --seal-window-ms 2000
```

`--sink-allow-private-addresses true` lets sinks post to `127.0.0.1`. Leave it off in production.

### Tokens and environment

The token issuer picks the author, so give every agent, host or service its own author id and grant it only the streams it needs:

```bash
cd ~/silly-demo
silly token issue --secret-file secret --author 1 --grant '*:read,publish,state,admin' --ttl-ms 86400000 > admin.tok
silly token issue --secret-file secret --author 101 --grant 'team.*:read,publish,state' --ttl-ms 86400000 > a101.tok
silly token issue --secret-file secret --author 102 --grant 'team.*:read,publish,state' --ttl-ms 86400000 > a102.tok
export SILLYSTREAM_URL=localhost:7443 SILLYSTREAM_CA=$HOME/silly-demo/cert.pem SILLYSTREAM_TOKEN=$(cat admin.tok)
```

`silly --token-file a101.tok …` acts as author 101.

### Shell helpers for the HTTP routes

The state routes speak CBOR. These helpers turn JSON into CBOR and back (`pip install cbor2`):

```bash
j2c() { python3 -c 'import sys,json,cbor2; sys.stdout.buffer.write(cbor2.dumps(json.load(sys.stdin)))'; }
c2j() { python3 -c 'import sys,json,cbor2; print(json.dumps(cbor2.loads(sys.stdin.buffer.read() or b"\xf6")))'; }
st()  { curl -s --cacert "$SILLYSTREAM_CA" -H "Authorization: Bearer $SILLYSTREAM_TOKEN" "$@"; }
API=https://localhost:7443/v1/stream
```

### Rust snippets

Each full Rust snippet below is a compiled example in [`crates/sdk/examples/`](../crates/sdk/examples), and the code shown is that file without its header. Build them all and put them on `PATH`, so that the shell lines below run as written:

```bash
cargo build -p sdk --examples
export PATH=$PWD/target/debug/examples:$PATH
```

They share these helpers (`crates/sdk/examples/common/mod.rs`):

```rust
use ciborium::Value;
use sdk::http::HttpClient;
use sdk::{Client, ClientConfig};

pub fn env() -> (String, Vec<u8>, String) {
    let url = std::env::var("SILLYSTREAM_URL").expect("SILLYSTREAM_URL");
    let ca = std::fs::read(std::env::var("SILLYSTREAM_CA").expect("SILLYSTREAM_CA")).unwrap();
    let token = std::env::var("SILLYSTREAM_TOKEN").expect("SILLYSTREAM_TOKEN");
    (url, ca, token)
}

pub async fn connect() -> (Client, HttpClient) {
    let (url, ca, token) = env();
    let client = Client::connect(ClientConfig::new(&url, &ca, &token))
        .await
        .unwrap();
    (client, HttpClient::new(&url, &ca, &token).unwrap())
}

/// `GET /state`: the doc, its version and the stream's head, in one consistent step.
pub async fn state_get(http: &HttpClient, stream: &str) -> (Value, u64, u64) {
    let reply = http
        .request("GET", &format!("/v1/stream/{stream}/state"), &[], b"")
        .await
        .unwrap();
    let number = |name| reply.header(name).and_then(|v| v.parse().ok()).unwrap_or(0);
    // A stream that doesn't exist yet is a 404: no state, version 0.
    let doc = if reply.status == 404 || reply.body.is_empty() {
        Value::Map(vec![])
    } else {
        ciborium::from_reader(&reply.body[..]).unwrap()
    };
    (doc, number("x-state-ver"), number("x-head"))
}

pub fn to_cbor(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).unwrap();
    out
}

/// `doc[key]` for a text key.
pub fn get<'a>(doc: &'a Value, key: &str) -> Option<&'a Value> {
    doc.as_map()?
        .iter()
        .find(|(k, _)| k.as_text() == Some(key))
        .map(|(_, v)| v)
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
```

## 1. Where each fact goes

A record has four places to put information, and each one does a different job:

| put it in | when it is | because |
|---|---|---|
| **stream id** | who may see it, or a high-cardinality partition (a tenant, an agent, a host, a session) | access is per stream (a token grants id patterns such as `team.*`), state is per stream, and a stream is the unit of ordering and placement |
| **`options`** | low-cardinality facts you filter on: kind, type, level, service, region, flags | the server filters on it without opening data, block summaries skip whole blocks, and records group on the exact value |
| **`author`** | who wrote it | the token sets it and producers can't forge it, so it's the field to trust and filter by (`author:N`) |
| **data** | everything else: ids, timestamps, payloads, per-item details | opaque to the server, and compressed together within a group |

Rules of thumb:

- **Exact values group.** Records group on an exact `options` value, so every distinct value in a publish is at least one record. Ten options values in a batch of 1,000 lines give about ten zstd groups. A request id or a timestamp in options makes 1,000 groups of one, and the compression is gone.
- **Aim for a few dozen distinct values per busy stream.** Stay well under the batch size.
- **Never put anything you must keep secret in options.** Options are the one part of a record the server reads.
- **Order inside one publish follows options.** The SDK sorts a publish stably by options, so a `warn` line can get an earlier id than an `error` line that was added before it. If order across kinds matters, publish them separately or keep the order in the data.
- **Ids are time, roughly.** An id's top 48 bits are ingest ms. After a restart, new ids start up to 70 s ahead of the clock (`MAX_ID_LEAD_MS`, D221), so take an id as an order and an approximate time. Put the exact event time in the data.

### 1.1 Layout recipes

A layout is a convention between your producers and consumers, and the server knows nothing about it. Put the field you filter on most at the top. Make a field that people ask "any of" questions about one-hot, and give a field that people ask "exactly" questions about a value.

**Agent coordination** (`team.<id>`):

```
bits 56–63  kind         0x01 task, 0x02 message, 0x03 result, 0x04 handoff, 0x05 heartbeat
bits 40–55  skills       one-hot: bit 40 rust, 41 python, 42 docs, …
bit  39     urgent
bits 2–38   app-defined
bits 0–1    partition    for static partitions (section 7.4)
```

| what | options |
|---|---|
| a task needing rust | `0x0100010000000000` |
| an urgent python task | `0x0100028000000000` |
| a result | `0x0300000000000000` |

**Event bus** (`bus.<domain>`), a hierarchical type so that a prefix is a filter:

```
bits 56–63  domain       0x01 order, 0x02 payment, 0x03 shipping
bits 48–55  event        within its domain: order 0x01 created, 0x02 paid, 0x03 cancelled
bits 44–47  schema version
bits 32–43  source service
bits 0–31   app-defined
```

`order.created` is `0x0101000000000000`, and `order.paid` is `0x0102000000000000`.

**Logs** (`logs.<service>.<host>`), the layout from SPEC 2.1:

```
bit  63     error flag
bits 56–62  level        1 debug, 2 info, 3 warn, 4 error, 5 fatal
bits 40–55  service
bits 24–39  region
bits 0–23   app-defined
```

`info` from service 1 in region 1 is `0x0200010001000000`, and an `error` is `0x8400010001000000`.

If the question is usually "this level *or worse*", encode the level **thermometer-style** instead: bit 56 is debug or worse, bit 57 info or worse, bit 58 warn or worse, bit 59 error or worse. Then "warn or worse from service 1" is a single `eq` over both fields (section 1.2).

### 1.2 Filters

A filter is up to 4 clauses OR-ed together, optionally AND-ed with an author:

- `eq:MASK:VALUE` matches when `(options & MASK) == VALUE`. **One `eq` can AND several fields**: put all of them in the mask.
- `any:MASK` matches when `options & MASK != 0`. It suits one-hot fields and flags.
- `author:N` matches records written by that author, and is AND-ed with the options clauses.

There is no AND between two clauses, so a question that needs "field A is x AND some bit of B is set" has to fit into one `eq`, or into at most 4 OR-ed `eq`s. Design the layout so that your common questions fit.

These filters ran against the records above:

```bash
# All tasks
silly tail team.t1 --from start --filter eq:0xff00000000000000:0x0100000000000000
# Tasks I can do: rust OR python, each as kind AND skill in one eq
silly tail team.t1 --from start --filter eq:0xff00010000000000:0x0100010000000000,eq:0xff00020000000000:0x0100020000000000
# Anything urgent, whatever its kind
silly tail team.t1 --from start --filter any:0x0000008000000000
# Results written by agent 101
silly tail team.t1 --from start --filter eq:0xff00000000000000:0x0300000000000000,author:101

# Logs: every error
silly tail logs.all --from start --filter any:0x8000000000000000
# Logs: one service
silly tail logs.all --from start --filter eq:0x00ffff0000000000:0x0000020000000000
# Logs: warn OR error, from service 1 (two eqs)
silly tail logs.api.host9 --from start --filter eq:0x7fffff0000000000:0x0300010000000000,eq:0x80ffff0000000000:0x8000010000000000
# Thermometer layout: warn or worse, from service 1 (one eq)
silly tail logs.thermo --from start --filter eq:0x04ffff0000000000:0x0400010000000000

# Event bus: every order event, or just order.paid
silly tail bus.orders --from start --filter eq:0xff00000000000000:0x0100000000000000
silly tail bus.orders --from start --filter eq:0xffff000000000000:0x0102000000000000
```

A time range is an id range: `--from ID --to ID`. Over HTTP, `GET /v1/stream/<id>/read?from=&limit=&filter=` takes the same filter text.

## 2. Readers and named cursors

- **Client-held cursor.** The client remembers the last id it processed, and the server keeps nothing. This suits a process that keeps its own checkpoint, such as one in its own database.
- **Named cursor.** The server keeps `(stream, name) → id`. It moves forward only through `COMMIT`, which gives at-least-once delivery. Name it after the consumer (`worker-a`, `billing`, `indexer`). Two processes that share a name share one cursor and both get every record: this is not a consumer group.

```bash
silly tail team.t1 --name worker-a --from start --limit 1   # task 1
silly tail team.t1 --name worker-a --from start --limit 1   # task 2: the stored cursor wins over --from
st -X PUT --data 0 "$API/team.t1/cursor/worker-a"             # never rewinds: answers {"id": <where it is>}
st -X PUT --data 0 "$API/team.t1/cursor/worker-a?reset=true"  # rewinds to the start
```

The durable consumer loop commits after the work, and commits `PROGRESS` too, so that a rare filter doesn't rescan from its old position after a reconnect:

```rust
// consumer STREAM CURSOR [eq:MASK:VALUE]
use sdk::{Event, FROM_START, Subscribe, WireClause};
use std::time::Duration;

fn hex(text: &str) -> u64 {
    u64::from_str_radix(text.trim_start_matches("0x"), 16).unwrap()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (client, _) = common::connect().await;
    // `from` counts only the first time: once the server holds the cursor, it starts there.
    let mut spec = Subscribe::new(&args[0], FROM_START);
    spec.name = Some(args[1].clone());
    if let Some(filter) = args.get(2) {
        let parts: Vec<&str> = filter.split(':').collect();
        spec.clauses.push(WireClause {
            mask: hex(parts[1]),
            value: hex(parts[2]),
            op: 0,
        });
    }
    let mut sub = client.subscribe(spec).unwrap();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
        match event.unwrap() {
            Event::Items(items) => {
                for item in &items {
                    println!("{} {}", item.id, String::from_utf8_lossy(&item.bytes)); // the work
                }
                // After the work: a crash before this line means these come again.
                sub.commit(items.last().unwrap().id).unwrap();
            }
            // Nothing matches up to `id`, so the cursor can move there too.
            Event::Progress(id) => sub.commit(id).unwrap(),
            Event::Behind { resume_from } => eprintln!("catching up from {resume_from}"),
            _ => {}
        }
    }
    // A commit is one-way: give it a moment to leave before the process ends.
    drop(sub);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
```

Running it twice prints the 8 results the first time and nothing the second. A result published in between is the only line of the third run.

**Consumers without a connection** (a cron job, a serverless function) can use the same named cursor over HTTP. The cursor PUT doubles as a read, because it never moves the cursor back:

```bash
POS=$(st -X PUT --data 0 "$API/team.t1/cursor/lambda-1" | jq -r .id)
st -D headers.txt -o batch.bin "$API/team.t1/read?from=$POS&limit=100&filter=eq:0xff00000000000000:0x0100000000000000"
# … process batch.bin (the binary records format) …
st -X PUT --data "$(grep -i x-through headers.txt | tr -dc 0-9)" "$API/team.t1/cursor/lambda-1"
```

`X-Through` is how far the read looked, filtered-out records included, so committing it skips them for good. `X-Count` is how many items came back.

## 3. The state doc

One CBOR doc per stream, at most 64 KB, with a version that is the id of the last change. Use it for how things stand now: plans, owners, leases, counters, room metadata. The log keeps what happened. Growing lists, blobs and anything updated many times a second don't belong in it.

Its shape follows from merge patches:

- Use **maps with text keys**, never arrays, for anything you add to or remove from one entry at a time. A merge patch replaces an array whole.
- A `null` in a merge deletes that key.
- Keep the doc bounded. Remove finished entries rather than flagging them.

```bash
U=$API/room.alpha/state
echo '{"topic":"launch","members":{"a1":"planner"}}' | j2c | st -X PATCH -H 'X-State-Op: set' --data-binary @- $U
echo '{"members":{"a2":"coder"},"topic":null}' | j2c | st -X PATCH --data-binary @- $U   # merge is the default
echo '{"path":["turns"],"n":1}' | j2c | st -X PATCH -H 'X-State-Op: incr' --data-binary @- $U
st -D h.txt $U | c2j      # {"members": {"a1": "planner", "a2": "coder"}, "turns": 1}
grep -i 'x-state-ver\|x-head' h.txt
```

### 3.1 Compare-and-set

Any op can carry `expect_ver`: `If-Match` over HTTP, or `StateChange::expect_ver` in the SDK. A mismatch is a version conflict, which is HTTP 409 or `SdkError::VersionConflict`, and nothing changes.

```bash
V=$(grep -i x-state-ver h.txt | tr -dc 0-9)
echo '{"owner":"a1"}' | j2c | st -X PATCH -H "If-Match: \"$V\"" --data-binary @- $U   # {"ok":true,"ver":"…"}
echo '{"owner":"a2"}' | j2c | st -X PATCH -H "If-Match: \"$V\"" --data-binary @- $U   # 409, version conflict
```

> **The first claim.** `expect_ver = 0` means *unconditional*, and a stream whose state was never set has version 0. To make the first change conditional too, send `expect_ver = EXPECT_UNSET` (`u64::MAX`), or `If-None-Match: *` over HTTP: it succeeds only while the state was never set, so of several workers racing on a new queue exactly one wins.

### 3.2 Atomic changes

A publish can carry one state change. Its records and the change commit together with consecutive ids, or neither does. The ack's `count` includes the change, so the new version is `first_id + count - 1`. It works only within one stream, because the state and the records belong to the same stream.

### 3.3 Snapshot + read, and watching the doc

- `GET /state` returns the doc, `X-State-Ver` and `X-Head` in one step, and `SUBSCRIBE` with `WITH_STATE` returns them in `SUBSCRIBED`. Read records from `head + 1` and you miss nothing and see nothing twice.
- `WITH_STATE` then pushes the whole doc on each later change, as a `STATE` event in id order among the records: after the items below its version, before any above. One subscription gives the doc and the log, and a catch-up from history gets the changes as `STATE` only from the head on. `STATE_ONLY` is the same with no records, for watching just the doc. `INCLUDE_SYSTEM` also sends each change as a system record, if the operation itself matters.

The core of it, from `crates/sdk/examples/watch_state.rs`:

```rust
// watch_state STREAM: a state_only subscription gets the doc now and each new one.
let mut spec = Subscribe::new(&stream, FROM_LIVE);
spec.state_only = true;
let mut sub = client.subscribe(spec).unwrap();
while let Some(event) = sub.next().await {
    let (ver, doc) = match event.unwrap() {
        Event::Subscribed { state_ver, state, .. } => (state_ver, state),
        Event::State { ver, doc } => (ver, doc),
        _ => continue,
    };
    println!("v{ver}: {:?}", ciborium::from_reader::<ciborium::Value, _>(&doc[..]).unwrap());
}
```

## 4. Ack levels

| level | what it survives | use it for |
|---|---|---|
| 1 | everything but a process crash | metrics, high-volume logs, presence and heartbeats |
| 2 (default) | a process crash, and machine loss when `wal_dir` is on EBS | tasks, results, handoffs, business events |
| 3 | segment in S3 | audit and billing events, anything that must survive losing the AZ |

Measured over the simple HTTP form on a laptop (local disk, 2 s seal window, connection setup included): level 1 took 4 ms, level 2 16 ms, and level 3 170 ms. Section 7.2 of the spec has the expected ranges.

- **Set a floor per stream.** With `silly stream config audit.billing --min-ack 3`, a careless producer can't ask for less: a level 1 request then waits for the floor.
- **Delivery doesn't wait for durability.** A record is delivered once it is in the WAL. On a stream that uses level 1 only, a subscriber can see a record that a crash then erases. So consumers of a level 1 stream must not treat what they saw as committed.
- **For idempotent retries without the SDK**, use the simple HTTP form's producer headers. A retry of the latest publish gets the original ack, and an older one gets `first_id = 0`:

```bash
curl -s --cacert "$SILLYSTREAM_CA" -H "Authorization: Bearer $SILLYSTREAM_TOKEN" \
  -H 'X-Options: 0x0100000000000000' -H 'X-Producer-Id: 7' -H 'X-Producer-Seq: 42' \
  --data-binary '{"task":"t1"}' "$API/tasks.demo/publish?ack=2"
```

Sent twice, it gave the same `first_id` both times.

## 5. Sinks

A sink is a filter plus a destination, with its own named cursor `sink:<id>`. **Sink ids are u64 numbers**, and **the source stream must exist** before a sink can be created on it. Create it with `silly stream create` or a first publish.

### 5.1 `webhook.notify`: wake someone up

```bash
silly sink create team.t1 1 --kind webhook.notify --sink-url http://127.0.0.1:8099/wake \
  --filter eq:0xff00000000000000:0x0100000000000000
```

It sends `{"sink_id":"1","head":"117583273568239616","unread":1}`, at most about once a second. It carries no records, so the woken process reads them itself, with its own named cursor (section 2). Use it for agents that sleep: a serverless function, a laptop agent, a human's phone.

### 5.2 `webhook.deliver`: push records to a service

```bash
silly sink create team.t1 2 --kind webhook.deliver --sink-url http://127.0.0.1:8099/results \
  --secret s3cret --filter eq:0xff00000000000000:0x0300000000000000 --start earliest --compression zstd
```

Each POST is `application/x-sillystream-records` with `X-Sink-Id`, `X-First-Id`, `X-Last-Id`, `X-Signature` (lowercase hex HMAC-SHA256 of the body as sent) and, with compression, `Content-Encoding: zstd|gzip`. A receiver does three things:

1. Checks the signature **before** decompressing.
2. Splits the records and opens each record's items. The packing of items inside a record is the SDK's, not the server's: zstd if `codec = 1`, and with `count > 1`, `count` u32 end offsets come before the item bytes.
3. Drops every item at or below the last id it took from that sink, because a resend after a crash starts at the same first id.

This receiver was checked against both encodings:

```python
import gzip, hashlib, hmac, struct
from compression import zstd  # Python 3.14+; or the zstandard package

def records(body):
    at = 0
    while at < len(body):
        rid, options, author, length, count, codec = struct.unpack_from("<QQQIHH", body, at)
        yield rid, options, author, count, codec, body[at + 32 : at + 32 + length]
        at += 32 + (length + 7) // 8 * 8

def items(count, codec, data):
    if codec == 1:
        data = zstd.decompress(data)
    if count == 1:
        return [data]
    ends = struct.unpack_from(f"<{count}I", data)
    base, start, out = 4 * count, 0, []
    for end in ends:
        out.append(data[base + start : base + end])
        start = end
    return out

def handle(headers, body, secret, last_taken):
    want = hmac.new(secret, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(headers["X-Signature"], want):
        return 401
    if headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    elif headers.get("Content-Encoding") == "zstd":
        body = zstd.decompress(body)
    sink = headers["X-Sink-Id"]
    for rid, options, author, count, codec, data in records(body):
        for i, item in enumerate(items(count, codec, data)):
            if rid + i > last_taken.get(sink, 0):
                process(rid + i, options, author, item)
    last_taken[sink] = max(last_taken.get(sink, 0), int(headers["X-Last-Id"]))
    return 200
```

Answer over HTTP/1.1 keep-alive with a `Content-Length`, and the sink reuses the connection for its next POST. A batch grows while the one before it is in flight: in the test, a sink that started `earliest` sent 2 records in its first POST and the next 4 together in its second.

`silly sink reset STREAM ID --start earliest` replays from the start, and a receiver that drops ids at or below its last taken id drops that replay too. Clear its memory when you reset on purpose.

### 5.3 `forward`: fan-in and routing

```bash
silly stream create logs.all --retention 7d
silly stream create logs.worker.host2
silly sink create logs.api.host1    1 --kind forward --target logs.all --start earliest
silly sink create logs.worker.host2 1 --kind forward --target logs.all --start earliest
```

- **No wildcards:** each source needs its own sink, so create the sink when you create the stream.
- **What survives:** the original author and options. **What changes:** the id, since the target stamps its own, so a reader of `logs.all` can't tell the source id.
- **Duplicates:** a forward dedupes through normal idempotency, so the target can hold duplicates only after a dedupe entry is gone (SPEC 3).
- **Routing:** a forward sink with a filter routes. Section 7.4 uses that for static partitions.

### 5.4 `archive`: keep records past retention

```bash
silly sink create logs.all 9 --kind archive --start earliest
```

The records, exactly as stored, go to the node's `--sink-archive-store` as `archive/<stream key hash>/<sink id>/<first id, 20 digits>.records`. An object is written once it reaches the byte budget, or when its oldest record has waited 60 s. Read them back with `records()` from section 5.2. The stream appears only as its keyed hash, so you need the deployment's stream key to find a stream's prefix (section 10).

## 6. Agent coordination: setup

Streams:

- `team.<id>`: the shared bus for the team. It holds tasks, messages, results and handoffs, typed by `kind` (layout in section 1.1).
- `inbox.<agent>`: messages to one agent. Grant `inbox.<agent>:read` to that agent, and `inbox.*:publish` to the agents that may write to it.
- `team.<id>.lease.<role>`, `team.<id>.room`: one stream per lease or room, because each has one state doc.

Each agent's token carries its author id, so `author:N` answers "what did agent N say" and nobody can impersonate N.

## 7. Agent coordination recipes

### 7.1 Leader lease, with the version as fencing token

The lease is `{"holder", "until"}` in a stream's state. To acquire it, CAS it when it's free, expired or already yours. Every write the holder makes also renews the lease, **in the same publish and conditional on the current version**. Once someone else has taken over, the version has moved, and the old holder's write commits nothing. That's real fencing, done by the server, but only for writes to the lease's own stream. To fence writes elsewhere, carry the version along as a token and have the receiver refuse an older one.

```rust
// lease STREAM HOLDER TTL_MS STEPS STEP_MS
use ciborium::{Value, cbor};
use common::*;
use sdk::{EXPECT_UNSET, PublisherConfig, SdkError, StateChange, StateOp};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (stream, me) = (args[0].as_str(), args[1].as_str());
    let ttl: u64 = args[2].parse().unwrap();
    let (steps, step_ms): (u32, u64) = (args[3].parse().unwrap(), args[4].parse().unwrap());
    let (client, http) = connect().await;

    // 1. Acquire: take the lease if it's free, expired or already ours, by CAS on its version.
    let mut fence = loop {
        let (doc, ver, _) = state_get(&http, stream).await;
        let holder = get(&doc, "holder").and_then(Value::as_text).unwrap_or("");
        let until = get(&doc, "until")
            .and_then(Value::as_integer)
            .map_or(0, |n| u64::try_from(n).unwrap());
        if holder.is_empty() || holder == me || until < now_ms() {
            let lease = cbor!({ "holder" => me, "until" => now_ms() + ttl }).unwrap();
            let change = StateChange {
                // A never-set state is version 0, which EXPECT_UNSET guards.
                expect_ver: if ver == 0 { EXPECT_UNSET } else { ver },
                op: StateOp::Set,
                doc: to_cbor(&lease),
            };
            match client.state_op(stream, change).await {
                Ok(new_ver) => break new_ver,
                Err(SdkError::VersionConflict { .. }) => continue, // someone else moved first
                Err(error) => panic!("{error}"),
            }
        }
        println!("{me}: {holder} holds it until {until}; waiting");
        tokio::time::sleep(Duration::from_millis(until.saturating_sub(now_ms()) + 10)).await;
    };
    println!("{me}: acquired, fence {fence}");

    // 2. Work: every write renews the lease in the same publish, conditional on the fence.
    let mut publisher = client
        .publisher(stream, PublisherConfig::default())
        .unwrap();
    for step in 0..steps {
        tokio::time::sleep(Duration::from_millis(step_ms)).await;
        publisher.add(0, format!("{me} step {step}"));
        let renew = cbor!({ "until" => now_ms() + ttl }).unwrap();
        let change = StateChange {
            expect_ver: fence,
            op: StateOp::Merge,
            doc: to_cbor(&renew),
        };
        match publisher
            .send_with_state(change)
            .await
            .unwrap()
            .remove(0)
            .wait()
            .await
        {
            // The state change is the publish's last id, and that id is the new version.
            Ok(ack) => fence = ack.first_id + u64::from(ack.count) - 1,
            Err(SdkError::VersionConflict { .. }) => {
                println!("{me}: fenced out at step {step}; nothing of it committed");
                return;
            }
            Err(error) => panic!("{error}"),
        }
        println!("{me}: step {step} committed, fence {fence}");
    }

    // 3. Release, still conditional, so a late release can't free a successor's lease.
    let free = cbor!({ "holder" => "", "until" => 0 }).unwrap();
    let change = StateChange {
        expect_ver: fence,
        op: StateOp::Set,
        doc: to_cbor(&free),
    };
    println!(
        "{me}: released {:?}",
        client.state_op(stream, change).await.map(|_| ())
    );
}
```

Run it with a 1 s TTL. Agent 101's steps are 1.5 s apart, so it stalls past its own lease:

```bash
echo '{"holder":"","until":0}' | j2c | st -X PATCH -H 'X-State-Op: set' --data-binary @- $API/team.lease.planner/state
SILLYSTREAM_TOKEN=$(cat a101.tok) lease team.lease.planner agent-101 1000 3 1500 &
SILLYSTREAM_TOKEN=$(cat a102.tok) lease team.lease.planner agent-102 1000 2 200
```

```
agent-101: acquired, fence 117583289984614400
agent-102: agent-101 holds it until 1794178620150; waiting
agent-102: acquired, fence 117583290051526656
agent-102: step 0 committed, fence 117583290065223681
agent-102: step 1 committed, fence 117583290079510529
agent-102: released Ok(())
agent-101: fenced out at step 0; nothing of it committed
```

The stream then holds only agent 102's two steps.

### 7.2 Work queue A: competing workers with state-CAS claims

The queue is one stream. Tasks are records, and the state doc is a **shared claim cursor plus a lease per task in progress**:

```json
{"next": 117583294532878355, "leases": {"117583294532878356": {"by": "w2", "until": 1794178700000}}}
```

Every worker reads every task. To claim one, a worker moves `next` to that task's id and adds its lease, in one CAS. When it's done, the result and the removal of the lease commit atomically, and only while the lease is still the worker's. The doc stays small: one entry per task in flight, about 60 bytes each, so roughly a thousand in flight.

```rust
// queue STREAM WORKER TTL_MS WORK_MS
use ciborium::{Value, cbor};
use common::*;
use sdk::{
    EXPECT_UNSET, Event, PublisherConfig, SdkError, StateChange, StateOp, Subscribe, WireClause,
};
use std::time::Duration;

const KIND: u64 = 0xff << 56;
const TASK: u64 = 0x01 << 56;
const RESULT: u64 = 0x03 << 56;

fn int(doc: &Value, key: &str) -> u64 {
    get(doc, key)
        .and_then(Value::as_integer)
        .map_or(0, |n| u64::try_from(n).unwrap())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (stream, me) = (args[0].as_str(), args[1].as_str());
    let (ttl, work_ms): (u64, u64) = (args[2].parse().unwrap(), args[3].parse().unwrap());
    let (client, http) = connect().await;
    let mut results = client
        .publisher(stream, PublisherConfig::default())
        .unwrap();

    // Read tasks from the shared claim cursor on. The CAS below decides who runs each one.
    let (doc, _, _) = state_get(&http, stream).await;
    let mut spec = Subscribe::new(stream, int(&doc, "next"));
    spec.clauses.push(WireClause {
        mask: KIND,
        value: TASK,
        op: 0,
    });
    let mut tasks = client.subscribe(spec).unwrap();

    while let Some(event) = tasks.next().await {
        let Event::Items(items) = event.unwrap() else {
            continue;
        };
        for task in items {
            let key = task.id.to_string();
            // Claim: move "next" to this task and add its lease, if no one has moved it past.
            let claimed = loop {
                let (doc, ver, _) = state_get(&http, stream).await;
                if int(&doc, "next") >= task.id {
                    break false; // another worker claimed it
                }
                let claim = cbor!({
                    "next" => task.id,
                    "leases" => { key.as_str() => { "by" => me, "until" => now_ms() + ttl } },
                })
                .unwrap();
                let change = StateChange {
                    // A never-set state is version 0, which EXPECT_UNSET guards.
                    expect_ver: if ver == 0 { EXPECT_UNSET } else { ver },
                    op: StateOp::Merge,
                    doc: to_cbor(&claim),
                };
                match client.state_op(stream, change).await {
                    Ok(_) => break true,
                    Err(SdkError::VersionConflict { .. }) => continue,
                    Err(error) => panic!("{error}"),
                }
            };
            if !claimed {
                continue;
            }
            tokio::time::sleep(Duration::from_millis(work_ms)).await; // the work
            // Done: the result and the lease's removal commit together, while the lease is ours.
            loop {
                let (doc, ver, _) = state_get(&http, stream).await;
                let lease = get(&doc, "leases").and_then(|leases| get(leases, &key));
                if lease.and_then(|l| get(l, "by")).and_then(Value::as_text) != Some(me) {
                    println!("{me}: lost the lease on {key}; dropping the result");
                    break;
                }
                results.add(
                    RESULT,
                    format!("{me} did {}", String::from_utf8_lossy(&task.bytes)),
                );
                let done = cbor!({ "leases" => { key.as_str() => null } }).unwrap();
                let change = StateChange {
                    expect_ver: ver,
                    op: StateOp::Merge,
                    doc: to_cbor(&done),
                };
                match results
                    .send_with_state(change)
                    .await
                    .unwrap()
                    .remove(0)
                    .wait()
                    .await
                {
                    Ok(_) => break,
                    Err(SdkError::VersionConflict { .. }) => continue, // another claim moved it
                    Err(error) => panic!("{error}"),
                }
            }
        }
    }
}
```

```bash
echo '{"next":0,"leases":{}}' | j2c | st -X PATCH -H 'X-State-Op: set' --data-binary @- $API/team.jobs/state
seq 1 20 | sed 's/^/job-/' | silly publish team.jobs --options 0x0100000000000000 --no-compress
for w in w1 w2 w3; do queue team.jobs $w 5000 50 & done; wait
```

Three workers finished 7, 6 and 7 jobs. Each of the 20 jobs got exactly one result, and the doc ended as `{"next": …, "leases": {}}`.

What's left to you:

- **Expired leases.** A crashed worker leaves its lease behind. A sweeper (any worker, now and then) finds leases whose `until` has passed, CASes `by` to itself, re-reads the task by id (`--from ID-1 --to ID`) and runs it.
- **Contention.** Every claim is one CAS on one doc. That's fine for agent-scale queues of tens of claims a second. For much more, use B.

### 7.3 Work queue B: a dispatcher and inboxes

One reader, the dispatcher, holds a named cursor on the queue and republishes each task to `inbox.<worker>`. Each worker reads its own inbox with its own named cursor (section 2). Workers share no state, so there is no contention. The cost is one more hop, and a dispatcher that has to know which workers exist.

The dispatcher should publish with a fixed `producer_id`, so that a restart which re-reads tasks doesn't duplicate them, and commit its cursor after the acks. Workers should also skip a task id they have already done, because delivery is at-least-once.

### 7.4 Work queue B without code: static partitions through forward sinks

When the set of workers is fixed, forward sinks can do the dispatching. The producer puts a partition in the bottom bits (here `hash(task) % 2` in bits 0–1), and one filtered forward sink per partition fills each inbox:

```bash
silly stream create jobs.p
for p in 0 1; do
  silly stream create inbox.w$p
  silly sink create jobs.p $((10+p)) --kind forward --target inbox.w$p --filter eq:0x3:$p --start earliest
done
for i in 1 2 3 4 5; do silly publish jobs.p --options $((0x0100000000000000 + i % 2)) "job-$i"; done
silly tail inbox.w0 --name w0 --from start    # job-2, job-4
silly tail inbox.w1 --name w1 --from start    # job-1, job-3, job-5
```

This is a partitioned group composed from parts, which SPEC 15 keeps out of the server. Adding a worker means a new partition, its sink, and producers that know about it.

**Picking one:**

- Use **A** when the workers come and go, a task may take a long time, and the rate is modest.
- Use **B / 7.4** for a higher rate, a stable set of workers, and order kept within each worker.

### 7.5 Rooms and handoffs

A room is a stream whose state says how things stand (`owner`, `phase`, `goal`) and whose log is the conversation. To join, an agent takes the snapshot and head in one step and reads on from there. To hand off, the owner publishes the handoff message and the owner change atomically, conditional on the version it read. If anything moved in between, the handoff commits nothing.

```rust
// room STREAM ME [handoff NEXT]
use ciborium::{Value, cbor};
use common::*;
use sdk::{Event, FROM_LIVE, PublisherConfig, SdkError, StateChange, StateOp, Subscribe};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (stream, me) = (args[0].as_str(), args[1].as_str());
    let (client, _) = connect().await;

    // 1. Join: SUBSCRIBED carries the state and `head`, and the records follow from head + 1.
    let mut spec = Subscribe::new(stream, FROM_LIVE);
    spec.with_state = true;
    let mut sub = client.subscribe(spec).unwrap();
    let (ver, doc) = loop {
        if let Some(Ok(Event::Subscribed {
            head,
            state_ver,
            state,
        })) = sub.next().await
        {
            let doc: Value = ciborium::from_reader(&state[..]).unwrap();
            println!("{me}: joined at head {head}, state v{state_ver}: {doc:?}");
            break (state_ver, doc);
        }
    };

    // 2. Hand off: only the owner does, and only from the version it saw.
    if args.get(2).map(String::as_str) == Some("handoff") {
        let next = args[3].as_str();
        assert_eq!(
            get(&doc, "owner").and_then(Value::as_text),
            Some(me),
            "only the owner hands off"
        );
        let mut publisher = client
            .publisher(stream, PublisherConfig::default())
            .unwrap();
        publisher.add(
            0x04 << 56,
            format!("{me} -> {next}: parser done, please review"),
        );
        let change = cbor!({ "owner" => next, "phase" => "review" }).unwrap();
        let change = StateChange {
            expect_ver: ver,
            op: StateOp::Merge,
            doc: to_cbor(&change),
        };
        match publisher
            .send_with_state(change)
            .await
            .unwrap()
            .remove(0)
            .wait()
            .await
        {
            Ok(ack) => println!("{me}: handed off at {}", ack.first_id),
            Err(SdkError::VersionConflict { ver }) => println!("{me}: the room moved on (v{ver})"),
            Err(error) => panic!("{error}"),
        }
    }

    // 3. Follow the room.
    while let Some(event) = sub.next().await {
        if let Event::Items(items) = event.unwrap() {
            for item in items {
                println!(
                    "{me}: {} {}",
                    item.author,
                    String::from_utf8_lossy(&item.bytes)
                );
            }
        }
    }
}
```

```bash
echo '{"owner":"agent-101","phase":"build","goal":"ship the parser"}' | j2c | st -X PATCH -H 'X-State-Op: set' --data-binary @- $API/team.room1/state
SILLYSTREAM_TOKEN=$(cat a102.tok) room team.room1 agent-102 &
SILLYSTREAM_TOKEN=$(cat a101.tok) room team.room1 agent-101 handoff agent-102
```

Both agents see `101 agent-101 -> agent-102: parser done, please review`, and the doc is now `owner: agent-102, phase: review`. Run the same handoff from agent 101 again and it stops, because it no longer owns the room. A follower that also wants the doc's later versions adds a `STATE_ONLY` subscription (section 3.3).

## 8. Event bus

- **Per-tenant streams for producers.** A producer writes to `bus.orders.<tenant>` with a token for that stream alone, which the server enforces: a token for `bus.orders.acme` gets error 1 on `bus.orders.globex`.
- **One aggregate for internal consumers:** a forward sink from each tenant stream into `bus.orders`. The original author survives, so `author:N` still tells tenants apart.
- **Typed events in options**, hierarchical so that a prefix is a filter (layout in section 1.1). Payloads, ids and event time go in the data.
- **Consumers are either:**
  - services that hold a connection: a named cursor and a filter (section 2);
  - services behind HTTP: a `webhook.deliver` sink with a filter (section 5.2);
  - sleeping agents: `webhook.notify`, then a read with their own named cursor.

```bash
silly stream create bus.orders
for t in acme globex; do
  silly stream create bus.orders.$t
  silly sink create bus.orders.$t 1 --kind forward --target bus.orders
done
# billing only wants order.paid
silly sink create bus.orders 20 --kind webhook.deliver --sink-url http://127.0.0.1:8099/billing \
  --secret s3cret --filter eq:0xffff000000000000:0x0102000000000000 --compression zstd
silly --token-file acme.tok publish bus.orders.acme --options 0x0101000000000000 '{"order":"A-1","total":40}'
silly --token-file acme.tok publish bus.orders.acme --options 0x0102000000000000 '{"order":"A-1","paid":40}'
silly publish bus.orders.globex --options 0x0102000000000000 '{"order":"G-9","paid":12}'
```

Billing received the two `order.paid` events, signed, and not `order.created`. A schema change bumps bits 44–47, and an old consumer keeps working by filtering on the version it knows.

## 9. Log aggregation

- **One stream per host or process:** `logs.<service>.<host>`, written with a token whose author is that host. Fan it in with a forward sink to `logs.all`, or to `logs.<region>` first if one aggregate would be too hot. One stream can't ingest more than one core can handle (SPEC 6.1).
- **Options from the layout in section 1.1**, and nothing else. A request id, latency or user id goes in the message.
- **Batch hard.** One publish holds many lines, the SDK groups them by options and compresses each group with zstd, and level 1 acks keep the cost low.
- **Retention and archive:** keep `logs.all` at `7d` or `30d`, and add an `archive` sink for anything kept longer.

```rust
// logship STREAM SERVICE REGION, with lines `LEVEL message` on stdin
use sdk::{AckLevel, PublisherConfig};
use std::io::BufRead;

const ERROR_FLAG: u64 = 1 << 63;

fn options(level: u64, service: u64, region: u64) -> u64 {
    let flag = if level >= 4 { ERROR_FLAG } else { 0 };
    flag | level << 56 | service << 40 | region << 24
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (service, region): (u64, u64) = (args[1].parse().unwrap(), args[2].parse().unwrap());
    let (client, _) = common::connect().await;
    // Level 1: acked from memory. A process crash can lose the last few ms of logs.
    let config = PublisherConfig {
        ack: AckLevel::Memory,
        ..PublisherConfig::default()
    };
    let mut publisher = client.publisher(&args[0], config).unwrap();
    let mut lines = std::io::stdin().lock().lines();
    loop {
        // One publish per 1,000 lines here; a real shipper also flushes on a timer.
        for line in lines.by_ref().take(1000) {
            let line = line.unwrap();
            let (level, message) = line.split_once(' ').unwrap();
            let level = match level {
                "debug" => 1,
                "info" => 2,
                "warn" => 3,
                "error" => 4,
                _ => 5,
            };
            publisher.add(options(level, service, region), message.to_owned());
        }
        if publisher.pending_count() == 0 {
            break;
        }
        for ack in publisher.flush().await.unwrap() {
            println!("first_id={} items={}", ack.first_id, ack.count);
        }
    }
}
```

```bash
silly token issue --secret-file secret --author 5009 --grant 'logs.api.host9:publish' --ttl-ms 3600000 > host9.tok
silly stream create logs.api.host9
silly sink create logs.api.host9 1 --kind forward --target logs.all
silly tail logs.all --filter any:0x8000000000000000 &   # live errors, from every host
(for i in $(seq 1 3000); do echo "info GET /item/$i 200 $((i%50))ms"; done; echo "error POST /pay 500 db timeout") \
  | SILLYSTREAM_TOKEN=$(cat host9.tok) logship logs.api.host9 1 1
```

The 3,001 lines went out as 4 publishes, and the live tail on `logs.all` printed only the error, with author 5009.

## 10. Gaps found while writing this

None of these is changed here. Each needs a decision first.

**Spec and server**

1. **No conditional create.** Fixed: `expect_ver = u64::MAX` (`EXPECT_UNSET`, or `If-None-Match: *`) succeeds only if the state was never set (SPEC section 10).
2. **`WITH_STATE` versus `STATE_ONLY` is undocumented.** Fixed: both push a `STATE` frame for each change after the snapshot, `WITH_STATE` in id order among the records and `STATE_ONLY` with no records (SPEC 11.1).
3. **Atomic publish + state is per stream.** That's by design, but it means fencing (7.1) and claims (7.2) work only when the results go to the same stream as the state. Writes to another stream can only carry the version as a token.
4. **There's no way to read a named cursor over HTTP.** `PUT /cursor/<name>` with `0` works as a read only because it never rewinds. A `GET` would say what it does.
5. **Archive objects can't be found by stream name.** The path holds the stream's keyed hash, and no route returns it, so finding a stream's archive needs the stream key and BLAKE3 outside the server.
6. **Forward drops the source id.** Only the author and options survive, so a reader of an aggregate can't point back to the source record unless the producer puts a reference in the data.
7. **The item packing inside a record isn't in the spec.** SPEC 2 calls it "client-owned item offsets", and the Rust SDK uses u32 end offsets. Every `webhook.deliver` receiver and every non-Rust client has to match it, so it's a de facto format. It matters to the client-libs work.
8. **A sink can't be created before its source stream exists** (404), even though streams are otherwise created on first use.
9. **Sink ids are u64**, which the CLI's usage text doesn't say. Names like `billing` fail with "a sink id is a u64".

**CLI and SDK**

10. `silly` has no `state get|set|merge|incr`, and the HTTP state routes take CBOR only, so the shell examples above need a Python helper.
11. `silly tail` has no `--include-system` or state flags, and `silly publish` has no `--producer-id` / `--seq`. The simple HTTP form covers the latter.
12. The SDK's `Client` has no way to read state except through a subscription. These snippets use `GET /state` through `sdk::http::HttpClient`.
13. A `Publisher` numbers its seqs itself from `first_seq`. So a dispatcher (7.3) can't use "seq = source id" for idempotent re-publishing without creating a publisher per record.
