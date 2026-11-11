# Logs on Sillystream

Status: design agreed with the user on 2026-11-09, not built. It sits on top of `docs/SPEC.md` and changes nothing in it or in the server. Everything here is client-side: the runtime still never looks at record data.

`silly-logs` is a new binary, separate from `silly`, run as a DaemonSet on each Kubernetes node. It reads container log files, journald, OTLP over HTTP and syslog, and publishes them into namespace streams. The first version gets logs out again in two ways: `archive` sinks to S3 or R2, and `silly-logs cat` to read those archives back. Exports to Loki or Elastic come later, as client-side workers (section 8).

## 1. Decisions

| question | decision |
|---|---|
| agent shape | new binary `silly-logs`, as a DaemonSet, plus a small token issuer |
| streams | one per namespace (`logs.ns.<ns>`), one for node logs (`logs.node`), and one state-only stream per agent (`logs.agent.<node>`) |
| restarts | at-least-once: a crash can duplicate lines but never lose them; each line carries its file offset, so a reader can drop exact duplicates |
| author | one token per node, so `author = hash(node)` |
| levels | one-hot bits in `options` |
| item encoding | one JSON object per item, with records zstd-compressed by the SDK |
| retention | the operator sets each stream's class with `silly` |
| inputs, v1 | container log files, journald, OTLP logs over HTTP, syslog |
| outputs, v1 | `archive` sinks and `silly-logs cat` |

## 2. Streams

| stream | written by | holds |
|---|---|---|
| `logs.ns.<namespace>` | the agent on every node that runs a pod of that namespace, and OTLP | container stdout and stderr, OTLP logs |
| `logs.node` | every agent | journald and syslog |
| `logs.agent.<node>` | that node's agent | state only: file offsets and the journald cursor |

Namespace names are DNS labels, so they are always valid stream ids. Access in Sillystream is per stream (SPEC section 12), so a token for `logs.ns.payments` reads that namespace and nothing else. Retention is per stream too, so classes follow namespaces.

The agent writes into the namespace streams directly, without forwards. A cross-namespace feed is a fan-in: a `forward` sink from each namespace stream into an aggregate stream (SPEC section 9.1), set up by the operator when it's wanted. It isn't part of v1.

## 3. Records and items

**One item is one log line.** The agent batches lines into records with `count > 1`. It uses `sdk::build_records` with zstd level 3 and the default 1 MiB `record_target_bytes`, so each record's data is the SDK's item table plus the item bytes, compressed together (codec 1).

**Item:** a UTF-8 JSON object. Keys are short because every line repeats them, although zstd removes most of that cost.

```json
{"t":1762650000123456789,"node":"n1","pod":"api-7d9f8c-x2kq","c":"api","off":81920,"msg":"GET /health 200"}
```

| key | meaning | present for |
|---|---|---|
| `t` | event time in ns since the epoch: the CRI timestamp, journald's `__REALTIME_TIMESTAMP`, OTLP `time_unix_nano`, or the syslog timestamp. Ingest time is the record id. | all |
| `node` | node name | all |
| `pod`, `c` | pod and container name | container files, and OTLP when its resource has them |
| `off` | byte offset of the line's start in its file | container files |
| `unit` | systemd unit | journald |
| `app` | syslog APP-NAME, or OTLP `service.name` | syslog, OTLP |
| `msg` | the line, with no trailing newline; the joined line for CRI partial lines | all |
| `attrs` | OTLP attributes and other structured fields, as a JSON object | OTLP, syslog structured data, journald fields kept by config |

**Ordering:** the SDK sorts a publish's items stably by options (SPEC section 3), so within one flush a container's warn lines can get ids before its earlier info lines. `t` gives the real order. Lines with the same options keep their order.

**Duplicates:** `(node, pod, c, off)` identifies a container line, so a reader can drop exact duplicates after an agent crash (section 5.3). Other inputs don't dedupe.

**No dictionaries in v1.** A dictionary pays off for small records, and an agent that lingers up to a second per file mostly makes records of tens to hundreds of KB, where plain zstd already does well. Adding dictionaries later means a dictionary id prefix inside the data (SPEC section 3) and a new codec value, which is a spec change to raise first.

## 4. Options

| bits | field | values |
|---|---|---|
| 56–63 | level, one-hot | 56 trace, 57 debug, 58 info, 59 warn, 60 error, 61 fatal, 62 unknown, 63 reserved |
| 48–55 | source, one-hot | 48 stdout, 49 stderr, 50 journald, 51 OTLP, 52 syslog, 53–55 reserved |
| 24–47 | workload hash, 24 bits | see below |
| 8–23 | container hash, 16 bits | container name, otherwise 0 |
| 0–7 | reserved | 0 |

Exactly one level bit and one source bit is set on every record. The rest of a line's details stay inside the data, so a record groups everything one input produced at one level for one workload.

**Workload:** a stable name for what produced the line:

- container files: the pod name with its controller suffix removed (`api-7d9f8c-x2kq` → `api`, `db-0` → `db`), found from the pod name alone, with no API server;
- OTLP: `service.name`, otherwise the same rule on `k8s.pod.name`;
- journald: the systemd unit;
- syslog: APP-NAME.

**Hashes:** the low bits of the first 8 bytes (little-endian) of unkeyed BLAKE3 over the UTF-8 name. They aren't the stream key hash. Anyone can compute them from a name, so a filter is built from names. A collision only widens what a filter matches, and a reader that needs to be exact checks `pod`, `c` or `app` in the item.

**Filters** (SPEC section 4), written as `silly tail` takes them:

| want | filter |
|---|---|
| warn or worse | `any:0x3800000000000000` |
| errors and fatals | `any:0x3000000000000000` |
| one workload, all levels | `eq:0x0000FFFFFF000000:<hash << 24>` |
| one workload's errors | `eq:0x1000FFFFFF000000:<0x1000000000000000 \| hash << 24>` |
| stderr only | `any:0x0002000000000000` |
| one node | `author:<hash(node)>` |

One-hot levels are what make "warn or worse" a single `any` clause. Block summaries skip well too: a block with no error-level record has bit 60 clear in its `options_or`.

**Level detection,** in order, with the first match winning:

1. OTLP `severity_number` (1–4 trace, 5–8 debug, 9–12 info, 13–16 warn, 17–20 error, 21–24 fatal), and syslog severity (7 debug, 6–5 info, 4 warn, 3 error, 2–0 fatal), and journald `PRIORITY` (the same mapping as syslog).
2. A JSON line with a `level`, `lvl`, `severity` or `log.level` field.
3. A logfmt `level=` pair.
4. A leading level word (`ERROR`, `WARN`, `INFO`, …, either case, optionally in brackets), or a klog prefix (`E1109`, `W1109`, `I1109`).
5. Otherwise `unknown`. Stderr is not taken to mean error.

## 5. The agent

`silly-logs agent` runs one per node and uses the Rust SDK's `Client` and `Publisher`, at ack level 2.

### 5.1 Container log files

- **Discovery:** it watches `/var/log/pods/<namespace>_<pod>_<uid>/<container>/<n>.log` with inotify and a rescan every 10 s. The path gives namespace, pod and container, so the agent needs no Kubernetes API access.
- **Parsing:** CRI lines are `<RFC3339Nano time> <stdout|stderr> <P|F> <text>`. `P` parts are joined until the `F`, with the joined line capped at 256 KiB and anything past that cut off.
- **Producers:** each file is a producer: `producer_id = hash(node, path, inode)` with the top bit cleared (it's reserved for forwards, SPEC section 9.1), and `producer_seq` = the end offset of the batch's last line in that file. Offsets only grow within one file, so the seq always increases. A rotated file is a new inode and so a new producer.
- **Flushing:** a file's pending lines are published when they reach 1 MiB or the oldest has waited 1 s. A publish holds one file's lines, so its records hold one container's lines at each level.
- **Rotation:** kubelet renames `0.log` and starts a new one. The agent finishes the old inode before it lets go of it. If the agent falls behind by more than kubelet keeps, the lost bytes are counted and logged, because they're gone from disk.
- **Backpressure:** when publishing backs up, the agent stops reading. Unread lines stay in the files.

### 5.2 Node and pushed inputs

| input | goes to | producer | resumes from |
|---|---|---|---|
| journald (`/var/log/journal`, `/run/log/journal`) | `logs.node` | one per node, with a seq the same as OTLP's (not journald's clock, which can go back and would make new lines look like old ones) | the journald cursor in `logs.agent.<node>`, saved like file offsets, so a crash can repeat lines |
| OTLP logs over HTTP, protobuf and JSON, on host port 4318 | `logs.ns.<k8s.namespace.name>`, or `logs.node` without one | one per (node, stream), seq = a counter that starts at the current ms << 16, so it keeps increasing across restarts | nothing: the sender retries |
| syslog RFC 5424 and 3164, UDP and TCP, on host port 514 | `logs.node` | one per node, with a seq the same as OTLP's | nothing |

An OTLP request gets 200 only after its publish is acked at level 2, and 503 with `Retry-After` while publishing is backed up, so the backpressure reaches the sender. UDP syslog is lossy, as it always is.

### 5.3 Offsets and restarts

The agent keeps how far it has got in the state of `logs.agent.<node>` (SPEC section 10):

```json
{"files": {"<pod uid>/<container>/<n>.log": {"ino": 1234, "off": 81920}}, "journald": "<cursor>"}
```

- After a publish is acked, its file's entry moves to the batch's end. Changes are merged into the state at most once a second, in one `merge`. A file's entry is removed when the file is gone.
- On start, the agent reads the state and resumes each file whose inode still matches.
- **A crash between an ack and the merge only duplicates.** The agent rereads from the saved offset, which is older. A batch that ends at or below what the stream took has a seq the stream has already seen, so it is dropped and nothing is lost (SPEC section 3). A batch that ends past it is new, and it repeats the lines between the saved offset and the old end. `off` lets a reader drop those.
- **Size:** about 80 bytes per file, so about 800 live log files fit in 64 KB, well past a node's usual pod count. If the state would pass 48 KB, the agent logs a warning and saves only the entries that changed most recently.
- **Dedupe entries:** a namespace stream keeps at most 1,000 producers' entries (SPEC section 3), and a large namespace can have more live files than that. An evicted entry costs only duplicates on a retry, never loss.

### 5.4 Identity and tokens

`author = hash(node)` with 0 remapped to 1, so every record says which node ingested it, and `author:N` filters a node's logs. Producers can't set it (SPEC section 2).

Tokens come from `silly-logs issuer`, a small Deployment that holds the token secret:

1. The agent sends its projected ServiceAccount token, with audience `sillystream`, to the issuer.
2. The issuer checks it with a TokenReview and takes the node name from the bound token's node claim (Kubernetes 1.30 and later).
3. It returns a token with that author, valid for 1 hour, granting `logs.ns.*:publish`, `logs.node:publish` and `logs.agent.<node>:read,state`.
4. The agent renews it at half its life with `Client::set_token`.

No node holds the token secret, and a node can't publish as another node. The agent has no `admin`, so it can't change retention or sinks.

## 6. Retention

Streams are created on first publish with the default `1d` class (SPEC section 7.4). The operator sets a longer one with `silly`, before or after the stream exists:

```
silly stream create logs.ns.payments --retention 30d
silly stream config logs.ns.audit --retention 365d
```

| class | suggested for |
|---|---|
| `1d` | dev namespaces, `logs.node` on short-lived clusters |
| `7d` | most namespaces |
| `30d` | production namespaces |
| `365d` | audit, security |

`logs.agent.<node>` holds only state, which outlives retention, so its class doesn't matter. Raising a class affects new data only. Older data keeps the class it was written under (SPEC section 7.4).

## 7. Getting logs out, v1

**Live:** `silly tail logs.ns.payments --filter any:0x3800000000000000` prints one line per item: id, options, author, then the item's bytes, which here is the JSON.

**Archive:** an `archive` sink per stream that should outlive its class, or be kept in a cheaper bucket:

```
silly sink create logs.ns.payments archive --kind archive --start earliest
```

The sink writes objects of whole records as stored (zstd, the SDK item table), at `archive/<stream key hash>/<sink id>/<first id, 20 digits>.records`, each up to the sink's byte budget or 60 s old. The server keeps no stream names, so the path names the stream by its key hash.

**`silly-logs cat`** reads archives back:

```
silly-logs cat --store s3:log-archive --stream logs.ns.payments --stream-key-file stream-key --sink archive \
    --from 2026-11-09T10:00Z --to 2026-11-09T11:00Z [--filter ..] [--dedupe]
```

- It lists the sink's objects, keeps those whose first id (the ingest ms is in its top 48 bits) can overlap the range, and prints matching items as JSON lines.
- `--filter` takes the same clauses as `silly tail` and is checked on each record's header before decompressing it.
- `--dedupe` drops repeats of `(node, pod, c, off)`.
- The stream hash is computed from `--stream NAME` with `--stream-key-file`, the deployment's key (SPEC section 13), or passed as `--stream-hash`. Nothing on the server reports it today. Needing the key to find an archive is the price of keeping names out of the bucket.

## 8. Later

Not in v1. Each one is client-side or operator setup, and none needs a server change:

- **Exporters:** `silly-logs export --to loki|elastic` subscribes to a stream with a named cursor, decodes items, sends them to Loki's push API or Elastic's `_bulk`, and commits after a 2xx, which gives at-least-once delivery. `webhook.deliver` can't do this, because it sends records as stored and the server never transforms data (SPEC section 9).
- **Fan-in:** a `forward` from each namespace stream into an aggregate stream, for one feed across namespaces.
- **Retention from namespace annotations,** read by a controller with an `admin` token.
- **Dictionaries:** section 3.

## 9. Deployment sketch

```
                     ┌───────────────── node ─────────────────┐
 pods ─▶ /var/log/pods ─┐                                      │
 journald ──────────────┼─▶ silly-logs agent ──WS, ack 2──┐    │
 apps ─OTLP :4318───────┤      ▲ token (1 h)              │    │
 syslog :514 ───────────┘      │                          │    │
                     └─────────┼──────────────────────────┼────┘
                               │                          ▼
               silly-logs issuer (Deployment)    sillystream-server
               TokenReview, token secret          logs.ns.*  logs.node  logs.agent.*
                                                          │ archive sinks
                                                          ▼
                                                       S3 / R2
```

**DaemonSet:**

- Mounts: `/var/log/pods` read-only, `/var/log/journal` and `/run/log/journal` read-only, and the CA bundle for the server.
- Host ports: 4318 (OTLP HTTP) and 514 (syslog, TCP and UDP). Pods reach the node's agent at `status.hostIP` through the downward API.
- Its ServiceAccount token is projected with audience `sillystream` and a 1-hour expiry. The pod needs no RBAC.
- `NODE_NAME` comes from `spec.nodeName`. It names the agent's stream and is checked by the issuer against the token's node claim.
- Starting resources: 100m CPU and 128 MiB, limited at 1 CPU and 512 MiB. To be benchmarked.

**Issuer:**

- One or two replicas, with a ClusterRole for `tokenreviews.create` only.
- Mounts the token secret.

**Operator setup:**

```
silly stream create logs.ns.payments --retention 30d
silly stream create logs.node --retention 7d
silly sink create logs.ns.payments archive --kind archive --start earliest
```

## 10. Open questions

- **Kubelet rotation limits:** an agent that falls behind loses rotated-away bytes (section 5.1). Should `containerLogMaxSize` and `containerLogMaxFiles` guidance go in the deployment notes?
- **Workload names without the API:** stripping suffixes from pod names gets Deployments, ReplicaSets, StatefulSets and DaemonSets right, but not bare pods or Jobs with custom names. Is that good enough, or should the agent read owner references from the kubelet's `/pods` endpoint?
- **A namespace's first publish** creates its stream at `1d`, before the operator may have set a class. Is that acceptable, or should the issuer, which could hold `admin`, create streams at a configured class?
