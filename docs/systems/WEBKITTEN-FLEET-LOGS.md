# Webkitten fleet logs on Sillystream

Status: draft for review, 2026-11-12, not built. It sits on top of `docs/SPEC.md` and adds nothing to the server. The webkitten facts come from reading `/Users/s/work/patrol/webkitten` on 2026-11-12, mainly `docs/design/FLEET-UPDATE-AND-OBSERVABILITY.md` (a design draft, itself not built) and `host-app/WebkittenHost/ARCHITECTURE-M2.md` section 5.4. Nothing here was agreed with the webkitten threads; section 11 lists what they would need to confirm.

`docs/LOGS.md` covers Kubernetes logs. This is a different source with the same idea: structured, low-volume, closed-vocabulary events from Mac minis.

## 1. What it is

Each Mac mini runs one webkitten host with 32–64 browser sessions that record suspicious sites. The host already has an operational log (JSON lines with a closed set of names and no site strings), an event ring, and a result per visit. This system:

1. publishes those to Sillystream, one stream per mini;
2. forwards what went wrong (failed recordings and failed systems) into one alert stream, using filters on `options` only;
3. wakes investigating agents when alerts arrive (`docs/systems/AGENT-HARNESS.md`);
4. keeps the agents' findings in a stream of their own.

It is not the metrics path. The per-minute rollups and the collector in the webkitten observability draft (its section 4.3, option c) stay as they are; section 9 notes where the two could meet.

## 2. Rules carried over from webkitten

These are the fleet's own rules, and this design keeps them:

- **Nothing on disk.** The host keeps what it hasn't sent in a memory ring and drops the oldest when full. No spool file.
- **No listening port on a mini.** The host connects out, over WSS.
- **Never through a session's proxy or WebKit's network process.** The publisher uses the Mac's own network, like the Depot client. Pages can't see it.
- **No URL, host name, domain or IP address, anywhere:** not in options, not in data. The operational log already has no site strings by design.
- **Closed vocabularies.** Names, kinds, reasons and codes come from the host's enums.

One thing is new and needs the owner's yes (section 11, question 1): **recording and task ids inside record data.** The observability draft keeps them out of metrics and in the DODO result only. An agent investigating a failed recording needs them to find the task and the recording. This design puts them in the data, encrypted with the client envelope (CLIENTS.md 2.4), and never in options.

## 3. Streams

| stream | written by | holds | state doc | retention |
|---|---|---|---|---|
| `wk.mini.<mini>` | that mini's host | everything the mini reports: run results, events, log lines | the mini's status (section 6) | `7d` |
| `wk.alerts` | one `forward` sink per mini | only what went wrong (section 5) | nothing | `30d` |
| `wk.incidents` | investigating agents | findings and notes | open incidents and who holds each | `365d` |

`<mini>` is the mini's DODO worker id, which the observability draft already uses as the `mini` label.

**Tokens:** each mini gets a token granting `publish` and `state` on `wk.mini.<its id>` only, with `author = hash(worker id)`. So a mini can't write another's stream, and `author` on `wk.alerts` says which mini a record came from even after the forward. Agents get `read` on `wk.*` and `publish` and `state` on `wk.incidents`. Tokens are short-lived (SPEC section 12), so the mini's token is rotated like the DODO queue token: a file re-read at each connect.

**Volume (guess, from the draft's one visit per slot per minute):** about 64 run records a minute per mini plus a few events, well under 1 MB an hour per mini. At 100 minis the whole fleet is a trickle for one node.

## 4. Records

### 4.1 Options

One layout for all three streams:

```
bits 56–63  level, one-hot    58 info, 59 warn, 60 error, 61 fatal   (as docs/LOGS.md)
bits 48–55  kind, one-hot     48 run, 49 process, 50 host, 51 memory,
                              52 queue, 53 depot, 54 dns, 55 updater
bits 40–47  run outcome,      40 ok, 41 failed retryable, 42 failed permanent,
            one-hot           43 silent (lease lost), 44 upload failed,
                              45 recording incomplete or unsealed
bits 24–39  code              a number from the code registry (section 4.3)
bits 16–23  channel, one-hot  16 canary, 17 stable
bits 0–15   reserved          0
```

Exactly one level bit and one kind bit on every record. Outcome bits are set only on `run` records, and more than one can be set (an upload failure is also retryable).

**Levels for runs:** `ok` is info; a retryable failure is warn; a permanent failure, an upload failure, an incomplete or unsealed recording and a silent run are error. That rule is what lets one filter mean "anything bad".

| want | filter |
|---|---|
| anything bad | `any:0x3000000000000000` (error or fatal) |
| failed recordings | `any:0x0000300000000000` (upload failed, or incomplete or unsealed) |
| every failed run, retryable included | `any:0x00003e0000000000` |
| system failures, no runs | `eq:0x1001000000000000:0x1000000000000000,eq:0x2001000000000000:0x2000000000000000` (error, or fatal, with the run bit clear) |
| page-process crashes | `eq:0x0002000000000000:0x0002000000000000` |
| one failure code | `eq:0x000000ffff000000:<code << 24>` |
| one mini | `author:<hash(worker id)>`, or read `wk.mini.<mini>` |
| canary only | `any:0x0000000000010000` |

Records group on the exact options value. With closed lists that is a few dozen values per mini, so batches still compress.

### 4.2 Data

One JSON object per item, encrypted (section 2). Common fields:

| key | meaning |
|---|---|
| `t` | event time, ms |
| `name` | the code as a string: `depot_upload_failed`, `process.crashed`, `host_hold`, … |
| `rel` | host build, WebKit patch series tip and recorder layout, as the host's hello reports them |
| `slot` | session slot number, when it applies |

By kind:

| kind | source in the host | extra fields |
|---|---|---|
| run | `run.finished` plus the upload facts (`RecorderBridge.swift`) | `task`, `recording`, `attempt`, `code`, `retryable`, `ms`, `ms_seal_to_staged`, `stream_bytes`, `http_status`, `failure`, `peak_mib` |
| process | `process.crashed` | `process_kind` (webcontent, network, gpu), `reason` |
| host | operational log names such as `host_hold`, `host_failure`, `previous_instance_died`, `main_thread_stalled`, `event_dropped` | the line's own fields |
| memory | `memory.limit`, `memory.admission` | limit kind, footprint |
| queue | `queue.abort`, `queue.report` not accepted, reconnects | reason |
| depot | health not ok | the Depot's failure field |
| dns | listener down, errors by route | route (`direct` or a country) |
| updater | release staged, switched, rejected, rolled back | release ids |

Successful runs are published too (info). They are the baseline an agent compares failures against, and they cost little.

### 4.3 Code registry

`code` is a 16-bit number per name, kept in one file in the webkitten repository and generated into the host and into the agents' tools. Run failure codes come from `TaskFailureCode` (for example `preflight_dns`, `open_failed`, `depot_upload_failed`, `recording_incomplete`, `detonated_unsealed`); the others from the operational log's names and the event kinds. A name never changes its number.

## 5. From a mini to the alert stream

**Publishing.** The host's Rust side (beside `webkitten-dodo`, using the Rust SDK) keeps one connection to Sillystream and publishes with ack level 2, flushing every second or at 256 KB. One `producer_id` per host start, with an increasing seq, so a retry after a reconnect isn't duplicated. If Sillystream is unreachable, records wait in a memory ring of a fixed size; when it overflows, the oldest are dropped and a `dropped` counter is reported in the next status (section 6). Delivery from a mini is therefore at-most-once across a long outage and at-least-once otherwise.

**Forwarding.** Each `wk.mini.<mini>` has one `forward` sink into `wk.alerts`, with the filter "anything bad, or any failed run":

```
any:0x30003e0000000000
```

The sink is created when a mini is enrolled. The forward keeps each record's author and options, so every filter of section 4.1 works on `wk.alerts` unchanged. Forwarding is at-least-once; a reader dedupes on `(author, t, name, slot)` if it must.

`wk.alerts` is what agents read. They go back to `wk.mini.<mini>` for context: what else that mini reported around that time, including successes.

## 6. Mini status, and silent minis

Each mini's stream has a state doc, merged once a minute and at every change of release or hold:

```
{"seen_ms": …, "rel": {…}, "channel": "stable", "state": "ready|draining|hold|probation",
 "slots": {"configured": 64, "usable": 63, "quarantined": 1}, "dropped": 0}
```

A stream can't report its own silence. So a watcher (a timer in the harness, section 7) reads each mini's state every minute and publishes a `host`-kind fatal record named `mini_silent` to `wk.alerts` when `seen_ms` is more than 3 minutes old, and `mini_back` when it returns. That is the same threshold as the draft's "Silent mini" alert.

A state write is a system record in the stream, so once a minute per mini adds about 1,440 small records a day each. That is acceptable here; it would not be for a per-second gauge.

## 7. Waking agents

Two `webhook.notify` sinks on `wk.alerts`, each posting `{sink_id, head, unread}` to the harness, at most about once a second:

| sink | filter | wakes |
|---|---|---|
| 1 | failed recordings and failed runs: `any:0x00003e0000000000` | the recording investigator |
| 2 | system failures: the two-clause filter of section 4.1 | the fleet investigator |

A wake carries no records. The woken harness reads `wk.alerts` with its own named cursor and the same filter, then decides what deserves an investigation. Not every alert does:

| alert | becomes an incident when |
|---|---|
| permanent run failure, incomplete or unsealed recording | at once, grouped by `(code, rel)` |
| retryable run failure | 5 or more with the same `(code, rel)` in 10 minutes, or over 2 % of that mini's runs in an hour |
| upload failure | over 1 % across the fleet in 15 minutes, or any mini with 3 in a row |
| page-process crash | over 2 % of visits in an hour on a release, memory kills excluded |
| network or GPU process death | more than 2 in an hour on one mini |
| host restart, hold, `main_thread_stalled` | at once, per mini |
| `mini_silent` | at once |

The starting numbers are the alert values in the observability draft, section 4.6. This triage is plain code in the harness, not a model call.

## 8. Incidents and findings

`wk.incidents` uses the claims pattern of AGENT-COORDINATION.md section 4:

- **State:** `{"open": {"<incident key>": {"by": <agent>, "until_ms": …, "first_id": <alert id>, "count": n}}}`. The key is the grouping of section 7, such as `run:depot_upload_failed:<rel>` or `mini:<worker id>:hold`. An agent claims a key with a conditional merge, so two harness instances never investigate the same thing, and new alerts for an open key only raise its count.
- **Records:** one per finding: `{"key", "summary", "evidence": [record ids and stream names], "cause", "confidence", "suggested": [...], "alerts": [first id, last id]}`. Options reuse the layout: the incident's kind and code, level by severity.
- A `webhook.notify` or `webhook.deliver` sink on `wk.incidents` is how people hear about findings (a chat hook, a pager for fatal).

What an investigating agent can look at:

| source | how |
|---|---|
| the alert and its neighbours | `wk.alerts` and `wk.mini.<mini>`, by id range and filter |
| a mini's status and release | the state doc of `wk.mini.<mini>` |
| the same code on other minis or releases | `wk.alerts` with the code filter |
| the task and its attempts | DODO, by `task` id (outside Sillystream) |
| the recording | the Depot, by `recording` id (outside Sillystream) |

Agents only read. Nothing here lets an agent act on a mini; see question 4.

## 9. Relation to the metrics plan

The observability draft pushes per-minute rollups to a collector the owner hasn't picked yet. Two ways the plans could meet, neither assumed here:

- the rollup could be one more record kind on `wk.mini.<mini>`, with an `archive` sink for long-term keeping, which would make Sillystream the draft's "heartbeat JSON" option without building storage for it;
- or the two stay separate, and agents read the collector as one more source.

## 10. What this needs

**From Sillystream:** nothing new. It uses streams, filters, `forward`, `webhook.notify`, state and tokens as specified. It needs a server the minis can reach over the internet with TLS, and a token issuer for mini tokens.

**From webkitten** (new work in that repository):

- a publisher in the host's Rust side, with the memory ring and the status merge;
- the code registry file and its generated constants;
- a mapping from the operational log, the event ring and run results to records, with a test that no record holds a URL, host name or IP (the draft already plans that test for metrics);
- enrolment: create the stream and its forward sink, and deliver the token through the MDM secrets path.

## 11. Questions for the owner and the webkitten threads

1. **Task and recording ids in record data, encrypted:** allowed? Without them an agent can see that recordings fail and why by code, but can't open one.
2. **Who holds the decryption key:** the harness only, or also people's tools?
3. **Are successful runs published?** They double as the baseline and as a visit log; leaving them out cuts volume by about 95 % but makes rates impossible to compute from the stream alone.
4. **May agents act?** For example drain a mini, quarantine a slot, halt a rollout. That needs a command path to a host with no listening port (a stream the host subscribes to), and is a separate design.
5. **Operational log lines at info level:** all of them, or warn and above only?
6. **Thresholds in section 7:** taken from the draft's alert table; who tunes them?
7. **One alert stream, or one per channel** (canary apart from stable)?
