# A stream-driven agent harness on pi-durable

Status: draft for review, 2026-11-12, not built. It sits on top of `docs/SPEC.md` and adds nothing to the server.

What I know of pi-durable comes from the README of `@earendil-works/pi-durable` (`github.com/earendil-works/pi`, `packages/durable`), read on 2026-11-12 through a summary, not from its source. The package calls itself experimental and says its API may change between releases. Every pi-durable name below (`Harness`, `submit`, `requestId`, `defineTool`, `defineSignal`, `sleepUntil`, `onWake`, `resume`) is from that README and should be checked against the version we pin before building.

## 1. What it is

A small host process, called `silly-agent` here, that:

1. consumes one or more Sillystream streams, each with a filter and a named cursor;
2. is **woken** by new records, either on a live subscription or by a `webhook.notify` sink while it sleeps;
3. turns records of chosen types into **investigations**: pi-durable conversations that a model drives with tools;
4. writes each investigation's result back to a stream.

Sillystream holds the events and the cursors. pi-durable holds the agent's work: transcript, model turns and tool calls are written to storage before they show, so a crash resumes where it stopped. The harness is the thin piece between them, and its one hard job is to move an event from the first to the second **exactly once in effect**.

The first user is the webkitten fleet (`docs/systems/WEBKITTEN-FLEET-LOGS.md`). It is equally the "sleeping agent" of AGENT-COORDINATION.md section 8 and the "wake" consumer of EVENT-BUS.md section 5.3.

## 2. Shape

```
Sillystream                          silly-agent                         pi-durable
-----------                          -----------                         ----------
wk.alerts ──subscription or──▶  source ─▶ triage ─▶ router ──submit──▶  conversation per incident
            notify + read          │         (code)      │                 model turns, tools
                                   │                     └──signal─────▶  a parked task
named cursor ◀──commit─────────────┘ (after the submit is durable)
wk.incidents ◀──publish finding + release claim──────────────────────── tool: report
```

One process, one pi-durable storage (the README says one process should write a storage at a time). It runs in one of two hosts:

| host | storage | wake | when |
|---|---|---|---|
| Node service | SQLite (`openNodeSqliteStorage`) | holds WebSocket subscriptions; timers in process | the default: always on, simplest |
| Cloudflare Durable Object | Durable Object SQLite | `webhook.notify` posts to the object; `onWake` arms the object's alarm | when it should cost nothing while idle |

Both need the JavaScript client from `docs/CLIENTS.md` section 4.2, which isn't built yet. The Durable Object host needs only its HTTP half (read, cursor, state, publish).

## 3. Configuration

One file. Everything a deployment changes is here; no code.

```yaml
server: wss://silly.example
token_file: /run/secrets/silly-agent.tok
storage: sqlite:/var/lib/silly-agent/agent.db

sources:
  - name: recordings
    stream: wk.alerts
    filter: any:0x00003e0000000000          # failed runs and recordings
    cursor: agent.recordings
    mode: subscribe                          # or: notify (sink id 1)
    route: recording-failures
  - name: systems
    stream: wk.alerts
    filter: eq:0x1001000000000000:0x1000000000000000,eq:0x2001000000000000:0x2000000000000000
    cursor: agent.systems
    mode: subscribe
    route: fleet-failures

routes:
  recording-failures:
    key: "run:{code}:{rel}"                  # one investigation per key
    trigger: {count: 5, window: 10m}         # or: immediate
    agent: recording-investigator
    claims: wk.incidents                     # state-CAS claims live here
    report: wk.incidents
    budget: {turns: 40, minutes: 30, usd: 2}
  fleet-failures:
    key: "mini:{author}:{name}"
    trigger: immediate
    agent: fleet-investigator
    claims: wk.incidents
    report: wk.incidents
    budget: {turns: 30, minutes: 20, usd: 1}

agents:
  recording-investigator:
    model: {provider: anthropic, modelId: claude-opus-5-5}
    extensions: [silly-tools, webkitten-tools]
    instructions: prompts/recording-investigator.md
```

A source says what to read. A route says which records become investigations and how they are grouped. An agent is a pi-durable agent configuration plus named extensions from the process's registry (conversations store extension names, not code).

## 4. Sources and waking

### 4.1 `subscribe` mode

The harness holds a WebSocket subscription per source with `NAMED_CURSOR`. Records arrive as `DATA`; `PROGRESS` ids are committed too, so a rare filter doesn't rescan after a reconnect. On `BEHIND` it resubscribes from `resume_from`.

### 4.2 `notify` mode

The source has a `webhook.notify` sink, created by the operator. The sink posts `{sink_id, head, unread}` to `POST /wake` at most about once a second. The body names the sink, so the config maps sink ids to sources. The harness then:

1. reads `GET /v1/stream/<id>/read?from=<cursor>&filter=…` in batches;
2. handles each batch (section 5);
3. moves the cursor with `PUT /v1/stream/<id>/cursor/<name>`;
4. repeats until it reaches `head`, then reads once more, because a wake that arrived meanwhile may have been coalesced.

`/wake` does no work itself: it records "source X is dirty", answers 2xx, and lets the loop run. The request is authenticated by a secret in the URL path or a signed header (question 3: `webhook.notify` isn't HMAC-signed in SPEC section 9; `webhook.deliver` is).

A wake means "look". It never means "one new record", and it can arrive with nothing new.

### 4.3 Timers

Some triggers are about absence: a mini that stopped reporting, an incident nobody closed. The harness has a small list of timed checks (for example "every minute, read each `wk.mini.*` state doc and compare `seen_ms`"), run from an in-process timer on Node and from the alarm on a Durable Object. A check produces synthetic records that go through the same triage and routing.

## 5. From a record to an investigation

This is the part that must be right. For each batch from a source, in id order:

1. **Decode.** Records to items, through the client library (item packing, zstd, the encrypted envelope if the stream uses it).
2. **Triage** (plain code, no model). Compute the route key from the template (`{code}`, `{rel}`, `{author}`, `{name}` and any field of the item). Update the key's counter in a small pi-durable document (`silly.triage`): count and first and last record ids within the window. Decide: below the trigger, do nothing more; at the trigger, open; already open, add.
3. **Claim** (when the route names a claims stream). A conditional `merge` on that stream's state doc sets `open.<key>` to this harness instance, as in AGENT-COORDINATION.md section 4. Losing the claim means another instance has it: skip.
4. **Submit.** One pi-durable commit that creates or finds the conversation for the key and submits the input, with

   `requestId = "<source>:<record id>"` for a record, or `"<source>:<key>:<first id>"` for an opening batch.

   The README says a `requestId` makes a submission idempotent: a retry returns the existing one. That is the whole exactly-once story: the cursor can be behind, the record can be read twice, and the second submit is a no-op.
5. **Commit the cursor** to the batch's last id, only after step 4's commit has returned.

A crash between 4 and 5 replays the batch, and `requestId` absorbs it. A crash before 4 replays it with nothing lost. The triage document is updated in the same pi-durable commit as the submit, so counters don't double-count on replay either (the commit records the last record id it counted, and skips ids at or below it).

**What the model gets** as input is small and structured: the key, the trigger that fired, the first few records (decoded), and where to read more (stream names and the id range). It doesn't get the whole backlog; it has tools for that.

### 5.1 More records for an open investigation

| conversation is | the harness |
|---|---|
| running a turn | submits with `whenBusy: "steer"` only for a change of severity (for example warn to fatal); otherwise just updates the count in `silly.triage`, which the model can read with a tool |
| parked on a wait | delivers a signal (section 6.3) |
| finished, key closed | starts over at step 2: a new window |

So a burst of 5,000 identical failures is one investigation and one counter, not 5,000 model turns.

## 6. The agent side

### 6.1 Tools

An extension, `silly-tools`, registered in the process. Each tool is a `defineTool` with a schema; all are `replay: "safe"` except `report` and `publish`, which are made safe by idempotency keys.

| tool | does | notes |
|---|---|---|
| `stream_read` | records from a stream by id or time range, filter and limit | decoded items; capped output, with a continuation id |
| `stream_state` | a stream's state doc and version | |
| `stream_count` | counts by options field over a range | rates and breakdowns from record headers; the tool reads the range and counts, so the model never sees the items |
| `triage_status` | the current count and ids for this investigation's key | |
| `publish` | writes records to an allowed stream | `producer_seq` derived from the tool call id, so a replay after a crash is dropped by the server |
| `report` | publishes the finding to the route's report stream **and** removes the claim, in one atomic publish with `expect_ver` | ends the investigation; a stale claim fails it, as fencing should |
| `wait_for` | parks the task until a matching record arrives or a time passes | section 6.3 |

Domain tools live in their own extension (for webkitten: look up a task in DODO, fetch a recording's metadata from the Depot). They are outside Sillystream and outside this spec.

The token given to the harness decides what the tools can touch: `read` on the sources, `publish` and `state` on the report and claims streams, nothing else. The model can't widen that.

### 6.2 Budgets and ends

Each route sets a budget of turns, wall time and spend (pi-durable keeps usage in its `pi.usage` document and has `maxTurns`). An investigation ends in one of three ways, and each one releases the claim:

- `report` was called: done;
- the budget ran out: the harness publishes a `gave_up` finding with the transcript's last summary;
- the task faulted (pi-durable's `restartLimit`, 5 by default): the harness publishes a `failed` finding from its `onReport` handler.

A claim also has `until_ms`. The harness renews it while the conversation is alive, so a dead harness's incidents become claimable again on their own.

### 6.3 Waiting without holding a process

An investigation often has to wait: "see whether the next ten runs on this release also fail", "check again in 15 minutes". The `wait_for` tool parks (the README describes tools that return `{ sleep }` or `{ wait }` and hold no process). The harness keeps a table of waits in a document: `(task id, stream, filter, until)`.

- When a record matching a wait arrives on a source, the router calls `harness.signal(taskId, RecordArrived, key, value)` with `key` = the record id. Signals report `duplicate` for a repeated key, so a replayed batch is harmless.
- When `until` passes first, pi-durable's own sleep deadline wakes the task; on a Durable Object that is `onWake` arming the alarm, and the alarm calling `resume()`.

On a Durable Object this is what makes the harness cost nothing between events: no subscription is held, the object sleeps, and either a `/wake` POST or its alarm brings it back.

## 7. Failure behaviour

| what happens | result |
|---|---|
| the harness crashes mid-batch | the cursor wasn't committed; the batch is read again and `requestId` drops what was already submitted |
| it crashes mid-investigation | `harness.resume()` at start continues unfinished work from the last checkpoint; an interrupted model request counts as a failed attempt |
| a tool call was interrupted | `replay: "safe"` tools rerun; `publish` and `report` rerun too and are deduplicated by seq, while the server's dedupe entry lives (24 h idle) |
| Sillystream is unreachable | sources reconnect with backoff; open investigations keep running until a tool needs the server, which then fails that call for the model to see |
| a wake is lost | `subscribe` mode has none to lose. In `notify` mode the next wake, or a safety poll every few minutes, catches up from the cursor |
| the harness is down longer than retention | error 7 on resume. It publishes a `host`-kind fatal finding saying which range was missed, and restarts from `earliest` |
| two harness instances run | each has its own storage; the claims stream decides who investigates a key. Two instances must not share one cursor name |
| the model loops or stalls | the route's budget ends it with `gave_up` |

## 8. Security

- **Record data is untrusted input to the model.** A log line or an event payload can contain text written by someone else, including text that looks like instructions. The input wraps record content as data, the agent's instructions say to treat it that way, and the token limits what a misled agent could do to the report stream.
- The README says nothing authenticates signal senders. Signals here are only ever sent by the harness's own router, never from the network.
- `/wake` accepts no content it acts on: only "source X is dirty".
- Decryption keys for encrypted streams live with the harness and never reach the model as text.

## 9. Build order

1. The JavaScript client's HTTP half (read, cursor, state, publish), against CLIENTS.md.
2. `silly-agent` on Node with SQLite: one source in `notify` mode, immediate trigger, `stream_read` and `report`, no claims. A fake stream of failures in a local server proves the exactly-once path (kill it between submit and commit).
3. Triage windows and counters; `subscribe` mode once the client's WebSocket half exists.
4. Claims, budgets, `wait_for` and signals.
5. The Durable Object host.
6. The webkitten extension and prompts, once WEBKITTEN-FLEET-LOGS.md is agreed.

## 10. What this needs from Sillystream

Nothing new in the server. It does lean on:

- the JS client (CLIENTS.md 4.2), not built;
- a `GET` for a named cursor (PATTERNS gap 4), which today is the `PUT` with 0 trick;
- the item packing being in SPEC (PATTERNS gap 7; being written down).

## 11. Open questions

1. **Node first or Durable Object first?** Node is simpler and can hold subscriptions; the Durable Object is the one that truly sleeps.
2. **One conversation per incident key (as written), or one long-lived conversation per route** that sees every incident and keeps memory across them? The first isolates failures and budgets; the second learns patterns but grows without bound and needs compaction.
3. **Authenticating `/wake`:** a secret path is enough for "look", but should `webhook.notify` be signed like `webhook.deliver`? That would be a spec change to raise.
4. **Which model and what budgets** per route. The values in section 3 are placeholders.
5. **How findings reach people:** a `webhook.deliver` sink on the report stream into chat, or something richer.
6. **pi-durable's stability:** it is experimental. Pin a version and keep the harness's use of it to the calls named at the top, so a breaking release is a small port.
