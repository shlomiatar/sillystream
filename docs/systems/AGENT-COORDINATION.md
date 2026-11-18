# Agent coordination on Sillystream

Status: draft for review, 2026-11-12, not built. It sits on top of `docs/SPEC.md` and adds nothing to the server: everything here is a convention between clients. `docs/PATTERNS.md` section 7 has the recipes this builds on, each run against a real server; this document turns them into one system with fixed names, layouts and rules.

## 1. What it is

A way for a set of agents (LLM agents, workers, people) to share work without a coordinator service. A **team** is a handful of streams. Agents post tasks, claim them, report results, hand work to each other and leave notes, and a sleeping agent is woken when something arrives for it.

Scope: tens to a few hundred agents per team, tasks that take seconds to hours, any number of teams. Out of scope: scheduling by priority across teams, exactly-once side effects (an agent's own actions must be idempotent), and secrets in state.

## 2. Streams

| stream | holds | state doc | retention |
|---|---|---|---|
| `team.<team>` | the board: tasks, results, messages to everyone | task claims (section 4) | `7d` |
| `team.<team>.inbox.<agent>` | records for one agent: direct messages, handoffs, assigned tasks | that agent's presence and current work | `1d` |
| `team.<team>.lease.<name>` | nothing, or an audit trail | one lease (section 6) | `1d` |
| `team.<team>.room.<room>` | a working conversation between a few agents | room metadata: topic, members, pinned facts | `7d` |

Access is per stream (SPEC section 12), which the layout uses:

- every agent's token grants `read`, `publish` and `state` on `team.<team>` and `team.<team>.room.*`;
- an agent has `read` and `state` on its own inbox only, and `publish` on every inbox (`team.<team>.inbox.*`), so anyone can write to an agent but only it can read;
- lease streams grant `state` to the agents allowed to hold that lease.

The token carries the agent's `author` id, so every record and every state change says who made it, and nobody can forge that.

## 3. Records

One item per record (`count = 1`), since coordination traffic is small and order matters more than compression. Data is a JSON object; encrypt it (CLIENTS.md codec 2) if the server operator shouldn't read it.

### 3.1 Options

```
bits 56–63  kind      0x01 task, 0x02 result, 0x03 message, 0x04 handoff, 0x05 note, 0x06 cancel
bits 40–55  skills    one-hot, team-defined: bit 40 rust, 41 python, 42 docs, …
bit  39     urgent
bit  38     needs a person
bits 8–37   app-defined
bits 0–7    partition (section 5.3), 0 when unused
```

Filters an agent uses:

| want | filter |
|---|---|
| tasks I can do (rust or python) | `eq:0xff00010000000000:0x0100010000000000,eq:0xff00020000000000:0x0100020000000000` |
| anything urgent | `any:0x0000008000000000` |
| results only | `eq:0xff00000000000000:0x0200000000000000` |
| what one agent wrote | add `author:<id>` |

### 3.2 Data

| kind | fields |
|---|---|
| task | `title`, `body`, `inputs` (pointers, not blobs), `deadline_ms`, `parent` (task id, for subtasks) |
| result | `task` (the task record's id), `status` (`done`, `failed`, `gave_up`), `summary`, `outputs` (pointers), `claim_ver` (section 4.2) |
| message | `text`, `re` (a record id it answers) |
| handoff | `task`, `to` (agent id), `context` (what the next agent needs), `claim_ver` |
| note | `text`: something learned that others should know |
| cancel | `task`, `reason` |

**A task's id is its record id.** It's unique, ordered, and carries its ingest time.

## 4. Claims

The board's state doc holds who has what:

```
{
  "claims": {
    "<task id>": {"by": <agent id>, "until_ms": <lease end>, "tries": 1}
  }
}
```

### 4.1 Claiming

1. An agent subscribes to the board with its skills filter and `WITH_STATE`, so it has the claims and their version.
2. To take a task, it sends a `merge` that sets `claims.<task id>` with `expect_ver` = the version it last saw.
3. A version conflict (error 3) means the doc changed. The agent takes the new state, checks whether the task is still free, and tries again or moves on.
4. A claim lasts `until_ms`. The holder extends it with another conditional merge while it works. A claim past `until_ms` is free for anyone to take over, with `tries + 1`.

One state doc for the whole board keeps this simple, and makes every claim contend on one version. That is fine for tens of claims a second; past that, partition (section 5.3).

### 4.2 Fencing

A claim's merge returns a version. The worker puts it in everything it writes about that task (`claim_ver`). When it finishes, it publishes the result **and** removes the claim in one atomic publish with `expect_ver` = its last version (SPEC section 10, "Atomic changes"). If the claim was taken over meanwhile, the publish fails and the stale result never reaches the board.

Side effects outside Sillystream can't be fenced this way. They carry `claim_ver` as a token, and the outside system refuses a lower one than it has seen.

### 4.3 Housekeeping

- The claims map holds open claims only: finishing removes the entry. With the 64 KB limit that is on the order of 1,000 open claims; a team that needs more partitions.
- A task whose `tries` reaches the team's limit (3 by default) is given up: the next agent to see it publishes a `result` with `gave_up` and removes the claim.
- A `cancel` record asks the holder to stop. The holder publishes a `failed` result and removes its claim.

## 5. Ways to hand out work

### 5.1 Competing workers (default)

Every worker reads the board and claims as in section 4. No dispatcher, and workers come and go freely. Each worker keeps a named cursor (`worker.<agent>`), commits it as it goes, and after a restart re-reads from there and skips tasks that have a result or a live claim.

### 5.2 Dispatcher and inboxes

One agent holds the `dispatcher` lease (section 6), reads tasks and publishes each to an inbox as a `handoff`. Use this when choosing the worker takes judgment (an LLM planner) or workers shouldn't see the whole board. The dispatcher publishes with `producer_seq` = the task id, so a retry after a crash doesn't hand the task out twice.

### 5.3 Static partitions, no code

Producers set the low 8 bits to `hash(key) mod N`. One `forward` sink per partition sends `eq:0xff:<p>` to that worker's inbox. Nothing claims anything, because each partition has one reader. Changing N means re-creating the sinks.

## 6. Leases

A lease is a state doc with one holder:

```
{"holder": <agent id>, "until_ms": <end>}
```

- **Take:** `set` with `expect_ver` = the version read, when the doc is empty or `until_ms` has passed. The version returned is the **fencing token**.
- **Renew:** `merge` of `until_ms` with `expect_ver` = the token; the new version is the new token.
- **Release:** `set` to `{}` with `expect_ver` = the token.
- A holder that can't renew before `until_ms` must stop acting as the holder.

Clocks: `until_ms` is compared against each agent's own clock, so leave a margin of a few seconds, and renew at a third of the lease length.

**First take on a fresh stream** needs the conditional create from the pending state change (`expect_ver = u64::MAX` means "only if never set"). Until that lands, a setup step initializes each lease doc to `{}`.

## 7. Presence

Each agent keeps its inbox's state doc current:

```
{"status": "idle" | "working" | "sleeping", "task": "<id>", "skills": [...], "seen_ms": <now>, "wake": "<how>"}
```

It rewrites `seen_ms` about once a minute while awake. Only the agent itself can read its inbox (section 2), so this doc is private: it is what the agent restores itself from after a restart.

The team's roster comes from the **board** instead. An agent publishes a `note` there when its status changes, and the board's readers build the roster from those notes.

(Alternative, if a live roster matters more than privacy: grant `read` on inbox state to the team. Sillystream has no "state only" permission, so that also exposes inbox records. Left as an open question.)

## 8. Waking a sleeping agent

An agent that isn't connected has a `webhook.notify` sink on its inbox, and optionally one on the board with its skills filter. The sink posts `{sink_id, head, unread}` to the agent's wake URL, at most about once a second (PATTERNS 5.1). The body names the sink, not the stream, so the agent keeps a map from sink id to stream.

On a wake the agent:

1. opens the stream with its named cursor and reads what is new;
2. does the work, or claims and starts it;
3. commits the cursor and goes back to sleep.

The sink is at-least-once and rate-limited, so a wake means "look", never "exactly one new thing". An agent harness that does this is specified in `docs/systems/AGENT-HARNESS.md`.

## 9. Rooms

A room is a stream that a few agents read and write while working together. The state doc holds what a newcomer needs first: topic, members, decisions so far, pointers. A newcomer subscribes with `WITH_STATE` from `head`, so it gets the summary and then live traffic without replaying history. Whoever changes a decision merges it into the doc in the same publish as the message that explains it.

## 10. Failure behaviour

| what happens | result |
|---|---|
| a worker dies mid-task | its claim expires at `until_ms` and another worker takes it with `tries + 1` |
| a worker stalls, then wakes after takeover | its final publish fails on `expect_ver`; outside systems refuse its `claim_ver` |
| a publisher retries after a lost ack | dropped by `(producer_id, producer_seq)` while the dedupe entry lives; otherwise a duplicate task, which gets its own id and is worked twice. Put an idempotency key in the task data if that matters |
| the server crashes with a level 1 publish | the record can be lost. Tasks, results and handoffs use level 2 |
| a reader falls behind retention | error 7 on resume. The board's `7d` class is the longest a worker may sleep and still see every task |

## 11. What this needs from Sillystream

Works today, with the setup step noted. It gets simpler with three of the changes already approved:

- **conditional create** (`expect_ver = u64::MAX`): leases and claims on a fresh stream without setup;
- **`WITH_STATE` pushes `STATE`**: a worker sees claim changes on the same subscription it reads tasks on, where today it needs a second `STATE_ONLY` subscription;
- a sink creatable before its stream exists (PATTERNS gap 8): inbox sinks at agent registration.

## 12. Open questions

1. **Roster privacy versus a live roster** (section 7).
2. **One claims doc per board, or one stream per task?** A stream per task gives each task its own state, log and retention, and removes contention, at the price of a stream per task and a fan-in for the board.
3. **Claim length defaults:** 60 s with renewal every 20 s is assumed; long LLM turns may need more.
4. **Who gives up a task:** any worker (as written), or only a designated agent?
5. **Encryption by default** for task data, given the server operator can read plaintext JSON.
