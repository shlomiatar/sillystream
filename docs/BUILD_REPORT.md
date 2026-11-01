# Build report

Written at commit `39aa32f` on `spec-rewrite`, plus the docs commit that adds this file. Nothing was pushed. `docs/SPEC.md` is unchanged in meaning; every choice it left open is in `docs/BUILD_NOTES.md`.

## In short

All ten milestones have code, and the real server and CLI work end to end on one Mac. The full test suite passed the one time I ran it at this commit. It is **not production ready**, and four things come first:

1. **Real S3, R2, EBS, Linux, power loss and machine loss were never exercised.** The durability story the spec cares most about is therefore unproven outside a process crash on macOS.
2. **Records can go missing with no error (Q32).** After a restart or eviction, a cursor below expired records resumes past them silently. This is the most serious open item and it needs your decision.
3. **M9's stalled-sink acceptance is not met.** Publishing slows to about 0.73 to 0.84 of baseline with stalled sinks, and 0.24 to 0.54 while they fill. The test passes only because its threshold is 0.7 and it checks only the steady phase. No cause was isolated.
4. **Clippy fails, and one quick-start trap exists.** Both are small and described below.

**Please check this first:** during M1, a broad `pkill` of mine matched outside this repo and killed other processes, including two other Claude Code sessions and possibly your apps. It can't be undone. Please check what needs restarting and whether any unsaved work was lost. Later work followed a rule of exact PIDs only.

## What was built

| crate | what it holds |
|---|---|
| `format` | records, ids, filters, block summaries |
| `proto` | WebSocket frames |
| `auth` | tokens, with no I/O on verify |
| `storage` | WAL, segments, object store access, uploader, metadata |
| `server` | cores, write path, state, delivery, HTTP and WebSocket APIs, recovery, compaction, sinks, and the `sillystream-server` binary |
| `sdk` | the Rust client, and the only crate with zstd |
| `cli` | the `silly` binary: tokens, admin, publish, tail, load generator |
| `e2e` | the real binaries as child processes over real TLS sockets |

About 66,000 lines of Rust, tests included, in 127 commits on this branch's history. Also see `docs/DEPLOYMENT.md`.

**Milestone status**

| milestone | state |
|---|---|
| M1 records, M2 frames and tokens, M3 storage, M4 cores and state | built; open findings below |
| M5 delivery | built; the commit and test gaps from its first pass were closed later |
| M6 APIs, SDK, CLI | built |
| M7 recovery, M8 retention and compaction | built |
| M9 sinks | built; **stall acceptance not met** |
| M10 end-to-end suite | built; **"3 consecutive runs" not repeated at this commit** (three clean runs are recorded from before it, see below) |

## How to run it

Tests and benchmarks:

```
export CARGO_TARGET_DIR=/Users/s/work/mcshlom/sillystream/target
cargo test --workspace --no-fail-fast
cargo test -p e2e
cargo test -p server --test crash_matrix -- --ignored   # the macOS full-volume test, needs hdiutil
cargo build --release -p server -p cli
target/release/silly bench latency --count 5000 --cores 8
```

A local node. I ran exactly these steps with the release binaries, and `publish` and `tail` round-tripped two records at level 2:

```
printf %s "$(head -c 32 /dev/urandom | xxd -p -c 64)" > secret   # no trailing newline, see below
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout ca.key -out ca.pem -days 1 -subj /CN=demo-ca
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout key.pem -out req.csr -subj /CN=localhost
printf 'subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\n' > ext
openssl x509 -req -in req.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -extfile ext -out cert.pem

target/release/sillystream-server --listen 127.0.0.1:18443 --wal-dir ./wal --data-dir ./data \
    --cert cert.pem --key key.pem --token-secret-file secret --object-store local:./store --cores 2

T=$(target/release/silly token issue --secret-file secret --author 7 \
    --grant 'demo*:read,publish,admin' --ttl-ms 600000)
target/release/silly publish demo.a --ack 2 --url localhost:18443 --ca ca.pem --token "$T" hello world
target/release/silly tail demo.a --from start --limit 2 --url localhost:18443 --ca ca.pem --token "$T"
```

A single self-signed certificate is refused by the client (`CaUsedAsEndEntity`), which is why there is a separate CA.

## Results as observed at `39aa32f`

| check | result |
|---|---|
| `cargo test --workspace --no-fail-fast`, debug, one run | **exit 0: 771 passed, 0 failed, 1 ignored**, plus the e2e harness's 21 tests and process check, across 69 test binaries. Workspace crates were rebuilt for this run |
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | **fails**: one `type_complexity` error, `crates/storage/benches/wal_prealloc.rs:43` |
| zstd in `server`, `storage`, `format`, `proto`, `auth` | none, in each crate's normal dependency tree. `sdk` has it |
| local node smoke test | works, as above |

The automated final verification step never finished, so these come from running them again while writing this report. The ignored test wasn't run at this commit.

Before the bench commit `39aa32f`, `BUILD_NOTES.md` records three full runs in a row on one commit, all clean, with the same counts. I did not re-observe those.

### Benchmarks

One run each on an Apple M4 Max laptop that was also doing other things (load average 4 to 7), with the client on the same machine. Treat differences under 2x as noise. Full tables and commands are in `docs/BENCH.md` under "Short run at `39aa32f`".

**The full benchmark plan did not run.** 10,000 subscribers, 15,000 idle connections, 10,000 to 1M idle streams, the 10 s and 60 s seal windows, 1 GiB and larger recovery and most criterion suites were not re-measured at this commit. Older figures remain in `BENCH.md`, each with its own commit.

| measure | observed |
|---|---|
| Level 1 ack, closed loop | p50 36 µs, p99 63 µs, p99.9 107 µs |
| Level 0 publish to delivery | p50 36 µs, p99 140 µs, p99.9 209 µs |
| Level 2 ack, laptop SSD with `F_FULLFSYNC` | p50 4.6 ms, p99 13.8 ms, p99.9 76.7 ms |
| Level 3 ack, `LocalFileSystem`, 1 s window | p50 506 ms, p99 1.0 s (not a durability figure) |
| Throughput, 1 KiB, 4 connections, one stream | 401K/s at level 1, 383K/s at level 2 |
| Throughput, 64 KiB, 4 connections, one stream | 1,383 MB/s at level 1, 762 MB/s at level 2 |
| Level 2, 4 streams, 256 outstanding each | 111K publishes/s, ack p50 9.1 ms |
| Fan-out, 100 subscribers flat out | 255K deliveries/s, none lost, no `BEHIND` |
| Fan-out, 1,000 subscribers at 10 publishes/s | p50 6.5 ms, p99 13.5 ms, none lost |
| Recovery after `kill -9`, 256 MiB WAL | 0.21 s to the port, 65,536 of 65,536 acked records read back |
| Catch-up, 8 MiB | memory 1,325 to 2,085 MB/s; disk 1,182 to 2,283; `LocalFileSystem` 1,665 to 1,758 |
| Idle connection | 21.8 KB of server RSS each, at 5,000 |

Older results that still stand, from their own commits: a filter scan runs about 270 to 340 M headers/s, about 3 ns per record, and block skipping is about 60x faster on clustered data at 10% match or below, with no gain on scattered data. WAL throughput at 256 B is about 750K entries/s. Fan-out without sockets costs about 150 ns per subscriber on one core. Objects per seal are about 1 per window at 10, 1,000 and 10,000 streams (the last is an inexact count), and memory is about 9.4 KB per idle stream at 20,000.

Level 1 fits section 7.2's estimate. The level 2 EBS and level 3 S3 estimates remain **unmeasured**.

## What is not working

**New, found while writing this, not fixed:**

- `token issue` keeps a trailing newline in the secret file, while the server strips it. A secret from `xxd` or `echo` gives tokens that are always refused with "token signature is wrong". One side should change.
- Clippy fails, as above.

**Known and open:**

- **Flaky tests, not fixed:** `a_manifest_only_in_the_wal_leaves_its_object_unrecorded` and `on_expire_stop_warns_then_halts` are timing-dependent. Clean runs don't settle them, so a workspace run can still go red.
- **M9 stall acceptance:** as above.
- **Silent or lasting loss and damage cases:** Q32 (silent gap past retention), Q40 (a retry after losing both disks can be stamped again and apply state twice), Q15 (bit rot in a closed WAL file can leave a silent gap, and only warns), Q43 (WAL entries fenced by a start before the volume is attached stay unreadable, with no tool to restore them).
- **Crash windows:** a level 0 or 1 state change in a snapshot can refer to an id a crash erased (D136). A snapshot can show state that a crash then erases (Q20).
- **Stopped WAL:** the process stays up refusing writes, so a process-level health check still passes (Q42).
- **Stalls and retries:** an upload error the uploader can't recognise retries forever on its one thread (Q17).
- **Unbounded or uncapped:** no connection limit, so a client without a token can hold enough sockets to block new connections and pause writes while it holds them. Inbound HTTP bodies and WebSocket buffers have no total bound. Sink limits are per stream and not per tenant. Checkpoint files pile up on a running node until its next restart (Q8). Move history is never trimmed (Q21).
- **Sinks:** three forward-sink cases can still drop records silently (Q37). Forward cycles across streams aren't detected (Q35). An archive sink that hits a changed-filter name collision stays paused for that range (Q38). Sink secrets are in plaintext in metadata (Q36).
- **Two real processes starting together** can both run, one per generation, against the M7 "one winner" wording (Q31).
- **Older findings:** "Review findings not fixed" in `BUILD_NOTES.md` was only partly re-checked, so don't read it as complete or current.

**Bench observations with no proven cause:** at 10,000 connections and a load above capacity, a few subscribers stalled for 10 s or more. It looks like a macOS loopback TCP stall rather than a server bug, but that is inferred. In one run, about 4,000 connections appeared to reconnect, unexplained. Neither was repeated here.

## What was not exercised

- **Real S3 and R2.** Only `LocalFileSystem`, `InMemory` and wrappers over them. The S3 path is configured and compiled, and was run only against a local stub answering one PUT. Conditional puts on R2 are unverified.
- **EBS**, and any cloud service or credential.
- **Linux.** Everything ran on macOS, where `sync_data` is `F_FULLFSYNC`, so sparse preallocation (D51) is unmeasured where it matters.
- **Power loss and machine loss.** `kill -9` and abort prove a process crash only, since the page cache survives them.
- **Multiple machines.** Takeover is tested with processes on one Mac.
- **Fuzzing.** No `cargo-fuzz` targets. Decoders have property tests, byte-exact vectors and no-panic tests.
- **Test gaps:** see "Known test gaps" in `BUILD_NOTES.md`, which includes surviving mutations. `sink_forward`, `state_cas`, `atomic_publish` and `token_rejection` had no mutation run.

## Decisions to review

All are in `docs/BUILD_NOTES.md`, which has D-numbered decisions and Q-numbered questions. The ones that shape the product most:

- **New surface the spec doesn't list:** the admin HTTP routes (D21, Q2) and the manifest stream `_sys.manifests` with entry type 4 (D158, D159, Q3, Q4).
- **Ids:** a bound on how far ids run ahead of the clock, and the timestamp cost after a restart: new records carry timestamps up to 70 s ahead for the first 70 s (D221, Q7, Q13).
- **WAL:** recycling against the spec's "one preallocated file" (D8, Q5), sparse preallocation (D51, D60), and a dead WAL reported as error 4 (D79, Q22).
- **Durability of metadata:** config, sink and cursor changes durable at the next seal (D143), with a later partial fix for stream config (D248).
- **Locks:** one mutex per connection between a home core and that connection's reader (D99, Q27). **This departs from your "cores share no locks" rule in letter**, although no two cores hold it, so please decide.
- **Limits I picked:** cursors (D218, Q39), sinks (D222, Q41), the 16 MiB bound on HTTP read responses (D238), stall detection (Q28).
- **Permissions:** whether `admin` implies read through a sink (D235).

## Questions for you

**Needs a decision before going further:**

1. Q32: save what expired, so a cursor is told after a restart too? It needs one `u64` that section 7.6 doesn't list. The earlier recommendation is Q32's option (a).
2. Q40: should an object carry its dedupe entries? Every option adds a field or changes a guarantee.
3. Q42: should the server exit when the WAL writer stops?
4. Q43: should a start with an empty `wal_dir` fence at all?
5. Should the server limit connections? The spec has no such mechanism, so none was added.
6. **Your message "Yes let's add the rule" doesn't say which rule.** Nothing in the task named one, so I added no rule to `CLAUDE.md` or to config. Candidates are the exact-PID rule for stopping processes or an item from the notes. Please say which you meant.

**Please confirm or change the working assumption** (each is one line in `BUILD_NOTES.md`): Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20, Q21, Q22, Q23, Q24, Q25, Q26, Q27, Q28, Q29, Q30, Q31, Q33, Q34, Q35, Q36, Q37, Q38, Q39, Q41, Q44. Q25 is the one most likely to surprise agents: subscribing to a stream that doesn't exist yet gets error 2 instead of waiting.

## Housekeeping

- **Stray process:** PID 4275, `target/debug/deps/wal_writer-53ccb395b8f4af84`, parent 1, over a day old. I didn't start it, so I left it. Nothing depends on it, and it's safe for you to stop. Worth checking how a `wal_writer` test outlived its runner. No other server or test process was left by this run.
- **Shared target directory:** it can give false results while other checkouts build into it (`BUILD_NOTES.md`, Process). No other build was running when the test run above started, but I didn't watch for one during it.
- **A rebuilt notes section:** a script of mine once truncated an uncommitted `BUILD_NOTES.md`. D82 to D94 were rebuilt by hand from a captured diff, so their wording could differ slightly from the original.
- **Tracked test of unclear origin:** `crates/server/tests/move_stress.rs` was found untracked and later committed in `9347c70`. It passes, but who wrote it was never established.
