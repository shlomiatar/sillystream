//! The end-to-end suite (BUILD_PLAN M10): the real binaries, as child processes, with throwaway
//! TLS certificates, a temp dir, and `LocalFileSystem` as the object store. Clients use real TLS
//! WebSocket and HTTP sockets and never call server internals. Only `retention_and_compaction`
//! uses a `test-hooks` build. Real S3 and EBS aren't exercised, and `kill -9` proves process
//! crashes only, since the page cache survives it.
//!
//! `cargo test -p e2e` runs them all, one at a time. Words on the command line pick by name.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use e2e::*;
use hmac::{Hmac, Mac};
use proto::{AckLevel, ClientFrame, ErrorCode, StateOp, WireClause};
use sdk::{BatchConfig, FROM_LIVE, PublisherConfig, SdkError, StateChange, Subscribe};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const SUITE_DEADLINE: Duration = Duration::from_secs(1_500);
const TEST_DEADLINE: Duration = Duration::from_secs(400);
const AUTHOR: u64 = 77;
const ADMIN_AUTHOR: u64 = 5;

type TestFn = fn() -> std::pin::Pin<Box<dyn Future<Output = ()>>>;

macro_rules! tests {
    ($($name:ident),* $(,)?) => {
        &[$((stringify!($name), (|| Box::pin($name())) as TestFn)),*]
    };
}

const TESTS: &[(&str, TestFn)] = tests![
    sinks_leave_the_node_able_to_take_connections,
    a_stopped_wal_says_so_on_stderr,
    a_stream_id_cannot_forge_a_log_line,
    publish_to_live,
    filters,
    catch_up_memory,
    catch_up_disk,
    catch_up_object_store,
    state_cas,
    atomic_publish,
    token_rejection,
    kill9_mid_load,
    sink_webhook_outage,
    sink_forward,
    retention_and_compaction,
    normal_build_has_no_hooks,
    a_publisher_cannot_skew_other_streams_through_a_restart,
];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let words: Vec<&String> = args.iter().filter(|arg| !arg.starts_with('-')).collect();
    if args.iter().any(|arg| arg == "--list") {
        for (name, _) in TESTS {
            println!("{name}: test");
        }
        return;
    }
    std::thread::spawn(|| {
        std::thread::sleep(SUITE_DEADLINE);
        eprintln!("the suite ran past its deadline; stopping what it started");
        kill_everything_started();
        std::process::exit(2);
    });
    binaries();
    let leftover = servers_running();
    assert!(
        leftover.is_empty(),
        "servers before the suite: {leftover:?}"
    );
    let mut failed = Vec::new();
    let mut ran = 0;
    for (name, test) in TESTS {
        if !words.is_empty() && !words.iter().any(|word| name.contains(word.as_str())) {
            continue;
        }
        ran += 1;
        println!("test {name} ...");
        let started = Instant::now();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("a runtime");
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::time::timeout(TEST_DEADLINE, test())
                    .await
                    .expect("the test ran past its deadline");
            })
        }));
        // Its tasks, and any node they hold, go before the next test or the final check.
        runtime.shutdown_timeout(Duration::from_secs(10));
        let verdict = if result.is_ok() { "ok" } else { "FAILED" };
        println!(
            "test {name} ... {verdict} ({:.1} s)",
            started.elapsed().as_secs_f64()
        );
        if result.is_err() {
            failed.push(*name);
        }
    }
    // Acceptance 11: however the tests ended, no server of ours remains.
    let remaining = servers_running();
    println!(
        "test no_server_processes_remain ... {}",
        if remaining.is_empty() { "ok" } else { "FAILED" }
    );
    if !remaining.is_empty() {
        eprintln!("still running: {remaining:#?}");
        kill_everything_started();
        failed.push("no_server_processes_remain");
    }
    println!(
        "{ran} tests and the process check, {} failed: {failed:?}",
        failed.len()
    );
    if !failed.is_empty() {
        std::process::exit(1);
    }
}

fn raw_publisher(ack: AckLevel) -> PublisherConfig {
    PublisherConfig {
        ack,
        batch: BatchConfig {
            zstd_level: None,
            ..BatchConfig::default()
        },
        ..PublisherConfig::default()
    }
}

// ---------------------------------------------------------------------------------------------
// 1. publish_to_live
// ---------------------------------------------------------------------------------------------

async fn publish_to_live() {
    let node = Node::started(NodeOptions::default());
    let token = node.token(AUTHOR, &[&format!("*:{ALL_PERMS}")], 3_600_000);
    const STREAM: &str = "live.one";
    node.cli(&token, &["stream", "create", STREAM]);

    // Two subscribers on real sockets, and one through the CLI.
    let mut readers = Vec::new();
    for sub in [1, 2] {
        let mut ws = node.ws(&token).await;
        ws.subscribe(sub, STREAM, FROM_LIVE, false).await;
        assert!(matches!(ws.recv().await, Owned::Subscribed { .. }));
        readers.push(ws);
    }
    let mut expected = Vec::new();
    let mut writer = node.ws(&token).await;
    writer.open(1, STREAM).await;
    let http = node.http(&token);
    for at in 0..200u64 {
        let data: Vec<u8> = (0..(at % 97) as u8).chain([at as u8]).collect();
        let sent = record(at % 7, (at % 2) as u16, &data);
        let first_id = if at % 3 == 0 {
            http.publish_records(STREAM, &sent).await
        } else {
            writer
                .publish(&PublishFrame {
                    handle: 1,
                    producer_id: 9,
                    seq: at + 1,
                    ack: AckLevel::WalFsync,
                    state: None,
                    records: &sent,
                })
                .await;
            let Owned::Ack {
                first_id, count, ..
            } = writer.recv().await
            else {
                panic!("expected ACK");
            };
            assert_eq!(count, 1);
            first_id
        };
        expected.extend_from_slice(&stamped(&sent, first_id, AUTHOR));
    }
    // The simple form of section 11.2.
    let reply = http
        .call(
            "POST",
            &format!("/v1/stream/{STREAM}/publish?ack=2"),
            &[
                ("x-options", "0x8000000000000000"),
                ("x-producer-id", "7"),
                ("x-producer-seq", "42"),
            ],
            b"one item's bytes",
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let simple_id: u64 = json_field(&reply.text(), "first_id")
        .unwrap()
        .parse()
        .unwrap();
    expected.extend_from_slice(&stamped(
        &record(1 << 63, 0, b"one item's bytes"),
        simple_id,
        AUTHOR,
    ));

    let last_id = simple_id;
    for reader in &mut readers {
        let mut got = Vec::new();
        let mut at = 0;
        while at < last_id {
            match reader.recv().await {
                Owned::Data { records, .. } => {
                    at = parse_records(&records).last().unwrap().last_id();
                    got.extend_from_slice(&records);
                }
                Owned::Progress { .. } => {}
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(got, expected, "live bytes differ from what was published");
        let ids: Vec<u64> = parse_records(&got).iter().map(|one| one.id).collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "out of id order"
        );
    }
    assert_eq!(http.read_bytes(STREAM, 0, None).await, expected);

    // The CLI on both ends, with the SDK's own compression in between.
    let printed = node.cli(
        &token,
        &["publish", STREAM, "--options", "3", "from the cli"],
    );
    assert!(printed.starts_with("first_id="), "{printed}");
    let tail = node.cli(
        &token,
        &[
            "tail",
            STREAM,
            "--from",
            &last_id.to_string(),
            "--limit",
            "1",
        ],
    );
    assert!(
        tail.trim_end()
            .ends_with(&format!("0x3 {AUTHOR} from the cli")),
        "{tail}"
    );
    drop(node);
}

// ---------------------------------------------------------------------------------------------
// 2. filters
// ---------------------------------------------------------------------------------------------

struct FilterCase {
    name: &'static str,
    clauses: Vec<WireClause>,
    author: Option<u64>,
    http: &'static str,
    wanted: fn(u64, u64) -> bool,
}

fn filter_cases() -> Vec<FilterCase> {
    const ERROR_BIT: u64 = 1 << 63;
    let eq = |mask, value| WireClause { mask, value, op: 0 };
    let any = |mask| WireClause {
        mask,
        value: 0,
        op: 1,
    };
    vec![
        FilterCase {
            name: "eq",
            clauses: vec![eq(0xFF, 0x01)],
            author: None,
            http: "eq:255:1",
            wanted: |options, _| options & 0xFF == 1,
        },
        FilterCase {
            name: "any",
            clauses: vec![any(0x100)],
            author: None,
            http: "any:256",
            wanted: |options, _| options & 0x100 != 0,
        },
        FilterCase {
            name: "or",
            clauses: vec![eq(0xFF, 0x02), any(ERROR_BIT)],
            author: None,
            http: "eq:255:2,any:9223372036854775808",
            wanted: |options, _| options & 0xFF == 2 || options & ERROR_BIT != 0,
        },
        FilterCase {
            name: "author",
            clauses: vec![],
            author: Some(11),
            http: "author:11",
            wanted: |_, author| author == 11,
        },
        FilterCase {
            name: "author and any",
            clauses: vec![any(0x100)],
            author: Some(22),
            http: "any:256,author:22",
            wanted: |options, author| options & 0x100 != 0 && author == 22,
        },
        FilterCase {
            name: "nothing matches",
            clauses: vec![eq(0xFF, 0x77)],
            author: None,
            http: "eq:255:119",
            wanted: |_, _| false,
        },
    ]
}

async fn filters() {
    let node = Node::started(NodeOptions::default());
    let admin = node.admin_token(ADMIN_AUTHOR);
    const STREAM: &str = "filter.me";
    node.cli(&admin, &["stream", "create", STREAM]);
    let http = node.http(&admin);
    let cases = filter_cases();

    let reader = node.sdk(&admin).await;
    let mut live = Vec::new();
    for case in &cases {
        let mut spec = Subscribe::new(STREAM, FROM_LIVE);
        spec.clauses = case.clauses.clone();
        spec.author = case.author;
        let mut subscription = reader.subscribe(spec).unwrap();
        until_subscribed(&mut subscription).await;
        live.push(subscription);
    }
    let mut spec = Subscribe::new(STREAM, FROM_LIVE);
    spec.include_system = true;
    spec.clauses = vec![WireClause {
        mask: 0xFF,
        value: 0x77,
        op: 0,
    }];
    let mut system_live = reader.subscribe(spec).unwrap();
    until_subscribed(&mut system_live).await;

    // Two authors over many options, and a state op among them.
    let options_set: [u64; 8] = [1, 2, 3, 0x100, 0x101, 0x102, 1 << 63, (1 << 63) | 1];
    let mut published: Vec<(u64, u64, u64, Vec<u8>)> = Vec::new();
    let mut publishers = Vec::new();
    for author in [11u64, 22] {
        let client = node
            .sdk(&node.token(author, &[&format!("*:{ALL_PERMS}")], 3_600_000))
            .await;
        let publisher = client
            .publisher(STREAM, raw_publisher(AckLevel::WalFsync))
            .unwrap();
        publishers.push((author, client, publisher));
    }
    let mut state_id = 0;
    for round in 0..6usize {
        for (at, options) in options_set.iter().enumerate() {
            let (author, _, publisher) = &mut publishers[(at + round) % 2];
            let bytes = format!("{round}:{at}").into_bytes();
            publisher.add(*options, bytes.clone());
            let ack = publisher.flush().await.unwrap()[0];
            published.push((ack.first_id, *options, *author, bytes));
        }
        if round == 2 {
            let client = &publishers[0].1;
            state_id = client
                .state_op(
                    STREAM,
                    StateChange {
                        expect_ver: 0,
                        op: StateOp::Merge,
                        doc: cbor_map(&[("round", 2)]),
                    },
                )
                .await
                .unwrap();
        }
    }
    let wanted = |case: &FilterCase, after: u64| -> Vec<(u64, u64, u64, Vec<u8>)> {
        published
            .iter()
            .filter(|(id, options, author, _)| *id > after && (case.wanted)(*options, *author))
            .cloned()
            .collect()
    };
    let check = |what: String, items: Vec<sdk::Item>, wanted: Vec<(u64, u64, u64, Vec<u8>)>| {
        let got: Vec<(u64, u64, u64, Vec<u8>)> = items
            .into_iter()
            .map(|item| (item.id, item.options, item.author, item.bytes))
            .collect();
        assert_eq!(got, wanted, "{what}");
    };
    for (case, subscription) in cases.iter().zip(&mut live) {
        let want = wanted(case, 0);
        let items = collect_items(subscription, want.len()).await;
        check(format!("live {}", case.name), items, want);
    }
    let system = collect_items(&mut system_live, 1).await;
    assert_eq!(
        system.len(),
        1,
        "only the system record, whatever the filter (D3)"
    );
    assert_eq!(
        (system[0].id, system[0].author, system[0].options),
        (state_id, 0, 2)
    );
    assert_eq!(system[0].bytes[..8], 11u64.to_le_bytes(), "the actor");
    assert_eq!(system[0].bytes[8..], cbor_map(&[("round", 2)]));

    // The same sets in catch-up, from the start and from an id in the middle, in both tiers.
    let middle = published[published.len() / 2].0;
    for phase in ["memory", "sealed"] {
        if phase == "sealed" {
            wait_until("a seal", async || !node.objects("sealed").is_empty()).await;
        }
        for case in &cases {
            for from in [0, middle] {
                let mut spec = Subscribe::new(STREAM, from);
                spec.clauses = case.clauses.clone();
                spec.author = case.author;
                let mut subscription = reader.subscribe(spec).unwrap();
                let want = wanted(case, from);
                let items = collect_items(&mut subscription, want.len()).await;
                check(
                    format!("{phase} catch-up {} from {from}", case.name),
                    items,
                    want.clone(),
                );
                let bytes = http.read_bytes(STREAM, from, Some(case.http)).await;
                let got: Vec<u64> = parse_records(&bytes).iter().map(|one| one.id).collect();
                let want: Vec<u64> = want.iter().map(|one| one.0).collect();
                assert_eq!(got, want, "HTTP {phase} {} from {from}", case.name);
            }
        }
        let mut spec = Subscribe::new(STREAM, 0);
        spec.include_system = true;
        let mut everything = reader.subscribe(spec).unwrap();
        let items = collect_items(&mut everything, published.len() + 1).await;
        assert_eq!(items.len(), published.len() + 1);
        assert_eq!(items.iter().filter(|item| item.author == 0).count(), 1);
        // And a reader that didn't ask doesn't get it.
        let mut plain = reader.subscribe(Subscribe::new(STREAM, 0)).unwrap();
        let items = collect_items(&mut plain, published.len()).await;
        assert!(items.iter().all(|item| item.author != 0));
    }

    // From an id, across the end of catch-up into live: nothing lost or repeated at the joint.
    let mut crossing = Vec::new();
    for case in &cases {
        let mut spec = Subscribe::new(STREAM, middle);
        spec.clauses = case.clauses.clone();
        spec.author = case.author;
        let mut subscription = reader.subscribe(spec).unwrap();
        let want = wanted(case, middle);
        let items = collect_items(&mut subscription, want.len()).await;
        check(format!("crossing catch-up {}", case.name), items, want);
        crossing.push(subscription);
    }
    let mut later: Vec<(u64, u64, u64, Vec<u8>)> = Vec::new();
    for (at, options) in options_set.iter().enumerate() {
        let (author, _, publisher) = &mut publishers[at % 2];
        let bytes = format!("later:{at}").into_bytes();
        publisher.add(*options, bytes.clone());
        let ack = publisher.flush().await.unwrap()[0];
        later.push((ack.first_id, *options, *author, bytes));
    }
    for (case, subscription) in cases.iter().zip(&mut crossing) {
        let want: Vec<_> = later
            .iter()
            .filter(|(_, options, author, _)| (case.wanted)(*options, *author))
            .cloned()
            .collect();
        let items = collect_items(subscription, want.len()).await;
        check(format!("crossing live {}", case.name), items, want);
    }
}

// ---------------------------------------------------------------------------------------------
// 3. catch_up_*
// ---------------------------------------------------------------------------------------------

/// Publishes in several frames over WebSocket and returns the exact bytes a reader must get.
async fn fill_for_catch_up(node: &Node, token: &str, stream: &str, count: u64) -> (Vec<u8>, u64) {
    let mut writer = node.ws(token).await;
    writer.open(1, stream).await;
    let mut expected = Vec::new();
    let mut last_id = 0;
    for at in 0..count {
        let data = vec![at as u8; 100 + (at % 900) as usize];
        let sent = record(at, 0, &data);
        writer
            .publish(&PublishFrame {
                handle: 1,
                producer_id: 3,
                seq: at + 1,
                ack: AckLevel::WalFsync,
                state: None,
                records: &sent,
            })
            .await;
        let Owned::Ack { first_id, .. } = writer.recv().await else {
            panic!("expected ACK");
        };
        assert!(first_id > last_id);
        last_id = first_id;
        expected.extend_from_slice(&stamped(&sent, first_id, AUTHOR));
    }
    (expected, last_id)
}

/// Reads from `from = 0` on a socket and on the read route. Both must be exactly `expected`, and
/// the server's own trace must say this tier and no other.
async fn check_tier(
    node: &Node,
    token: &str,
    stream: &str,
    expected: &[u8],
    last_id: u64,
    tier: &str,
) {
    let mut ws = node.ws(token).await;
    let got = ws.read_through(1, stream, 0, false, last_id).await;
    assert!(
        got == expected,
        "WebSocket catch-up bytes differ ({} and {})",
        got.len(),
        expected.len()
    );
    let ids: Vec<u64> = parse_records(&got).iter().map(|one| one.id).collect();
    assert!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "out of order or doubled"
    );
    let read = node.tiers_read(stream);
    assert!(
        !read.is_empty(),
        "the server traced no catch-up: {}",
        node.log_text()
    );
    assert!(
        read.iter().all(|(name, _)| name == tier),
        "wanted only {tier}: {read:?}"
    );
    let bytes: u64 = read.iter().map(|(_, bytes)| bytes).sum();
    assert!(
        bytes >= expected.len() as u64,
        "{bytes} traced for {} read",
        expected.len()
    );

    let through_http = node.http(token).read_bytes(stream, 0, None).await;
    assert!(through_http == expected, "the read route's bytes differ");
    let read = node.tiers_read(stream);
    assert!(
        read.iter().all(|(name, _)| name == tier),
        "wanted only {tier}: {read:?}"
    );
    let total: u64 = read.iter().map(|(_, bytes)| bytes).sum();
    assert!(
        total >= bytes * 2,
        "the read route should have traced its own read: {read:?}"
    );
}

async fn catch_up_memory() {
    // The longest window: nothing seals during the test, so memory is all there is.
    let node = Node::started(NodeOptions {
        seal_window_ms: 120_000,
        ..NodeOptions::default()
    });
    let token = node.admin_token(AUTHOR);
    let (expected, last_id) = fill_for_catch_up(&node, &token, "tier.memory", 500).await;
    assert!(
        node.objects("sealed").is_empty(),
        "nothing should have sealed yet"
    );
    check_tier(&node, &token, "tier.memory", &expected, last_id, "memory").await;
    assert!(
        node.objects("sealed").is_empty(),
        "the read must have come from memory"
    );
}

/// Sealed, and stopped so that memory holds none of it.
async fn sealed_node(stream: &str) -> (Node, String, Vec<u8>, u64) {
    let mut node = Node::started(NodeOptions::default());
    let token = node.admin_token(AUTHOR);
    let (expected, last_id) = fill_for_catch_up(&node, &token, stream, 500).await;
    node.stop();
    assert!(!node.objects("sealed").is_empty(), "a clean stop seals");
    (node, token, expected, last_id)
}

async fn catch_up_disk() {
    let (mut node, token, expected, last_id) = sealed_node("tier.disk").await;
    assert!(
        node.path("data/cache").read_dir().unwrap().next().is_some(),
        "a local copy"
    );
    node.start();
    check_tier(&node, &token, "tier.disk", &expected, last_id, "local").await;
}

async fn catch_up_object_store() {
    let (mut node, token, expected, last_id) = sealed_node("tier.object").await;
    std::fs::remove_dir_all(node.path("data/cache")).unwrap();
    node.start();
    check_tier(&node, &token, "tier.object", &expected, last_id, "object").await;
}

// ---------------------------------------------------------------------------------------------
// 4. state_cas
// ---------------------------------------------------------------------------------------------

async fn state_cas() {
    let node = Node::started(NodeOptions::default());
    let token = node.admin_token(AUTHOR);
    let http = node.http(&token);
    let client = node.sdk(&token).await;
    const STREAM: &str = "state.cas";
    let path = format!("/v1/stream/{STREAM}/state");
    let change = |expect_ver, value| StateChange {
        expect_ver,
        op: StateOp::Set,
        doc: cbor_map(&[("value", value)]),
    };

    // Over WebSocket.
    let first = client.state_op(STREAM, change(0, 1)).await.unwrap();
    let second = client.state_op(STREAM, change(first, 2)).await.unwrap();
    assert!(second > first);
    let conflict = client.state_op(STREAM, change(first, 3)).await;
    assert!(
        matches!(conflict, Err(SdkError::VersionConflict { .. })),
        "{conflict:?}"
    );
    let (doc, _, ver) = http.snapshot(STREAM).await;
    assert_eq!((cbor_int(&doc, &["value"]), ver), (Some(2), second));

    // Over HTTP, and each sees the other's versions.
    let set = [("x-state-op", "set")];
    let stale = first.to_string();
    let reply = http
        .call(
            "PATCH",
            &path,
            &[set[0], ("if-match", &stale)],
            &cbor_map(&[("value", 4)]),
        )
        .await;
    assert_eq!(reply.status, 409, "{}", reply.text());
    assert_eq!(
        cbor_int(&http.snapshot(STREAM).await.0, &["value"]),
        Some(2)
    );
    let current = format!("\"{second}\"");
    let reply = http
        .call(
            "PATCH",
            &path,
            &[set[0], ("if-match", &current)],
            &cbor_map(&[("value", 5)]),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let third: u64 = json_field(&reply.text(), "ver").unwrap().parse().unwrap();
    assert!(third > second);
    assert_eq!(
        client
            .state_op(STREAM, change(third, 6))
            .await
            .map(|ver| ver > third),
        Ok(true)
    );
    assert!(client.state_op(STREAM, change(third, 7)).await.is_err());

    // Snapshot plus read: under concurrent writes, what follows the snapshot has everything else.
    let incr = |n| StateChange {
        expect_ver: 0,
        op: StateOp::Incr,
        doc: incr_doc(&["count"], n),
    };
    let base = client
        .state_op(
            STREAM,
            StateChange {
                expect_ver: 0,
                op: StateOp::Set,
                doc: cbor_map(&[("count", 0)]),
            },
        )
        .await
        .unwrap();
    let mut writers = Vec::new();
    for writer in 0..3u32 {
        let client = node.sdk(&token).await;
        writers.push(tokio::spawn(async move {
            let mut ids = Vec::new();
            for at in 0..300u32 {
                ids.push(client.state_op(STREAM, incr(1)).await.unwrap());
                if at % 16 == writer {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            ids
        }));
    }
    let mut readers = Vec::new();
    for at in 0..6u64 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        let client = node.sdk(&token).await;
        let http = node.http(&token);
        readers.push(tokio::spawn(async move {
            // Half take the snapshot in `SUBSCRIBED`, and half from the route, reading on from
            // `head` (D4). Each adds up the ops that follow, and ends on the same total.
            let (start, mut subscription) = if at % 2 == 0 {
                let mut spec = Subscribe::new(STREAM, FROM_LIVE);
                spec.with_state = true;
                spec.include_system = true;
                let mut subscription = client.subscribe(spec).unwrap();
                let (head, _, state) = until_subscribed(&mut subscription).await;
                ((head, state), subscription)
            } else {
                let (state, head, _) = http.snapshot(STREAM).await;
                let mut spec = Subscribe::new(STREAM, head);
                spec.include_system = true;
                ((head, state), client.subscribe(spec).unwrap())
            };
            let (head, state) = start;
            let mut total = cbor_int(&state, &["count"]).expect("a count");
            let mut seen = Vec::new();
            while total < 900 {
                let event = tokio::time::timeout(WAIT, subscription.next()).await;
                let event = event
                    .expect("more ops")
                    .expect("a client")
                    .expect("no error");
                if let sdk::Event::Items(items) = event {
                    for item in items {
                        assert!(item.id > head, "an op at or before the snapshot's head");
                        assert_eq!(item.author, 0);
                        total += 1;
                        seen.push(item.id);
                    }
                }
            }
            assert_eq!(total, 900, "the snapshot and the read disagree");
            (head, seen)
        }));
    }
    let mut every_id = BTreeSet::new();
    for writer in writers {
        for id in writer.await.unwrap() {
            assert!(id > base);
            assert!(every_id.insert(id), "two ops share a version");
        }
    }
    for reader in readers {
        let (head, seen) = reader.await.unwrap();
        let want: Vec<u64> = every_id.iter().copied().filter(|id| *id > head).collect();
        assert_eq!(seen, want, "a reader from {head} missed or repeated an op");
    }
    let (doc, _, ver) = http.snapshot(STREAM).await;
    assert_eq!(cbor_int(&doc, &["count"]), Some(900));
    assert_eq!(
        Some(&ver),
        every_id.last(),
        "the version is the last op's id"
    );
}

// ---------------------------------------------------------------------------------------------
// 5. atomic_publish
// ---------------------------------------------------------------------------------------------

async fn atomic_publish() {
    let node = Node::started(NodeOptions::default());
    let token = node.admin_token(AUTHOR);
    let http = node.http(&token);
    const STREAM: &str = "atomic.one";
    let client = node.sdk(&token).await;
    let start = client
        .state_op(
            STREAM,
            StateChange {
                expect_ver: 0,
                op: StateOp::Set,
                doc: cbor_map(&[("n", 0)]),
            },
        )
        .await
        .unwrap();

    let mut watcher = node.ws(&token).await;
    watcher.subscribe(1, STREAM, FROM_LIVE, true).await;
    assert!(matches!(watcher.recv().await, Owned::Subscribed { .. }));

    let mut publisher = client
        .publisher(STREAM, raw_publisher(AckLevel::WalFsync))
        .unwrap();
    publisher.add(1, b"first".to_vec());
    publisher.add(2, b"second".to_vec());
    let waits = publisher
        .send_with_state(StateChange {
            expect_ver: start,
            op: StateOp::Set,
            doc: cbor_map(&[("n", 1)]),
        })
        .await
        .unwrap();
    let ack = waits.into_iter().next().unwrap().wait().await.unwrap();
    assert_eq!(ack.count, 3, "two records and the state change");
    let state_id = ack.first_id + 2;

    // A subscriber sees all three together, with the ids consecutive and the state last (D5).
    let mut seen = Vec::new();
    while seen
        .last()
        .is_none_or(|last: &Stored| last.last_id() < state_id)
    {
        match watcher.recv().await {
            Owned::Data { records, .. } => seen.extend(parse_records(&records)),
            Owned::Progress { .. } => {}
            other => panic!("{other:?}"),
        }
    }
    let ids: Vec<u64> = seen.iter().map(|one| one.id).collect();
    assert_eq!(ids, [ack.first_id, ack.first_id + 1, state_id]);
    assert_eq!((seen[2].author, seen[2].options), (0, 1));
    let (doc, _, ver) = http.snapshot(STREAM).await;
    assert_eq!((cbor_int(&doc, &["n"]), ver), (Some(1), state_id));
    let before_read = http.read_all(STREAM).await;
    assert_eq!(before_read.len(), 2, "a read holds the records");
    let before_info = http.info(STREAM).await;

    // A conflict commits nothing, for a subscriber and for a read.
    publisher.add(3, b"never".to_vec());
    let waits = publisher
        .send_with_state(StateChange {
            expect_ver: start,
            op: StateOp::Set,
            doc: cbor_map(&[("n", 2)]),
        })
        .await
        .unwrap();
    let refused = waits.into_iter().next().unwrap().wait().await;
    assert!(
        matches!(refused, Err(SdkError::VersionConflict { .. })),
        "{refused:?}"
    );
    assert_eq!(
        http.info(STREAM).await,
        before_info,
        "the head and the version stay"
    );
    assert_eq!(http.read_all(STREAM).await, before_read);
    let (doc, _, _) = http.snapshot(STREAM).await;
    assert_eq!(cbor_int(&doc, &["n"]), Some(1));

    // A later good one is what the watcher sees next, with nothing of the refused one before it.
    publisher.add(4, b"after".to_vec());
    let waits = publisher
        .send_with_state(StateChange {
            expect_ver: state_id,
            op: StateOp::Set,
            doc: cbor_map(&[("n", 3)]),
        })
        .await
        .unwrap();
    let ack = waits.into_iter().next().unwrap().wait().await.unwrap();
    let mut next = Vec::new();
    while next.len() < 2 {
        match watcher.recv().await {
            Owned::Data { records, .. } => next.extend(parse_records(&records)),
            Owned::Progress { .. } => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(next[0].data, b"after");
    assert_eq!(next[0].id, ack.first_id);
    assert_eq!(next[1].id, ack.first_id + 1);
    assert_eq!(next[1].author, 0);
    let all = http.read_all(STREAM).await;
    assert!(all.iter().all(|one| one.data != b"never"));
    assert_eq!(all.len(), 3);
}

// ---------------------------------------------------------------------------------------------
// 6. token_rejection
// ---------------------------------------------------------------------------------------------

/// Everything a token could try on a socket that has said `HELLO`. Each must get error 1.
async fn refused_on_websocket(ws: &mut Ws, stream: &str, what: &str, ops: &[&str]) {
    let doc = cbor_map(&[("bad", 1)]);
    let sent = record(1, 0, b"must not land");
    for op in ops {
        match *op {
            "subscribe" => {
                ws.subscribe(7, stream, 0, false).await;
                match ws.recv().await {
                    Owned::Error {
                        reference: 7,
                        code: ErrorCode::Unauthorized,
                    } => {}
                    other => panic!("{what}: subscribe got {other:?}"),
                }
            }
            "publish" => {
                ws.open(9, stream).await;
                ws.publish(&PublishFrame {
                    handle: 9,
                    producer_id: 1,
                    seq: 1,
                    ack: AckLevel::WalFsync,
                    state: None,
                    records: &sent,
                })
                .await;
                match ws.recv().await {
                    Owned::Nack {
                        code: ErrorCode::Unauthorized,
                        ..
                    }
                    | Owned::Error {
                        code: ErrorCode::Unauthorized,
                        ..
                    } => {}
                    other => panic!("{what}: publish got {other:?}"),
                }
            }
            "state" => {
                ws.open(10, stream).await;
                ws.send(&ClientFrame::StateOp {
                    handle: 10,
                    req: 11,
                    expect_ver: 0,
                    op: StateOp::Set,
                    doc: &doc,
                })
                .await;
                match ws.recv().await {
                    Owned::Error {
                        code: ErrorCode::Unauthorized,
                        ..
                    } => {}
                    other => panic!("{what}: state op got {other:?}"),
                }
            }
            "publish_state" => {
                ws.open(12, stream).await;
                ws.publish(&PublishFrame {
                    handle: 12,
                    producer_id: 2,
                    seq: 1,
                    ack: AckLevel::WalFsync,
                    state: Some((0, StateOp::Set, &doc)),
                    records: &sent,
                })
                .await;
                match ws.recv().await {
                    Owned::Nack {
                        code: ErrorCode::Unauthorized,
                        ..
                    }
                    | Owned::Error {
                        code: ErrorCode::Unauthorized,
                        ..
                    } => {}
                    other => panic!("{what}: publish with state got {other:?}"),
                }
            }
            other => panic!("{other}?"),
        }
    }
}

async fn refused_on_http(http: &Http, stream: &str, what: &str, ops: &[&str]) {
    let doc = cbor_map(&[("bad", 1)]);
    let base = format!("/v1/stream/{stream}");
    for op in ops {
        let reply = match *op {
            "info" => http.call("GET", &base, &[], b"").await,
            "read" => {
                http.call("GET", &format!("{base}/read?from=0"), &[], b"")
                    .await
            }
            "state_get" => http.call("GET", &format!("{base}/state"), &[], b"").await,
            "cursor" => {
                http.call("PUT", &format!("{base}/cursor/c"), &[], b"1")
                    .await
            }
            "publish" => {
                http.call(
                    "POST",
                    &format!("{base}/publish?ack=2"),
                    &[],
                    b"must not land",
                )
                .await
            }
            "state" => {
                http.call(
                    "PATCH",
                    &format!("{base}/state"),
                    &[("x-state-op", "set")],
                    &doc,
                )
                .await
            }
            "config" => http.call("PUT", &base, &[], br#"{"retention":"7d"}"#).await,
            "delete" => http.call("DELETE", &base, &[], b"").await,
            "sink" => {
                let body = br#"{"kind":"forward","target":"auth.elsewhere","start":"earliest"}"#;
                http.call("PUT", &format!("{base}/sink/4"), &[], body).await
            }
            other => panic!("{other}?"),
        };
        assert!(
            reply.status == 401 || reply.status == 403,
            "{what}: {op} on {stream} got {}: {}",
            reply.status,
            reply.text()
        );
    }
}

async fn token_rejection() {
    let node = Node::started(NodeOptions::default());
    let admin = node.admin_token(ADMIN_AUTHOR);
    let admin_http = node.http(&admin);
    const STREAM: &str = "auth.target";
    const NEVER: &str = "auth.never";
    let first_id = admin_http
        .publish_records(STREAM, &record(1, 0, b"the only record"))
        .await;
    node.cli(&admin, &["stream", "config", STREAM, "--retention", "1d"]);
    let before = (
        admin_http.info(STREAM).await,
        admin_http.read_bytes(STREAM, 0, None).await,
        admin_http.snapshot(STREAM).await,
    );
    let everything = format!("*:{ALL_PERMS}");
    let endpoint = node.endpoint();

    // A token that isn't good stops at `HELLO`, and nothing else works on that socket either.
    let bad_signature = node.token_signed_wrongly(AUTHOR, &[&everything]);
    let expired = node.token(AUTHOR, &[&everything], 0);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut garbled = node.token(AUTHOR, &[&everything], 3_600_000);
    let last = garbled.pop().unwrap();
    garbled.push(if last == 'A' { 'B' } else { 'A' });
    let all_http = [
        "info",
        "read",
        "state_get",
        "cursor",
        "publish",
        "state",
        "config",
        "delete",
        "sink",
    ];
    for (what, token) in [
        ("bad signature", &bad_signature),
        ("expired", &expired),
        ("garbled", &garbled),
    ] {
        match endpoint.ws(token).await {
            Err(Owned::Error {
                code: ErrorCode::Unauthorized,
                ..
            }) => {}
            Err(other) => panic!("{what}: HELLO got {other:?}"),
            Ok(_) => panic!("{what}: HELLO got WELCOME"),
        }
        let http = node.http(token);
        refused_on_http(&http, STREAM, what, &all_http).await;
        refused_on_http(&http, NEVER, what, &all_http).await;
        // The SDK says the same.
        let connected =
            sdk::Client::connect(sdk::ClientConfig::new(&node.url(), &node.ca_pem, token)).await;
        assert!(connected.is_err(), "{what}: the SDK connected");
    }

    // Good tokens, for another stream. `auth.target` must not fall under them.
    let wrong_stream = node.token(
        AUTHOR,
        &[
            &format!("other.*:{ALL_PERMS}"),
            &format!("auth.targetx:{ALL_PERMS}"),
        ],
        3_600_000,
    );
    let mut ws = node.ws(&wrong_stream).await;
    refused_on_websocket(
        &mut ws,
        STREAM,
        "wrong stream",
        &["subscribe", "publish", "state", "publish_state"],
    )
    .await;
    refused_on_websocket(
        &mut ws,
        NEVER,
        "wrong stream",
        &["subscribe", "publish", "state", "publish_state"],
    )
    .await;
    let http = node.http(&wrong_stream);
    refused_on_http(&http, STREAM, "wrong stream", &all_http).await;
    refused_on_http(&http, NEVER, "wrong stream", &all_http).await;

    // Each permission alone allows only itself (section 12).
    let cases: [(&str, &[&str], &[&str]); 4] = [
        (
            "read",
            &["publish", "state", "publish_state"],
            &["publish", "state", "config", "delete", "sink"],
        ),
        (
            "publish",
            &["subscribe", "state", "publish_state"],
            &[
                "info",
                "read",
                "state_get",
                "cursor",
                "state",
                "config",
                "delete",
                "sink",
            ],
        ),
        (
            "state",
            &["subscribe", "publish"],
            &[
                "info",
                "read",
                "state_get",
                "cursor",
                "publish",
                "config",
                "delete",
                "sink",
            ],
        ),
        (
            "admin",
            &["subscribe", "publish", "state"],
            &["info", "read", "state_get", "cursor", "publish", "state"],
        ),
    ];
    for (perm, ws_ops, http_ops) in cases {
        let token = node.token(AUTHOR, &[&format!("auth.*:{perm}")], 3_600_000);
        let what = format!("only {perm}");
        let mut ws = node.ws(&token).await;
        refused_on_websocket(&mut ws, STREAM, &what, ws_ops).await;
        refused_on_http(&node.http(&token), STREAM, &what, http_ops).await;
    }

    // No side effects: not the records, ids, state or config, and no stream or cursor came of it.
    let after = (
        admin_http.info(STREAM).await,
        admin_http.read_bytes(STREAM, 0, None).await,
        admin_http.snapshot(STREAM).await,
    );
    assert_eq!(after, before, "a refused request changed something");
    assert_eq!(
        admin_http.info(NEVER).await,
        None,
        "a refused request made a stream"
    );
    assert_eq!(admin_http.info("auth.elsewhere").await, None);
    let info = admin_http
        .call("GET", &format!("/v1/stream/{STREAM}"), &[], b"")
        .await
        .text();
    assert!(info.contains("\"sinks\":[]"), "{info}");
    assert!(info.contains("\"retention\":\"1d\""), "{info}");
    // A commit of 1 reads back nothing higher, so no refused one stored `first_id`.
    let cursor = admin_http
        .call("PUT", &format!("/v1/stream/{STREAM}/cursor/c"), &[], b"1")
        .await;
    assert_eq!(
        json_field(&cursor.text(), "id").as_deref(),
        Some("1"),
        "{}",
        cursor.text()
    );
    assert!(first_id > 1);

    // The good token still works, so the refusals were about the tokens.
    assert!(
        admin_http
            .publish_records(STREAM, &record(1, 0, b"fine"))
            .await
            > first_id
    );
}

// ---------------------------------------------------------------------------------------------
// 7. kill9_mid_load
// ---------------------------------------------------------------------------------------------

async fn kill9_mid_load() {
    const ROUNDS: u32 = 6;
    const PRODUCER_COUNT: u64 = 6;
    let mut rng = Rng::seeded("kill9_mid_load");
    let mut node = Node::started(NodeOptions {
        cores: 4,
        ..NodeOptions::default()
    });
    let stream_of = |producer: u64| format!("crash.{}", producer % 3);
    let author_of = |producer: u64| 100 + producer;
    let mut producers = Vec::new();
    for producer in 0..PRODUCER_COUNT {
        producers.push(Producer::start(ProducerSpec {
            endpoint: node.endpoint(),
            token: node.token(author_of(producer), &[&format!("*:{ALL_PERMS}")], 3_600_000),
            stream: stream_of(producer),
            producer_id: 1_000 + producer,
            count: None,
            window: 8,
            state_every: Some(4),
            pace: Duration::from_micros(500),
            options_of: |seq| seq % 5,
        }));
    }
    let admin = node.admin_token(ADMIN_AUTHOR);
    let check_order = async |node: &Node, when: String| {
        let http = node.http(&admin);
        for stream in 0..3 {
            let records = http.read_all(&format!("crash.{stream}")).await;
            let ids: Vec<u64> = records.iter().map(|one| one.id).collect();
            assert!(
                ids.windows(2).all(|pair| pair[0] < pair[1]),
                "{when}: ids in crash.{stream}"
            );
            let payloads: HashSet<&Vec<u8>> = records.iter().map(|one| &one.data).collect();
            assert_eq!(
                payloads.len(),
                records.len(),
                "{when}: a record twice in crash.{stream}"
            );
        }
    };
    let mut acked_at_kill = 0;
    for round in 0..ROUNDS {
        tokio::time::sleep(Duration::from_millis(200 + rng.below(1_800))).await;
        let acked: usize = producers.iter().map(Producer::acked_count).sum();
        println!("    round {round}: kill -9 with {acked} acked");
        node.kill9();
        tokio::time::sleep(Duration::from_millis(rng.below(300))).await;
        node.start();
        // Only what was acked by the kill is sure to exist; the load carries on regardless.
        wait_until("producers to resume", async || {
            producers.iter().map(Producer::acked_count).sum::<usize>() > acked
        })
        .await;
        check_order(&node, format!("round {round}")).await;
        assert!(
            acked > acked_at_kill || round == 0,
            "no progress in round {round}"
        );
        acked_at_kill = acked;
    }
    let mut published: BTreeMap<u64, Vec<Published>> = BTreeMap::new();
    for (producer, running) in producers.into_iter().enumerate() {
        published.insert(producer as u64, running.finish().await);
    }

    // Once with the load done, and once more after one more crash, which must change nothing.
    let mut first_pass = None;
    for pass in 0..2 {
        let http = node.http(&admin);
        let mut stored = Vec::new();
        for stream in 0..3 {
            let name = format!("crash.{stream}");
            let (head, state_ver) = http.info(&name).await.expect("the stream exists");
            let records = http.read_all(&name).await;
            let mut ws = node.ws(&admin).await;
            let with_system = parse_records(&ws.read_through(1, &name, 0, true, head).await);
            let system: Vec<&Stored> = with_system.iter().filter(|one| one.author == 0).collect();
            let plain: Vec<Stored> = with_system
                .iter()
                .filter(|one| one.author != 0)
                .cloned()
                .collect();
            assert_eq!(
                plain, records,
                "the socket and the read route differ on {name}"
            );

            let by_payload: HashMap<&[u8], &Stored> = records
                .iter()
                .map(|one| (one.data.as_slice(), one))
                .collect();
            assert_eq!(by_payload.len(), records.len(), "a record twice in {name}");
            assert!(
                with_system
                    .windows(2)
                    .all(|pair| pair[0].last_id() < pair[1].id),
                "ids in {name}"
            );

            let mut state_counts = Vec::new();
            let mut expected_count = 0;
            for (producer, acked) in &published {
                if stream_of(*producer) != name {
                    continue;
                }
                expected_count += acked.len();
                let seqs: Vec<u64> = acked.iter().map(|one| one.seq).collect();
                assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<u64>>());
                for one in acked {
                    let found = by_payload.get(one.payload.as_slice()).unwrap_or_else(|| {
                        panic!(
                            "acked record {} is missing from {name}",
                            String::from_utf8_lossy(&one.payload)
                        )
                    });
                    assert_eq!(found.author, author_of(*producer));
                    assert_eq!(found.options, one.options);
                    if one.first_id != 0 {
                        assert_eq!(
                            found.id, one.first_id,
                            "an ack's id isn't where the record is"
                        );
                    }
                }
                state_counts.push((*producer, acked.iter().filter(|one| one.has_state).count()));
            }
            // Nothing is there that wasn't published: all of it was retried to an ack.
            assert_eq!(records.len(), expected_count, "records in {name}");

            // State matches its operations, each exactly once, and each right after its records.
            let (doc, _, ver) = http.snapshot(&name).await;
            assert_eq!(ver, state_ver);
            assert_eq!(
                Some(ver),
                system.last().map(|one| one.id),
                "version of {name}"
            );
            let mut ops = 0;
            for (producer, count) in state_counts {
                let key = (1_000 + producer).to_string();
                assert_eq!(
                    cbor_int(&doc, &["by", &key]),
                    (count > 0).then_some(count as i64),
                    "state of producer {producer} in {name}"
                );
                ops += count;
                let actor = author_of(producer).to_le_bytes();
                let logged = system.iter().filter(|one| one.data[..8] == actor).count();
                assert_eq!(logged, count, "system records of producer {producer}");
            }
            assert_eq!(system.len(), ops, "system records in {name}");
            for op in &system {
                let before = with_system.iter().find(|one| one.last_id() + 1 == op.id);
                assert!(
                    before.is_some_and(|one| one.author != 0),
                    "the state op at {} didn't commit with a record",
                    op.id
                );
            }
            stored.push((with_system, doc, ver));
        }
        match &first_pass {
            None => first_pass = Some(stored),
            Some(first) => assert_eq!(first, &stored, "a crash with nothing running changed data"),
        }
        if pass == 0 {
            node.kill9();
            node.start();
        }
    }
    let total: usize = published.values().map(Vec::len).sum();
    println!("    {total} publishes acked across {ROUNDS} kills");
    assert!(total > 100, "too little load to mean anything: {total}");
}

// ---------------------------------------------------------------------------------------------
// 8. sink_webhook_outage
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Delivery {
    sink_id: u64,
    first_id: u64,
    last_id: u64,
    body: Vec<u8>,
}

/// A webhook endpoint that can go away. Everything it answered 200 to stays in `seen`.
struct Receiver {
    port: u16,
    secret: &'static str,
    seen: Arc<Mutex<Vec<Delivery>>>,
    bad_signature_count: Arc<Mutex<u64>>,
    tasks: tokio::task::JoinSet<()>,
}

impl Receiver {
    async fn up(port: u16, secret: &'static str) -> Receiver {
        let mut receiver = Receiver {
            port,
            secret,
            seen: Arc::default(),
            bad_signature_count: Arc::default(),
            tasks: tokio::task::JoinSet::new(),
        };
        receiver.come_back().await;
        receiver
    }

    async fn come_back(&mut self) {
        // Its old port, which can take a moment to come free.
        let started = Instant::now();
        let listener = loop {
            match TcpListener::bind(("127.0.0.1", self.port)).await {
                Ok(listener) => break listener,
                Err(error) => {
                    assert!(started.elapsed() < WAIT, "binding the receiver: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        self.port = listener.local_addr().unwrap().port();
        let (seen, bad, secret) = (
            Arc::clone(&self.seen),
            Arc::clone(&self.bad_signature_count),
            self.secret,
        );
        self.tasks.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            while let Ok((socket, _)) = listener.accept().await {
                connections.spawn(serve_delivery(
                    socket,
                    Arc::clone(&seen),
                    Arc::clone(&bad),
                    secret,
                ));
            }
        });
    }

    /// Stops listening and drops anything in progress, as a dead host would.
    fn go_down(&mut self) {
        self.tasks.abort_all();
    }
}

async fn serve_delivery(
    mut socket: tokio::net::TcpStream,
    seen: Arc<Mutex<Vec<Delivery>>>,
    bad: Arc<Mutex<u64>>,
    secret: &'static str,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
        match socket.read(&mut chunk).await {
            Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
            _ => return,
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let headers: HashMap<String, String> = head
        .split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let len: usize = headers["content-length"].parse().unwrap();
    while buffer.len() < head_end + len {
        match socket.read(&mut chunk).await {
            Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
            _ => return,
        }
    }
    let body = buffer[head_end..head_end + len].to_vec();
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(&body);
    let signature: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if headers.get("x-signature") != Some(&signature) {
        *bad.lock().unwrap() += 1;
        let _ = socket
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await;
        return;
    }
    let delivery = Delivery {
        sink_id: headers["x-sink-id"].parse().unwrap(),
        first_id: headers["x-first-id"].parse().unwrap(),
        last_id: headers["x-last-id"].parse().unwrap(),
        body,
    };
    seen.lock().unwrap().push(delivery);
    let _ = socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await;
}

async fn sink_webhook_outage() {
    const STREAM: &str = "hook.source";
    const SINK_ID: u64 = 21;
    const COUNT: u64 = 400;
    let mut node = Node::started(NodeOptions::default());
    let admin = node.admin_token(ADMIN_AUTHOR);
    let mut receiver = Receiver::up(0, "the sink's key").await;
    node.cli(&admin, &["stream", "create", STREAM]);
    let url = format!("http://127.0.0.1:{}/hook", receiver.port);
    node.cli(
        &admin,
        &[
            "sink",
            "create",
            STREAM,
            &SINK_ID.to_string(),
            "--kind",
            "webhook.deliver",
            "--sink-url",
            &url,
            "--secret",
            "the sink's key",
            "--filter",
            "eq:1:1",
            "--start",
            "earliest",
        ],
    );
    let producer = Producer::start(ProducerSpec {
        endpoint: node.endpoint(),
        token: node.token(AUTHOR, &[&format!("*:{ALL_PERMS}")], 3_600_000),
        stream: STREAM.to_owned(),
        producer_id: 44,
        count: Some(COUNT),
        window: 4,
        state_every: None,
        pace: Duration::from_millis(10),
        options_of: |seq| seq,
    });

    // Delivering, then the receiver goes, mid-stream, with the load still coming.
    let seen = Arc::clone(&receiver.seen);
    wait_until("the first deliveries", async || {
        seen.lock().unwrap().len() >= 10
    })
    .await;
    receiver.go_down();
    let at_outage = seen.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    // The server dies during the outage, with the cursor only as far as the last seal saved it.
    node.kill9();
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.start();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(
        seen.lock().unwrap().len(),
        at_outage,
        "delivered with the receiver down?"
    );
    receiver.come_back().await;

    let acked = producer.finish().await;
    assert_eq!(acked.len() as u64, COUNT);
    let want_count = acked.iter().filter(|one| one.options & 1 == 1).count();
    let http = node.http(&admin);
    let stored = http.read_all(STREAM).await;
    assert_eq!(stored.len() as u64, COUNT);
    let want: Vec<&Stored> = stored.iter().filter(|one| one.options & 1 == 1).collect();
    assert_eq!(want.len(), want_count);
    let want_last = want.last().unwrap().id;
    wait_until("the last record to be delivered", async || {
        seen.lock()
            .unwrap()
            .iter()
            .any(|one| one.last_id >= want_last)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let deliveries = seen.lock().unwrap().clone();
    assert_eq!(
        *receiver.bad_signature_count.lock().unwrap(),
        0,
        "every signature verifies"
    );
    // At-least-once: a repeat is the very bytes again. New ids come only in full id order.
    let mut bytes_of: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut first_time = Vec::new();
    let mut repeats = 0;
    for delivery in &deliveries {
        assert_eq!(delivery.sink_id, SINK_ID);
        let records = parse_records(&delivery.body);
        assert_eq!(records.first().unwrap().id, delivery.first_id);
        assert_eq!(records.last().unwrap().last_id(), delivery.last_id);
        for one in records {
            match bytes_of.get(&one.id) {
                Some(bytes) => {
                    assert_eq!(*bytes, one.bytes, "a repeat of {} differs", one.id);
                    repeats += 1;
                }
                None => {
                    first_time.push(one.id);
                    bytes_of.insert(one.id, one.bytes);
                }
            }
        }
    }
    let want_ids: Vec<u64> = want.iter().map(|one| one.id).collect();
    assert_eq!(
        first_time, want_ids,
        "every matching record, in id order, and nothing else"
    );
    for one in &want {
        assert_eq!(
            bytes_of[&one.id], one.bytes,
            "a delivered record differs from the stored one"
        );
    }
    println!(
        "    {} deliveries, {repeats} repeated records",
        deliveries.len()
    );
    assert!(node.is_running());
}

// ---------------------------------------------------------------------------------------------
// 9. sink_forward
// ---------------------------------------------------------------------------------------------

async fn sink_forward() {
    const SOURCE: &str = "fwd.source";
    const TARGET: &str = "fwd.target";
    const COUNT: u64 = 300;
    let mut rng = Rng::seeded("sink_forward");
    let mut node = Node::started(NodeOptions::default());
    let admin = node.admin_token(ADMIN_AUTHOR);
    node.cli(&admin, &["stream", "create", SOURCE]);
    node.cli(
        &admin,
        &[
            "sink", "create", SOURCE, "31", "--kind", "forward", "--target", TARGET, "--start",
            "earliest",
        ],
    );
    let mut producers = Vec::new();
    for (producer_id, author) in [(61u64, 11u64), (62, 22)] {
        producers.push(Producer::start(ProducerSpec {
            endpoint: node.endpoint(),
            token: node.token(author, &[&format!("{SOURCE}:publish,state")], 3_600_000),
            stream: SOURCE.to_owned(),
            producer_id,
            count: Some(COUNT),
            window: 4,
            state_every: Some(10),
            pace: Duration::from_millis(5),
            options_of: |seq| seq,
        }));
    }
    for round in 0..4 {
        tokio::time::sleep(Duration::from_millis(300 + rng.below(900))).await;
        println!("    round {round}: restart with kill -9");
        node.kill9();
        node.start();
    }
    for producer in producers {
        assert_eq!(producer.finish().await.len() as u64, COUNT);
    }
    // One more restart with forwarding probably mid-way, a clean one this time.
    node.stop();
    node.start();

    let http = node.http(&admin);
    let source = http.read_all(SOURCE).await;
    assert_eq!(source.len() as u64, COUNT * 2);
    wait_until("the target to fill", async || {
        http.info(TARGET).await.is_some() && http.read_all(TARGET).await.len() >= source.len()
    })
    .await;
    // Anything more would be a duplicate, so give the sink time to send one, past a restart too.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    node.kill9();
    node.start();
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let http = node.http(&admin);
    let target = http.read_all(TARGET).await;
    let key = |one: &Stored| (one.author, one.options, one.data.clone());
    let want: Vec<_> = source.iter().map(key).collect();
    let got: Vec<_> = target.iter().map(key).collect();
    assert_eq!(
        got.len(),
        want.len(),
        "target has {} of {}",
        got.len(),
        want.len()
    );
    assert_eq!(
        got, want,
        "each record once, in source order, with its original author"
    );
    assert!(source.iter().any(|one| one.author == 11) && source.iter().any(|one| one.author == 22));
    assert!(target.windows(2).all(|pair| pair[0].id < pair[1].id));
    // System records stay behind (D28).
    let (doc, _, ver) = http.snapshot(TARGET).await;
    assert!(
        ver == 0 && doc.is_empty(),
        "state leaked into the target: {ver}"
    );
    let info = http
        .call("GET", &format!("/v1/stream/{SOURCE}"), &[], b"")
        .await
        .text();
    assert!(info.contains("\"pause_reason\":null"), "{info}");
}

// ---------------------------------------------------------------------------------------------
// 10. retention_and_compaction
// ---------------------------------------------------------------------------------------------

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

const EXPIRE_PERIOD_MS: u64 = 200;
const EXPIRE_PERIOD_SLOW_MS: u64 = 1_000;

fn retention_args(expire_period_ms: u64) -> Vec<String> {
    [
        "--compact-period-ms",
        "200",
        "--compact-merge-wait-ms",
        "0",
        "--expire-period-ms",
        &expire_period_ms.to_string(),
    ]
    .iter()
    .map(|text| (*text).to_owned())
    .collect()
}

fn retention_options() -> NodeOptions {
    NodeOptions {
        hooks: true,
        extra: retention_args(EXPIRE_PERIOD_MS),
        ..NodeOptions::default()
    }
}

/// Watches each hour prefix of `before` go, and fails if the first object and the last of one
/// prefix go further apart than a pass of the delete job, or if a prefix never goes.
async fn watch_prefixes_go(node: &Node, before: &HashSet<String>) {
    let mut left = hours(before);
    let mut first_gone: HashMap<String, Instant> = HashMap::new();
    let started = Instant::now();
    while !left.is_empty() {
        assert!(
            started.elapsed() < WAIT,
            "these never expired: {:?}",
            left.keys()
        );
        let now_there = node.objects("sealed");
        left.retain(|prefix, files| {
            let there = files
                .iter()
                .filter(|file| now_there.contains(*file))
                .count();
            if there < files.len() {
                let first = *first_gone
                    .entry(prefix.clone())
                    .or_insert_with(Instant::now);
                let span_ms = first.elapsed().as_millis() as u64;
                assert!(
                    span_ms < EXPIRE_PERIOD_SLOW_MS,
                    "{prefix} still holds {there} of {} objects {span_ms} ms after it began to go",
                    files.len()
                );
            }
            there > 0
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

struct RetentionFill {
    admin: String,
    week: Vec<Stored>,
    day_head: u64,
    week_head: u64,
    state_ver: u64,
    cursor: u64,
}

async fn fill_two_classes(node: &Node) -> RetentionFill {
    let admin = node.admin_token(ADMIN_AUTHOR);
    node.cli(
        &admin,
        &["stream", "create", "keep.week", "--retention", "7d"],
    );
    node.cli(
        &admin,
        &["stream", "create", "keep.day", "--retention", "1d"],
    );
    let http = node.http(&admin);
    for at in 0..50u64 {
        let data = format!("record {at}").into_bytes();
        http.publish_records("keep.week", &record(at, 0, &data))
            .await;
        http.publish_records("keep.day", &record(at, 0, &data))
            .await;
    }
    let client = node.sdk(&admin).await;
    let state_ver = client
        .state_op(
            "keep.day",
            StateChange {
                expect_ver: 0,
                op: StateOp::Set,
                doc: cbor_map(&[("kept", 1)]),
            },
        )
        .await
        .unwrap();
    let day = http.read_all("keep.day").await;
    let week = http.read_all("keep.week").await;
    assert_eq!((day.len(), week.len()), (50, 50));
    let cursor = day[20].id;
    let reply = http
        .call(
            "PUT",
            "/v1/stream/keep.day/cursor/c",
            &[],
            cursor.to_string().as_bytes(),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let (day_head, _) = http.info("keep.day").await.unwrap();
    let (week_head, _) = http.info("keep.week").await.unwrap();
    RetentionFill {
        admin,
        week,
        day_head,
        week_head,
        state_ver,
        cursor,
    }
}

/// Everything about a `1d` stream that outlives its records, and that ids still go up.
async fn check_outlives_records(node: &Node, fill: &RetentionFill) {
    let http = node.http(&fill.admin);
    let (head, state_ver) = http
        .info("keep.day")
        .await
        .expect("the stream outlives its records");
    assert_eq!(
        (head, state_ver),
        (fill.day_head, fill.state_ver),
        "the head and the version remain"
    );
    let (doc, _, _) = http.snapshot("keep.day").await;
    assert_eq!(cbor_int(&doc, &["kept"]), Some(1), "state remains");
    // A lower commit is ignored, and answers with where the cursor is.
    let reply = http
        .call("PUT", "/v1/stream/keep.day/cursor/c", &[], b"1")
        .await;
    assert_eq!(
        json_field(&reply.text(), "id"),
        Some(fill.cursor.to_string()),
        "the cursor remains"
    );
    let info = http
        .call("GET", "/v1/stream/keep.day", &[], b"")
        .await
        .text();
    assert!(
        info.contains("\"retention\":\"1d\""),
        "config remains: {info}"
    );
}

/// Maps each `<root>/<hour>` to its files.
fn hours(files: &HashSet<String>) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for file in files {
        let mut parts = file.split('/');
        let prefix = format!("{}/{}", parts.next().unwrap(), parts.next().unwrap());
        out.entry(prefix).or_default().insert(file.clone());
    }
    out
}

/// Each hour prefix there was is now wholly there or wholly gone, as far as it was then.
fn assert_only_whole_prefixes(before: &HashSet<String>, after: &HashSet<String>, what: &str) {
    for (prefix, files) in hours(before) {
        let left = files.iter().filter(|file| after.contains(*file)).count();
        assert!(
            left == 0 || left == files.len(),
            "{what}: {prefix} lost {} of its {} objects",
            files.len() - left,
            files.len()
        );
    }
}

async fn retention_and_compaction() {
    // First, with the compactor free to work.
    let mut node = Node::started(retention_options());
    let fill = fill_two_classes(&node).await;
    // A record of the 7d class only gets to `7d/` through a seal and a compaction.
    wait_until("compaction to `7d/`", async || {
        !node.objects("7d").is_empty()
    })
    .await;
    let http = node.http(&fill.admin);
    let week_bytes: Vec<u8> = fill.week.iter().flat_map(|one| one.bytes.clone()).collect();
    assert_eq!(http.read_bytes("keep.week", 0, None).await, week_bytes);
    // More seals, so that some hour prefix holds several objects: two hours at most in this run.
    wait_until("several objects in `sealed/`", async || {
        http.publish_records("keep.pad", &record(1, 0, b"pad"))
            .await;
        node.objects("sealed").len() >= 4
    })
    .await;
    node.stop();
    let sealed_before = node.objects("sealed");
    let week_objects = node.objects("7d");
    assert!(
        hours(&sealed_before).values().any(|files| files.len() >= 2),
        "no prefix to delete whole: {sealed_before:?}"
    );
    assert!(
        !node.path("objects/1d").exists(),
        "the 1d class is never compacted"
    );

    // Three days on, past `sealed/`'s two. A slower delete job, because one pass of it takes a
    // prefix whole, and a job that took a prefix bit by bit would then need several seconds.
    node.clock_offset_ms = 3 * DAY_MS;
    node.options.extra = retention_args(EXPIRE_PERIOD_SLOW_MS);
    node.start();
    watch_prefixes_go(&node, &sealed_before).await;
    assert_only_whole_prefixes(&sealed_before, &node.objects("sealed"), "sealed/ at 3 days");
    assert!(
        node.objects("7d").is_superset(&week_objects),
        "7d/ has days to go"
    );
    let http = node.http(&fill.admin);
    assert_eq!(
        http.read_bytes("keep.week", 0, None).await,
        week_bytes,
        "7d data is byte-identical after sealed/ expired"
    );
    assert!(
        node.tiers_read("keep.week")
            .iter()
            .all(|(tier, _)| tier != "memory")
    );
    assert_eq!(
        http.read_all("keep.day").await,
        Vec::new(),
        "1d data is gone"
    );
    check_outlives_records(&node, &fill).await;

    // New ids stay above everything, for both.
    let new_day = http
        .publish_records("keep.day", &record(1, 0, b"after expiry"))
        .await;
    assert!(new_day > fill.day_head);
    let new_week = http
        .publish_records("keep.week", &record(1, 0, b"after expiry"))
        .await;
    assert!(new_week > fill.week_head);
    let day_after = http.read_all("keep.day").await;
    assert_eq!(day_after.len(), 1);
    assert_eq!(day_after[0].id, new_day);
    node.stop();

    // Nine days on, `7d/` goes too, whole, and nothing of the stream with it.
    let week_before = node.objects("7d");
    let sealed_now = node.objects("sealed");
    node.clock_offset_ms = 9 * DAY_MS;
    node.start();
    wait_until("the first `7d/` hour to expire", async || {
        node.objects("7d").is_disjoint(&week_objects)
    })
    .await;
    assert_only_whole_prefixes(&week_before, &node.objects("7d"), "7d/ at 9 days");
    assert_only_whole_prefixes(&sealed_now, &node.objects("sealed"), "sealed/ at 9 days");
    let http = node.http(&fill.admin);
    let week_left = http.read_all("keep.week").await;
    assert!(
        week_left.iter().all(|one| one.id > fill.week_head),
        "records past their class came back"
    );
    let (head, _) = http
        .info("keep.week")
        .await
        .expect("the stream outlives its records");
    assert!(head >= new_week);
    node.stop();

    // Second, a blocked compactor: `7d/` can't be written, so its data must stay in `sealed/`.
    let mut node = Node::new(retention_options());
    let blocked = node.path("objects/7d");
    std::fs::create_dir_all(&blocked).unwrap();
    set_writable(&blocked, false);
    node.start();
    let fill = fill_two_classes(&node).await;
    let week_bytes: Vec<u8> = fill.week.iter().flat_map(|one| one.bytes.clone()).collect();
    wait_until("the compactor to try and fail", async || {
        node.log_text().contains("compaction, will retry")
    })
    .await;
    wait_until("a first seal", async || !node.objects("sealed").is_empty()).await;
    node.stop();
    let sealed_before = node.objects("sealed");
    assert!(!sealed_before.is_empty());
    assert!(node.objects("7d").is_empty());

    node.clock_offset_ms = 3 * DAY_MS;
    node.start();
    // Many expiry periods, every one of which would have deleted this if it could.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    assert!(
        node.log_text().contains("compaction, will retry"),
        "{}",
        node.log_text()
    );
    assert!(
        node.objects("sealed").is_superset(&sealed_before),
        "a blocked compactor lost `sealed/`"
    );
    let http = node.http(&fill.admin);
    assert_eq!(
        http.read_bytes("keep.week", 0, None).await,
        week_bytes,
        "blocked, and 7d is readable"
    );

    // Unblocked, it catches up, and only then does `sealed/` go.
    set_writable(&blocked, true);
    wait_until("`sealed/` to expire once the compactor ran", async || {
        node.objects("sealed").is_disjoint(&sealed_before)
    })
    .await;
    assert!(!node.objects("7d").is_empty());
    assert_only_whole_prefixes(
        &sealed_before,
        &node.objects("sealed"),
        "sealed/ after the block",
    );
    let http = node.http(&fill.admin);
    assert_eq!(
        http.read_bytes("keep.week", 0, None).await,
        week_bytes,
        "7d is whole after the block"
    );
    assert_eq!(
        http.read_all("keep.day").await,
        Vec::new(),
        "1d data is gone"
    );
    check_outlives_records(&node, &fill).await;
}

fn set_writable(dir: &std::path::Path, writable: bool) {
    use std::os::unix::fs::PermissionsExt;
    let mode = if writable { 0o755 } else { 0o555 };
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
}

// ---------------------------------------------------------------------------------------------
// The normal build
// ---------------------------------------------------------------------------------------------

/// Every test above but retention runs on this build, so it had better be the one it says.
async fn normal_build_has_no_hooks() {
    let mut options = NodeOptions::default();
    options.extra.clear();
    let mut node = Node::new(options);
    node.start_with_env(&[
        ("SILLYSTREAM_CRASH_AT", "wal.before_write"),
        ("SILLYSTREAM_CLOCK_OFFSET_MS", "864000000"),
    ]);
    let token = node.admin_token(AUTHOR);
    let http = node.http(&token);
    let id = http
        .publish_records("plain.build", &record(1, 0, b"no crash point here"))
        .await;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(id >> 16 < now_ms + 60_000, "the clock moved: id {id}");
    assert!(node.is_running());
    node.stop();
}

// ---------------------------------------------------------------------------------------------
// Ids against the clock (D221)
// ---------------------------------------------------------------------------------------------

/// `record_count` empty records of 65,535 items: 32 bytes each, about 1 ms of ids each.
fn max_count_records(record_count: usize) -> Vec<u8> {
    let mut one = record(0, 0, b"");
    one[28..30].copy_from_slice(&u16::MAX.to_le_bytes());
    one.repeat(record_count)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as u64
}

/// One stream's publisher, with `publish` on that stream only, can't move its own ids without
/// limit, nor, through the restart that carries the highest id to all, anyone else's.
async fn a_publisher_cannot_skew_other_streams_through_a_restart() {
    const ATTACKER: &str = "attacker.x";
    const VICTIM: &str = "victim.y";
    const FRAME_COUNT: u64 = 200;
    /// `MAX_ID_LEAD_MS`, plus room for a slow restart's own credit.
    const LEAD_LIMIT_MS: u64 = 72_000;
    let lead_of = |id: u64| (id >> 16).saturating_sub(now_ms());
    let mut node = Node::started(NodeOptions::default());
    let admin = node.admin_token(ADMIN_AUTHOR);
    let http = node.http(&admin);
    http.publish_records(VICTIM, &record(1, 0, b"before")).await;
    node.cli(&admin, &["stream", "create", ATTACKER]);

    let token = node.token(9, &[&format!("{ATTACKER}:publish")], 3_600_000);
    let mut attacker = node.ws(&token).await;
    attacker.open(1, ATTACKER).await;
    let frame = max_count_records(65_500);
    let (mut acked, mut refused, mut highest) = (0, 0, 0);
    for seq in 1..=FRAME_COUNT {
        attacker
            .publish(&PublishFrame {
                handle: 1,
                producer_id: 9,
                seq,
                ack: AckLevel::WalFsync,
                state: None,
                records: &frame,
            })
            .await;
        match attacker.recv().await {
            Owned::Ack {
                first_id, count, ..
            } => {
                acked += 1;
                highest = first_id + u64::from(count) - 1;
            }
            Owned::Nack { code, .. } => {
                assert_eq!(code, ErrorCode::RateLimited);
                refused += 1;
            }
            other => panic!("the attacker got {other:?}"),
        }
    }
    println!("attacker: {acked} acked, {refused} refused");
    assert!(refused > 0, "nothing limited it: {acked} acked");
    assert!(
        lead_of(highest) <= LEAD_LIMIT_MS,
        "the stream itself is {} ms ahead",
        lead_of(highest)
    );

    node.kill9();
    node.start();
    let http = node.http(&admin);
    let id = http.publish_records(VICTIM, &record(1, 0, b"after")).await;
    assert!(
        lead_of(id) <= LEAD_LIMIT_MS,
        "the victim is {} ms ahead",
        lead_of(id)
    );
    node.stop();
}

// ---------------------------------------------------------------------------------------------
// What the log says
// ---------------------------------------------------------------------------------------------

/// The process goes on serving reads (D79), so its log is all that tells an operator.
async fn a_stopped_wal_says_so_on_stderr() {
    const STREAM: &str = "wal.stops";
    let node = Node::started(NodeOptions::default());
    let token = node.admin_token(AUTHOR);
    let mut ws = node.ws(&token).await;
    ws.open(1, STREAM).await;
    let wal = node.path("wal");
    let body = record(1, 0, b"x");
    let mut refused = None;
    for seq in 1..=2_000 {
        if seq == 5 {
            // A new file can't be made, which the next seal's rotation needs.
            set_writable(&wal, false);
        }
        ws.publish(&PublishFrame {
            handle: 1,
            producer_id: 5,
            seq,
            ack: AckLevel::WalFsync,
            state: None,
            records: &body,
        })
        .await;
        match ws.recv().await {
            Owned::Ack { .. } => tokio::time::sleep(Duration::from_millis(5)).await,
            other => {
                refused = Some(other);
                break;
            }
        }
    }
    let refused = refused.expect("the WAL never stopped");
    assert!(
        matches!(
            refused,
            Owned::Nack {
                code: ErrorCode::RateLimited,
                ..
            } | Owned::Error {
                code: ErrorCode::RateLimited,
                ..
            }
        ),
        "{refused:?}"
    );
    wait_until("the log to say the WAL stopped", async || {
        node.log_text().contains("the WAL writer stopped")
    })
    .await;
    assert!(node.is_running(), "reads go on until an operator acts");
    let (head, _) = node
        .http(&token)
        .info(STREAM)
        .await
        .expect("reads still work");
    assert!(head > 0);
    set_writable(&wal, true);
}

/// A stream id is the client's text, and may hold a newline (section 1).
async fn a_stream_id_cannot_forge_a_log_line() {
    const FORGED: &str = "x\ntrace catch_up stream=victim tier=memory bytes=1 ";
    /// Not even within one line can a name be taken for fields of the real ones.
    const WITHIN: &str = "y stream=\"victim\" bytes=999 tier=object";
    let mut options = NodeOptions::default();
    options
        .extra
        .extend(["--trace-catch-up".to_owned(), "true".to_owned()]);
    let node = Node::started(options);
    let token = node.admin_token(AUTHOR);
    let mut ws = node.ws(&token).await;
    for (handle, stream) in [(1, FORGED), (2, WITHIN)] {
        ws.open(handle, stream).await;
        ws.publish(&PublishFrame {
            handle,
            producer_id: 5,
            seq: 1,
            ack: AckLevel::WalFsync,
            state: None,
            records: &record(1, 0, b"x"),
        })
        .await;
        assert!(matches!(ws.recv().await, Owned::Ack { .. }));
        let sub = handle + 10;
        ws.subscribe(sub, stream, 0, false).await;
        let mut got_data = false;
        while !got_data {
            got_data = matches!(ws.recv().await, Owned::Data { .. });
        }
        wait_until("the trace of that read", async || {
            !node.tiers_read(stream).is_empty()
        })
        .await;
        let read = node.tiers_read(stream);
        assert!(read.iter().all(|(_, bytes)| *bytes != 999), "{read:?}");
    }
    assert_eq!(node.tiers_read("victim"), Vec::new());
    assert!(
        !node
            .log_text()
            .lines()
            .any(|line| line.starts_with("trace catch_up stream=victim")),
        "{}",
        node.log_text()
    );
}

// ---------------------------------------------------------------------------------------------
// What sinks can hold
// ---------------------------------------------------------------------------------------------

/// `admin` on some streams, a receiver that never answers, and sinks asking for all they can:
/// the node's default limits hold on the real binary, and another client still gets in.
async fn sinks_leave_the_node_able_to_take_connections() {
    const SINK_COUNT: usize = 6;
    const PER_SINK_MAX_COUNT: usize = 64;
    const NODE_MAX_COUNT: usize = 256;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let held = Arc::new(Mutex::new(Vec::new()));
    let holding = Arc::clone(&held);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            holding.lock().unwrap().push(socket);
        }
    });
    let node = Node::started(NodeOptions::default());
    let root = node.admin_token(ADMIN_AUTHOR);
    let token = node.token(AUTHOR, &[&format!("tenant.*:{ALL_PERMS}")], 3_600_000);
    let http = node.http(&token);
    let one = record(0, 0, b"");
    let body = format!(
        r#"{{"kind":"webhook.deliver","url":"http://127.0.0.1:{port}/","secret":"s",
             "concurrency":1000000,"start":"earliest"}}"#
    );

    http.publish_records("tenant.0", &one.repeat(PER_SINK_MAX_COUNT * 2))
        .await;
    let path = "/v1/stream/tenant.0/sink/1";
    let reply = http.call("PUT", path, &[], body.as_bytes()).await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let count = || held.lock().unwrap().len();
    wait_until("one sink's connections", async || {
        count() >= PER_SINK_MAX_COUNT
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(count(), PER_SINK_MAX_COUNT, "one sink's own limit");

    for at in 1..SINK_COUNT {
        let stream = format!("tenant.{at}");
        http.publish_records(&stream, &one.repeat(PER_SINK_MAX_COUNT * 2))
            .await;
        let path = format!("/v1/stream/{stream}/sink/1");
        let reply = http.call("PUT", &path, &[], body.as_bytes()).await;
        assert_eq!(reply.status, 200, "{}", reply.text());
    }
    wait_until("the node's connections", async || count() >= NODE_MAX_COUNT).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(count(), NODE_MAX_COUNT, "the node's limit");

    let mut other = node.ws(&root).await;
    other.send(&ClientFrame::Ping { nonce: 7 }).await;
    assert!(matches!(other.recv().await, Owned::Pong { nonce: 7 }));

    // A size no real sink needs, which once grew the server by 60 times what was sent.
    let huge = format!(
        r#"{{"kind":"webhook.deliver","url":"http://127.0.0.1:9/","secret":"{}"}}"#,
        "s".repeat(999_800)
    );
    let path = "/v1/stream/tenant.0/sink/2";
    assert_eq!(
        http.call("PUT", path, &[], huge.as_bytes()).await.status,
        400
    );
}
