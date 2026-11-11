//! The SDK against a real server over real TLS, a cuttable socket between them, and a scripted
//! server where what matters is exactly which frames arrive when.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use proto::{ClientFrame, ServerFrame, decode_client};
use sdk::http::HttpClient;
use sdk::{
    AckLevel, BatchConfig, CODEC_ZSTD, Client, ClientConfig, ErrorCode, Event, FROM_LIVE,
    FROM_START, Item, PublisherConfig, SdkError, StateChange, StateOp, Subscribe, Subscription,
    WireClause,
};
use server::net::{Server, ServerConfig};
use server::{ManualClock, NodeConfig};
use storage::DirVolume;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;

const SECRET: &[u8] = b"a test secret, thirty-two bytes or more";
const WAIT_MS: u64 = 10_000;
const START_MS: u64 = 1_800_000_000_000;

struct Fixture {
    server: Server,
    ca_pem: Vec<u8>,
    _wal: tempfile::TempDir,
}

fn node_config() -> NodeConfig {
    let mut node = NodeConfig::new(2);
    node.seal_window_ms = None;
    node.rebalance_period_ms = None;
    node.progress_interval_ms = 50;
    node
}

fn start_with(node: NodeConfig) -> Fixture {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let wal = tempfile::tempdir().unwrap();
    let ca_pem = cert.cert.pem().into_bytes();
    let server = Server::start(
        ServerConfig {
            node,
            listen: "127.0.0.1:0".parse().unwrap(),
            cert_pem: ca_pem.clone(),
            key_pem: cert.key_pair.serialize_pem().into_bytes(),
            token_secret: SECRET.to_vec(),
        },
        DirVolume::new(wal.path(), 1024 * 1024),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(ManualClock::new(START_MS)),
    )
    .unwrap();
    Fixture {
        server,
        ca_pem,
        _wal: wal,
    }
}

fn start() -> Fixture {
    start_with(node_config())
}

fn token(expiry_ms: u64) -> String {
    auth::Token {
        expiry_ms,
        author: std::num::NonZeroU64::new(9).unwrap(),
        grants: vec![auth::Grant {
            pattern: "*".to_owned(),
            perms: auth::PERM_READ | auth::PERM_PUBLISH | auth::PERM_STATE | auth::PERM_ADMIN,
        }],
    }
    .sign(SECRET)
    .unwrap()
}

impl Fixture {
    fn url(&self) -> String {
        format!("localhost:{}", self.server.local_addr().port())
    }

    fn config(&self) -> ClientConfig {
        ClientConfig::new(&self.url(), &self.ca_pem, &token(u64::MAX))
    }

    async fn client(&self) -> Client {
        Client::connect(self.config()).await.unwrap()
    }

    fn http(&self) -> HttpClient {
        HttpClient::new(&self.url(), &self.ca_pem, &token(u64::MAX)).unwrap()
    }

    /// A socket between client and server, which `cut` breaks without telling either.
    async fn cuttable(&self) -> (Client, Cutter) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let target = self.server.local_addr();
        let cutter = Cutter {
            cut: Arc::new(tokio::sync::Notify::new()),
            refuse: Arc::new(AtomicBool::new(false)),
        };
        let (cut, refuse) = (Arc::clone(&cutter.cut), Arc::clone(&cutter.refuse));
        tokio::spawn(async move {
            loop {
                let (mut near, _) = listener.accept().await.unwrap();
                if refuse.load(Ordering::SeqCst) {
                    continue;
                }
                let cut = Arc::clone(&cut);
                tokio::spawn(async move {
                    let mut far = TcpStream::connect(target).await.unwrap();
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut near, &mut far) => {}
                        _ = cut.notified() => {}
                    }
                });
            }
        });
        let config =
            ClientConfig::new(&format!("localhost:{port}"), &self.ca_pem, &token(u64::MAX));
        (Client::connect(config).await.unwrap(), cutter)
    }
}

struct Cutter {
    cut: Arc<tokio::sync::Notify>,
    refuse: Arc<AtomicBool>,
}

async fn next_event(subscription: &mut Subscription) -> Result<Event, SdkError> {
    tokio::time::timeout(Duration::from_millis(WAIT_MS), subscription.next())
        .await
        .expect("an event should come")
        .expect("the client is running")
}

/// Reads until `count` items are in.
async fn items(subscription: &mut Subscription, count: usize) -> Vec<Item> {
    let mut got = Vec::new();
    while got.len() < count {
        if let Event::Items(more) = next_event(subscription).await.unwrap() {
            got.extend(more);
        }
    }
    got
}

fn bytes_of(items: &[Item]) -> Vec<Vec<u8>> {
    items.iter().map(|item| item.bytes.clone()).collect()
}

fn doc(key: &str, value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    let value = ciborium::value::Value::Map(vec![(
        ciborium::value::Value::Text(key.to_owned()),
        ciborium::value::Value::Integer(value.into()),
    )]);
    ciborium::ser::into_writer(&value, &mut out).unwrap();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn items_round_trip_through_zstd_and_the_server_stores_what_was_sent() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client.publisher("z", PublisherConfig::default()).unwrap();
    let line = "the same log line, over and over, which compresses very well indeed. ".repeat(40);
    for index in 0..100u32 {
        publisher.add(7, format!("{index}: {line}").into_bytes());
    }
    let acks = publisher.flush().await.unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].count, 100);

    // Through the read route, as bytes: one zstd record, smaller than its items.
    let reply = fixture
        .http()
        .request("GET", "/v1/stream/z/read", &[], b"")
        .await
        .unwrap();
    assert_eq!(reply.status, 200);
    assert_eq!(
        u16::from_le_bytes([reply.body[30], reply.body[31]]),
        CODEC_ZSTD
    );
    assert_eq!(u16::from_le_bytes([reply.body[28], reply.body[29]]), 100);
    assert!(
        reply.body.len() < 100 * line.len() / 10,
        "{}",
        reply.body.len()
    );

    let mut subscription = client.subscribe(Subscribe::new("z", FROM_START)).unwrap();
    let got = items(&mut subscription, 100).await;
    for (index, item) in got.iter().enumerate() {
        assert_eq!(item.id, acks[0].first_id + index as u64);
        assert_eq!(item.bytes, format!("{index}: {line}").into_bytes());
        assert_eq!(item.options, 7);
        assert_eq!(item.author, 9, "the token's, not the client's");
    }
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn order_holds_within_options_and_across_publishes() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client.publisher("o", PublisherConfig::default()).unwrap();
    let mut expected = Vec::new();
    let mut waits = Vec::new();
    for batch in 0..50u32 {
        // Inside a publish: sorted by options, stable within each.
        let mut sorted = [Vec::new(), Vec::new(), Vec::new()];
        for index in 0..30u32 {
            let options = (index * 7 + batch) as u64 % 3;
            let bytes = format!("{batch}.{index}").into_bytes();
            publisher.add(options, bytes.clone());
            sorted[options as usize].push(bytes);
        }
        expected.extend(sorted.into_iter().flatten());
        // Not waiting: publishes are pipelined, and order still has to hold.
        waits.extend(publisher.send().await.unwrap());
    }
    let mut last = 0;
    for wait in waits {
        let ack = wait.wait().await.unwrap();
        assert!(ack.first_id > last, "ids follow the order they were sent");
        last = ack.first_id;
    }
    let mut subscription = client.subscribe(Subscribe::new("o", FROM_START)).unwrap();
    let got = items(&mut subscription, expected.len()).await;
    assert_eq!(bytes_of(&got), expected);
    assert!(got.windows(2).all(|pair| pair[0].id < pair[1].id));
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_over_the_frame_splits_and_an_item_that_cant_fit_is_refused() {
    let mut node = node_config();
    node.max_frame_bytes = 64 * 1024;
    let fixture = start_with(node);
    let client = fixture.client().await;
    assert_eq!(client.max_frame_bytes(), 64 * 1024);
    let raw = PublisherConfig {
        batch: BatchConfig {
            zstd_level: None,
            record_target_bytes: 16 * 1024,
        },
        ..PublisherConfig::default()
    };
    let mut publisher = client.publisher("big", raw).unwrap();
    for index in 0..40u8 {
        publisher.add(1, vec![index; 10 * 1024]);
    }
    let acks = publisher.flush().await.unwrap();
    assert!(acks.len() > 1, "{acks:?}");
    assert_eq!(acks.iter().map(|ack| ack.count).sum::<u32>(), 40);
    let mut subscription = client.subscribe(Subscribe::new("big", FROM_START)).unwrap();
    let got = items(&mut subscription, 40).await;
    for (index, item) in got.iter().enumerate() {
        assert_eq!(item.bytes, vec![index as u8; 10 * 1024]);
    }

    // The largest that fits goes through, and one byte over never leaves.
    publisher.add(1, vec![1u8; 64 * 1024 - 27 - 32 - 8]);
    publisher.flush().await.unwrap();
    publisher.add(1, vec![1u8; 64 * 1024]);
    assert_eq!(publisher.send().await.err(), Some(SdkError::TooLarge));
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn state_ops_and_publishes_with_state_report_conflicts() {
    let fixture = start();
    let client = fixture.client().await;
    let set = |expect_ver, value| StateChange {
        expect_ver,
        op: StateOp::Set,
        doc: doc("a", value),
    };
    let first = client.state_op("st", set(0, 1)).await.unwrap();
    let second = client.state_op("st", set(first, 2)).await.unwrap();
    assert_eq!(
        client.state_op("st", set(first, 3)).await,
        Err(SdkError::VersionConflict { ver: second })
    );

    let mut publisher = client.publisher("st", PublisherConfig::default()).unwrap();
    publisher.add(0, b"with state".to_vec());
    let acks = publisher.send_with_state(set(second, 4)).await.unwrap();
    let ack = acks.into_iter().next().unwrap().wait().await.unwrap();
    assert_eq!(ack.count, 2, "the system record of the change and the item");
    publisher.add(0, b"stale".to_vec());
    let acks = publisher.send_with_state(set(second, 5)).await.unwrap();
    let refused = acks.into_iter().next().unwrap().wait().await;
    assert!(matches!(refused, Err(SdkError::VersionConflict { .. })));

    // A snapshot and then what follows it.
    let mut spec = Subscribe::new("st", FROM_LIVE);
    spec.with_state = true;
    let mut subscription = client.subscribe(spec).unwrap();
    let Event::Subscribed {
        state, state_ver, ..
    } = next_event(&mut subscription).await.unwrap()
    else {
        panic!();
    };
    assert_eq!(state, doc("a", 4));
    assert!(state_ver > second);
    client.state_op("st", set(0, 6)).await.unwrap();
    publisher.add(0, b"after".to_vec());
    publisher.flush().await.unwrap();
    let got = items(&mut subscription, 1).await;
    assert_eq!(got[0].bytes, b"after");
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_publish_comes_back_as_its_error() {
    let fixture = start();
    let reader = auth::Token {
        expiry_ms: u64::MAX,
        author: std::num::NonZeroU64::new(9).unwrap(),
        grants: vec![auth::Grant {
            pattern: "*".to_owned(),
            perms: auth::PERM_READ,
        }],
    }
    .sign(SECRET)
    .unwrap();
    let mut config = fixture.config();
    config.token = reader;
    let client = Client::connect(config).await.unwrap();
    let mut publisher = client.publisher("no", PublisherConfig::default()).unwrap();
    publisher.add(0, b"x".to_vec());
    let error = publisher.flush().await.unwrap_err();
    assert!(
        matches!(
            &error,
            SdkError::Server {
                code: ErrorCode::Unauthorized,
                ..
            }
        ),
        "{error:?}"
    );

    let mut bad = fixture.config();
    bad.token = "nonsense".to_owned();
    assert!(matches!(
        Client::connect(bad).await.err(),
        Some(SdkError::Server {
            code: ErrorCode::Unauthorized,
            ..
        })
    ));
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn skipping_by_id_starts_inside_a_record() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client
        .publisher("skip", PublisherConfig::default())
        .unwrap();
    for index in 0..10u8 {
        publisher.add(1, vec![index]);
    }
    let first_id = publisher.flush().await.unwrap()[0].first_id;
    // The server can only send the whole record, and these three are what's left of it.
    let from = first_id + 6;
    let mut subscription = client.subscribe(Subscribe::new("skip", from)).unwrap();
    let got = items(&mut subscription, 3).await;
    assert_eq!(bytes_of(&got), [vec![7u8], vec![8], vec![9]]);
    assert_eq!(subscription.cursor(), first_id + 9);
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_reach_the_server() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client.publisher("f", PublisherConfig::default()).unwrap();
    for (options, data) in [(1u64, b"one"), (2, b"two"), (3, b"thr")] {
        publisher.add(options, data.to_vec());
    }
    publisher.flush().await.unwrap();
    let mut spec = Subscribe::new("f", FROM_START);
    spec.clauses = vec![WireClause {
        mask: 0xFF,
        value: 2,
        op: format::OP_EQ,
    }];
    let mut subscription = client.subscribe(spec).unwrap();
    assert_eq!(
        bytes_of(&items(&mut subscription, 1).await),
        [b"two".to_vec()]
    );
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_named_cursor_commits_and_a_new_subscriber_starts_after_it() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client.publisher("n", PublisherConfig::default()).unwrap();
    let mut ids = Vec::new();
    for index in 0..3u8 {
        publisher.add(0, vec![index]);
        ids.push(publisher.flush().await.unwrap()[0].first_id);
    }
    let mut spec = Subscribe::new("n", FROM_START);
    spec.name = Some("mine".to_owned());
    let mut subscription = client.subscribe(spec.clone()).unwrap();
    assert_eq!(items(&mut subscription, 3).await.len(), 3);
    subscription.commit(ids[1]).unwrap();
    // A commit has no answer, so a ping's round is what shows it has been taken.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(subscription);

    let other = fixture.client().await;
    let mut subscription = other.subscribe(spec).unwrap();
    assert_eq!(bytes_of(&items(&mut subscription, 1).await), [vec![2u8]]);
    fixture.server.shutdown();
}

/// Section 3's whole point: a cut connection re-sends, and nothing is written twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_connection_retries_with_the_same_seq_and_nothing_doubles() {
    let fixture = start();
    let (client, cutter) = fixture.cuttable().await;
    let mut publisher = client.publisher("r", PublisherConfig::default()).unwrap();
    let mut waits = Vec::new();
    for index in 0..300u32 {
        publisher.add(0, index.to_le_bytes().to_vec());
        waits.extend(publisher.send().await.unwrap());
        if index % 50 == 25 {
            cutter.cut.notify_waiters();
        }
    }
    let mut count = 0;
    for wait in waits {
        // `first_id = 0` is the older retry: committed before, id not kept.
        let ack = wait.wait().await.unwrap();
        assert!(ack.count == 1 || ack.first_id == 0, "{ack:?}");
        count += 1;
    }
    assert_eq!(count, 300);
    assert!(fixture.server.io_stats().accepted_count >= 2, "it was cut");

    let reader = fixture.client().await;
    let mut subscription = reader.subscribe(Subscribe::new("r", FROM_START)).unwrap();
    let got = items(&mut subscription, 300).await;
    let expected: Vec<Vec<u8>> = (0..300u32).map(|n| n.to_le_bytes().to_vec()).collect();
    assert_eq!(bytes_of(&got), expected, "in order, once each");
    // Nothing after those.
    let reply = fixture
        .http()
        .request("GET", "/v1/stream/r/read", &[], b"")
        .await
        .unwrap();
    assert_eq!(reply.header("x-count"), Some("300"));
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_resumes_after_a_cut_without_a_gap_or_a_repeat() {
    let fixture = start();
    let (client, cutter) = fixture.cuttable().await;
    let writer = fixture.client().await;
    let created = fixture
        .http()
        .request("PUT", "/v1/stream/resume", &[], b"")
        .await
        .unwrap();
    assert!(created.is_ok());
    let mut publisher = writer
        .publisher("resume", PublisherConfig::default())
        .unwrap();
    let mut live = client
        .subscribe(Subscribe::new("resume", FROM_LIVE))
        .unwrap();
    assert!(matches!(
        next_event(&mut live).await.unwrap(),
        Event::Subscribed { .. }
    ));
    let mut group = 0u32;
    let mut expected = Vec::new();
    for round in 0..5 {
        // Groups, so that a resume can land inside a record.
        for _ in 0..40 {
            for _ in 0..3 {
                publisher.add(0, group.to_le_bytes().to_vec());
                expected.push(group.to_le_bytes().to_vec());
                group += 1;
            }
            publisher.flush().await.unwrap();
        }
        if round < 4 {
            cutter.cut.notify_waiters();
        }
    }
    let got = items(&mut live, expected.len()).await;
    // One for the writer and one for the HTTP call, so the rest are this client's.
    assert!(fixture.server.io_stats().accepted_count >= 4, "it was cut");
    assert_eq!(bytes_of(&got), expected);
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_state_op_cut_off_says_disconnected_rather_than_guessing() {
    let fixture = start();
    let (client, cutter) = fixture.cuttable().await;
    cutter.refuse.store(true, Ordering::SeqCst);
    cutter.cut.notify_waiters();
    // Until the client notices, an op can be lost in the cut. Either way it mustn't hang.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let change = StateChange {
        expect_ver: 0,
        op: StateOp::Set,
        doc: doc("a", 1),
    };
    let result = tokio::time::timeout(
        Duration::from_millis(WAIT_MS),
        client.state_op("cutoff", change.clone()),
    )
    .await
    .expect("not a hang");
    assert_eq!(result, Err(SdkError::Disconnected));
    cutter.refuse.store(false, Ordering::SeqCst);
    let mut worked = false;
    for _ in 0..200 {
        if client.state_op("cutoff", change.clone()).await.is_ok() {
            worked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(worked, "it reconnected");
    fixture.server.shutdown();
}

/// A consumer that isn't reading makes the SDK let go of the subscription, and nothing is lost
/// when it comes back.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_reader_pauses_and_comes_back_without_loss() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client
        .publisher(
            "slow",
            PublisherConfig {
                batch: BatchConfig {
                    zstd_level: None,
                    record_target_bytes: 1024,
                },
                ..PublisherConfig::default()
            },
        )
        .unwrap();
    let mut expected = Vec::new();
    let mut publish = async |index: u32| {
        let mut item = vec![0u8; 1000];
        item[..4].copy_from_slice(&index.to_le_bytes());
        publisher.add(0, item.clone());
        publisher.flush().await.unwrap();
        item
    };
    // The first publish makes the stream, which may be on another core than the subscribe.
    expected.push(publish(0).await);
    let mut spec = Subscribe::new("slow", FROM_START);
    spec.max_queue_bytes = 16 * 1024;
    let mut subscription = client.subscribe(spec).unwrap();
    for index in 1..200u32 {
        expected.push(publish(index).await);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let got = items(&mut subscription, expected.len()).await;
    assert_eq!(bytes_of(&got), expected);
    fixture.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn renewing_a_token_keeps_the_connection_working() {
    let fixture = start();
    let mut config = fixture.config();
    // Good for a moment, on a clock the test holds still.
    config.token = token(START_MS + 1000);
    let client = Client::connect(config).await.unwrap();
    client.set_token(&token(u64::MAX)).unwrap();
    let mut publisher = client
        .publisher("renew", PublisherConfig::default())
        .unwrap();
    publisher.add(0, b"x".to_vec());
    publisher.flush().await.unwrap();
    fixture.server.shutdown();
}

/// A server that answers only as scripted, over the same TLS as the real one.
struct Scripted {
    url: String,
    ca_pem: Vec<u8>,
    frames: tokio::sync::mpsc::UnboundedReceiver<(u32, Vec<u8>)>,
    replies: tokio::sync::mpsc::UnboundedSender<Reply>,
}

enum Reply {
    Ack {
        handle: u32,
        seq: u64,
        first_id: u64,
    },
    Close,
}

async fn scripted() -> Scripted {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let ca_pem = cert.cert.pem().into_bytes();
    let tls =
        server::net::tls13_only_config(&ca_pem, cert.key_pair.serialize_pem().as_bytes()).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("localhost:{}", listener.local_addr().unwrap().port());
    let (frames_tx, frames) = tokio::sync::mpsc::unbounded_channel();
    let (replies, mut replies_rx) = tokio::sync::mpsc::unbounded_channel::<Reply>();
    tokio::spawn(async move {
        let mut connection = 0;
        loop {
            connection += 1;
            let (socket, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(socket).await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tls).await.unwrap();
            'session: loop {
                tokio::select! {
                    message = ws.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else {
                            break 'session;
                        };
                        if let Ok(ClientFrame::Hello { .. }) = decode_client(&bytes, usize::MAX) {
                            let mut out = Vec::new();
                            ServerFrame::Welcome {
                                session: 1,
                                max_frame_bytes: 1 << 20,
                                server_ms: 0,
                            }
                            .encode(&mut out)
                            .unwrap();
                            ws.send(Message::Binary(out)).await.unwrap();
                        } else {
                            frames_tx.send((connection, bytes)).unwrap();
                        }
                    }
                    reply = replies_rx.recv() => {
                        match reply.unwrap() {
                            Reply::Ack { handle, seq, first_id } => {
                                let mut out = Vec::new();
                                ServerFrame::Ack {
                                    handle,
                                    producer_seq: seq,
                                    first_id,
                                    count: 1,
                                }
                                .encode(&mut out)
                                .unwrap();
                                ws.send(Message::Binary(out)).await.unwrap();
                            }
                            Reply::Close => break 'session,
                        }
                    }
                }
            }
        }
    });
    Scripted {
        url,
        ca_pem,
        frames,
        replies,
    }
}

impl Scripted {
    /// The next publish, as `(connection, handle, id, seq)`, past any `OPEN`.
    async fn next_publish(&mut self) -> (u32, u32, u64, u64) {
        loop {
            let (connection, bytes) =
                tokio::time::timeout(Duration::from_millis(WAIT_MS), self.frames.recv())
                    .await
                    .expect("a frame should come")
                    .unwrap();
            if let ClientFrame::Publish(publish) = decode_client(&bytes, usize::MAX).unwrap() {
                return (
                    connection,
                    publish.handle,
                    publish.producer_id,
                    publish.producer_seq,
                );
            }
        }
    }

    async fn nothing_for_a_moment(&mut self) {
        let frame = tokio::time::timeout(Duration::from_millis(200), self.frames.recv()).await;
        assert!(frame.is_err(), "nothing should have been sent: {frame:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_with_expect_ver_holds_back_later_ones_until_answered() {
    let mut fake = scripted().await;
    let client = Client::connect(ClientConfig::new(&fake.url, &fake.ca_pem, "t"))
        .await
        .unwrap();
    let mut publisher = client.publisher("x", PublisherConfig::default()).unwrap();

    // Without `expect_ver`, publishes go out in parallel.
    publisher.add(0, b"1".to_vec());
    let first = publisher.send().await.unwrap();
    publisher.add(0, b"2".to_vec());
    let second = publisher.send().await.unwrap();
    let (_, handle, _, seq_one) = fake.next_publish().await;
    let (_, _, _, seq_two) = fake.next_publish().await;
    assert_eq!((seq_one, seq_two), (1, 2));

    publisher.add(0, b"3".to_vec());
    let change = StateChange {
        expect_ver: 41,
        op: StateOp::Set,
        doc: doc("a", 1),
    };
    let held = publisher.send_with_state(change).await.unwrap();
    assert_eq!(fake.next_publish().await.3, 3);
    publisher.add(0, b"4".to_vec());
    let after = publisher.send().await.unwrap();
    publisher.add(0, b"5".to_vec());
    let after_too = publisher.send().await.unwrap();
    fake.nothing_for_a_moment().await;

    // Earlier acks don't release them. Only the conditional one's own does.
    for seq in [1, 2] {
        fake.replies
            .send(Reply::Ack {
                handle,
                seq,
                first_id: seq * 10,
            })
            .unwrap();
    }
    assert_eq!(
        first
            .into_iter()
            .next()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .first_id,
        10
    );
    assert_eq!(
        second
            .into_iter()
            .next()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .first_id,
        20
    );
    fake.nothing_for_a_moment().await;
    fake.replies
        .send(Reply::Ack {
            handle,
            seq: 3,
            first_id: 30,
        })
        .unwrap();
    assert_eq!(
        held.into_iter()
            .next()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .first_id,
        30
    );
    assert_eq!(fake.next_publish().await.3, 4);
    assert_eq!(fake.next_publish().await.3, 5);
    for seq in [4, 5] {
        fake.replies
            .send(Reply::Ack {
                handle,
                seq,
                first_id: seq * 10,
            })
            .unwrap();
    }
    assert_eq!(
        after
            .into_iter()
            .next()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .first_id,
        40
    );
    assert_eq!(
        after_too
            .into_iter()
            .next()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .first_id,
        50
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn after_a_reconnect_the_same_producer_resends_in_order_with_the_same_seqs() {
    let mut fake = scripted().await;
    let client = Client::connect(ClientConfig::new(&fake.url, &fake.ca_pem, "t"))
        .await
        .unwrap();
    let mut publisher = client.publisher("x", PublisherConfig::default()).unwrap();
    let mut waits = Vec::new();
    for index in 0..3u8 {
        publisher.add(0, vec![index]);
        waits.extend(publisher.send().await.unwrap());
    }
    let (connection, handle, producer_id, _) = fake.next_publish().await;
    assert_eq!(connection, 1);
    assert_ne!(producer_id, 0);
    assert_eq!(fake.next_publish().await.3, 2);
    assert_eq!(fake.next_publish().await.3, 3);
    // The first is answered, and the other two are lost with the connection.
    fake.replies
        .send(Reply::Ack {
            handle,
            seq: 1,
            first_id: 10,
        })
        .unwrap();
    let mut waits = waits.into_iter();
    assert_eq!(waits.next().unwrap().wait().await.unwrap().first_id, 10);
    fake.replies.send(Reply::Close).unwrap();

    for seq in [2u64, 3] {
        let (connection, handle_again, id_again, seq_again) = fake.next_publish().await;
        assert_eq!(connection, 2);
        assert_eq!(
            (handle_again, id_again, seq_again),
            (handle, producer_id, seq)
        );
    }
    for seq in [2u64, 3] {
        fake.replies
            .send(Reply::Ack {
                handle,
                seq,
                first_id: seq * 10,
            })
            .unwrap();
    }
    assert_eq!(waits.next().unwrap().wait().await.unwrap().first_id, 20);
    assert_eq!(waits.next().unwrap().wait().await.unwrap().first_id, 30);
}

fn raw_records(items: &[(u64, &[u8])]) -> Vec<u8> {
    let raw = BatchConfig {
        zstd_level: None,
        ..BatchConfig::default()
    };
    sdk::build_records(items, &raw).unwrap().concat()
}

/// Reads until it is quiet, and gives back the items and the errors in the order they came.
async fn read_until_quiet(subscription: &mut Subscription) -> (Vec<Vec<u8>>, Vec<SdkError>) {
    let (mut got, mut errors) = (Vec::new(), Vec::new());
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(1_000), subscription.next()).await
    {
        match event {
            Ok(Event::Items(items)) => got.extend(items.into_iter().map(|item| item.bytes)),
            Ok(_) => {}
            Err(error) => errors.push(error),
        }
    }
    (got, errors)
}

/// Section 2: data is opaque, so any publisher can send a record nobody can decode. That costs
/// the reader that record and nothing else, and it is told which ids it lost.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_record_loses_only_itself_and_says_which_ids() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client
        .publisher("poison", PublisherConfig::default())
        .unwrap();
    let mut bad = raw_records(&[(2, b"garbage!")]);
    bad[30..32].copy_from_slice(&CODEC_ZSTD.to_le_bytes());
    let mut joined = raw_records(&[(1, b"good-before")]);
    joined.extend_from_slice(&bad);
    joined.extend_from_slice(&raw_records(&[(3, b"good-after")]));
    let first = publisher
        .send_records(&joined, None)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    publisher
        .send_records(&raw_records(&[(1, b"next-publish")]), None)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();

    let mut subscription = client
        .subscribe(Subscribe::new("poison", FROM_START))
        .unwrap();
    let (got, errors) = read_until_quiet(&mut subscription).await;
    assert_eq!(
        got,
        [
            b"good-before".to_vec(),
            b"good-after".to_vec(),
            b"next-publish".to_vec()
        ]
    );
    assert_eq!(
        errors,
        [SdkError::BadRecord {
            first_id: first.first_id + 1,
            last_id: first.first_id + 1,
            too_large: false
        }]
    );
    // Each of them is reported once, in id order, and the cursor is past the lot.
    assert!(subscription.cursor() > first.first_id + 2);
    fixture.server.shutdown();
}

/// The limit is what one `next` can make the reader hold, not a limit per record: otherwise many
/// small bombs in a frame multiply it. Nothing good is lost for it.
#[tokio::test(flavor = "multi_thread")]
async fn the_decompression_limit_bounds_what_next_holds_across_a_frame() {
    const LIMIT_BYTES: usize = 1024 * 1024;
    const RECORD_COUNT: u64 = 64;
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client
        .publisher("bomb", PublisherConfig::default())
        .unwrap();
    let zeros = vec![0u8; LIMIT_BYTES];
    let items: Vec<(u64, &[u8])> = (0..RECORD_COUNT).map(|i| (i, zeros.as_slice())).collect();
    let joined = sdk::build_records(&items, &BatchConfig::default())
        .unwrap()
        .concat();
    assert!(joined.len() < 64 * 1024, "one frame: {}", joined.len());
    publisher
        .send_records(&joined, None)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    // And a single record over the limit is refused, between the good ones.
    let huge = vec![0u8; LIMIT_BYTES + 1];
    let joined = sdk::build_records(
        &[(1, b"ok".as_slice()), (2, &huge), (3, b"ok")],
        &BatchConfig::default(),
    )
    .unwrap()
    .concat();
    let second = publisher
        .send_records(&joined, None)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();

    let mut spec = Subscribe::new("bomb", FROM_START);
    spec.max_decompressed_bytes = LIMIT_BYTES;
    let mut subscription = client.subscribe(spec).unwrap();
    let (mut total, mut seen, mut errors) = (0, 0, Vec::new());
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(2_000), subscription.next()).await
    {
        match event {
            Ok(Event::Items(items)) => {
                let bytes: usize = items.iter().map(|item| item.bytes.len()).sum();
                assert!(bytes <= LIMIT_BYTES, "one event held {bytes}");
                total += bytes;
                seen += items.len();
            }
            Ok(_) => {}
            Err(error) => errors.push(error),
        }
    }
    assert_eq!(seen as u64, RECORD_COUNT + 2);
    assert_eq!(total, RECORD_COUNT as usize * LIMIT_BYTES + 4);
    assert_eq!(
        errors,
        [SdkError::BadRecord {
            first_id: second.first_id + 1,
            last_id: second.first_id + 1,
            too_large: true
        }]
    );
    fixture.server.shutdown();
}

/// A write the node can't take yet is tried again however busy the connection is. The inbox of 1
/// makes that the common case, and subscribing on the same connection keeps events coming.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_waiting_for_room_goes_through_while_events_keep_arriving() {
    let mut node = node_config();
    node.write_inbox_capacity = 1;
    let fixture = start_with(node);
    let config = || PublisherConfig {
        ack: AckLevel::Memory,
        batch: BatchConfig {
            zstd_level: None,
            ..BatchConfig::default()
        },
        ..PublisherConfig::default()
    };
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let client = fixture.client().await;
        let stop = Arc::clone(&stop);
        tasks.push(tokio::spawn(async move {
            let mut publisher = client.publisher("hot", config()).unwrap();
            while !stop.load(Ordering::Relaxed) {
                publisher.add(1, vec![1u8; 64]);
                let _ = publisher.flush().await;
            }
        }));
    }
    let client = fixture.client().await;
    let mut subscription = client.subscribe(Subscribe::new("hot", FROM_LIVE)).unwrap();
    tasks.push(tokio::spawn(async move {
        while let Some(event) = subscription.next().await {
            if event.is_err() {
                return;
            }
        }
    }));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut publisher = client.publisher("hot", config()).unwrap();
    let mut stuck_at = None;
    for done in 0..300 {
        publisher.add(1, vec![2u8; 64]);
        let flushed = tokio::time::timeout(Duration::from_secs(8), publisher.flush()).await;
        if flushed.is_err() {
            stuck_at = Some(done);
            break;
        }
        flushed.unwrap().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    assert_eq!(stuck_at, None, "the ack never came with events flowing");
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_state_op_over_max_frame_is_answered() {
    let mut node = node_config();
    node.max_frame_bytes = 128 * 1024;
    let fixture = start_with(node);
    let client = fixture.client().await;
    let change = StateChange {
        expect_ver: 0,
        op: StateOp::Set,
        doc: vec![b'x'; 200 * 1024],
    };
    let answer = tokio::time::timeout(Duration::from_secs(5), client.state_op("ov", change)).await;
    assert_eq!(answer, Ok(Err(SdkError::TooLarge)));
    let small = StateChange {
        expect_ver: 0,
        op: StateOp::Set,
        doc: doc("a", 1),
    };
    let answer = tokio::time::timeout(Duration::from_secs(5), client.state_op("ov", small)).await;
    assert!(matches!(answer, Ok(Ok(_))), "{answer:?}");
    fixture.server.shutdown();
}

/// With `expect_ver` it holds back the producer's later publishes, so going unanswered would stop
/// that producer for good.
#[tokio::test(flavor = "multi_thread")]
async fn a_publish_over_max_frame_is_answered_and_doesnt_wedge_its_producer() {
    let mut node = node_config();
    node.max_frame_bytes = 128 * 1024;
    let fixture = start_with(node);
    let client = fixture.client().await;
    let mut publisher = client.publisher("ov", PublisherConfig::default()).unwrap();
    let big = raw_records(&[(1, &vec![b'x'; 200 * 1024])]);
    let first = publisher
        .send_records(
            &big,
            Some(StateChange {
                expect_ver: 7,
                op: StateOp::Set,
                doc: doc("a", 1),
            }),
        )
        .await
        .unwrap();
    let second = publisher
        .send_records(&raw_records(&[(1, b"small")]), None)
        .await
        .unwrap();
    let wait = Duration::from_secs(5);
    assert_eq!(
        tokio::time::timeout(wait, first.wait()).await,
        Ok(Err(SdkError::TooLarge))
    );
    let later = tokio::time::timeout(wait, second.wait()).await;
    assert!(matches!(later, Ok(Ok(_))), "{later:?}");
    fixture.server.shutdown();
}

/// A peer that takes the connection and then says nothing, which no socket error ever reports.
#[tokio::test(flavor = "multi_thread")]
async fn connecting_to_a_silent_peer_gives_up() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let holder = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    let fixture = start();
    let config = ClientConfig::new(&format!("localhost:{port}"), &fixture.ca_pem, "x");
    let result = tokio::time::timeout(Duration::from_secs(20), Client::connect(config)).await;
    assert!(
        matches!(result, Ok(Err(SdkError::Connect(_)))),
        "{:?}",
        result.map(|r| r.err())
    );
    holder.abort();
    fixture.server.shutdown();
}

/// One reconnect attempt stuck on a silent peer doesn't stop the client answering, and doesn't
/// stop it coming back when a later attempt would work.
#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_stuck_on_a_silent_peer_recovers_and_keeps_answering() {
    let fixture = start();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let target = fixture.server.local_addr();
    let cut = Arc::new(tokio::sync::Notify::new());
    let swallow_next = Arc::new(AtomicBool::new(false));
    let proxy = {
        let (cut, swallow_next) = (Arc::clone(&cut), Arc::clone(&swallow_next));
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut near, _)) = listener.accept().await {
                if swallow_next.swap(false, Ordering::SeqCst) {
                    held.push(near);
                    continue;
                }
                let cut = Arc::clone(&cut);
                tokio::spawn(async move {
                    let mut far = TcpStream::connect(target).await.unwrap();
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut near, &mut far) => {}
                        _ = cut.notified() => {}
                    }
                });
            }
        })
    };
    let mut config = ClientConfig::new(
        &format!("localhost:{port}"),
        &fixture.ca_pem,
        &token(u64::MAX),
    );
    config.reconnect_max_ms = 100;
    let client = Client::connect(config).await.unwrap();
    let mut publisher = client
        .publisher("heal", PublisherConfig::default())
        .unwrap();
    publisher.add(1, b"before".to_vec());
    publisher.flush().await.unwrap();

    swallow_next.store(true, Ordering::SeqCst);
    cut.notify_waiters();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let answer = tokio::time::timeout(
        Duration::from_secs(2),
        client.state_op(
            "heal",
            StateChange {
                expect_ver: 0,
                op: StateOp::Set,
                doc: doc("a", 1),
            },
        ),
    )
    .await;
    assert_eq!(answer, Ok(Err(SdkError::Disconnected)));

    publisher.add(1, b"after".to_vec());
    let flushed = tokio::time::timeout(Duration::from_secs(30), publisher.flush()).await;
    assert!(matches!(flushed, Ok(Ok(_))), "{flushed:?}");
    proxy.abort();
    fixture.server.shutdown();
}

/// A name the server can't accept would otherwise be a subscription that never hears anything.
#[tokio::test]
async fn a_subscription_to_an_invalid_id_is_refused() {
    let fixture = start();
    let client = fixture.client().await;
    let refused = client.subscribe(Subscribe::new("a/b", 0));
    assert!(matches!(refused, Err(SdkError::Config(_))));
    drop(client);
    fixture.server.shutdown();
}

/// A subscription the server ends gives its error and then its end, rather than a reader that
/// waits for ever.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_the_server_refuses_ends_after_its_error() {
    let fixture = start();
    let writer = auth::Token {
        expiry_ms: u64::MAX,
        author: std::num::NonZeroU64::new(9).unwrap(),
        grants: vec![auth::Grant {
            pattern: "*".to_owned(),
            perms: auth::PERM_PUBLISH,
        }],
    }
    .sign(SECRET)
    .unwrap();
    let mut config = fixture.config();
    config.token = writer;
    let client = Client::connect(config).await.unwrap();
    let mut subscription = client.subscribe(Subscribe::new("no", 0)).unwrap();
    let error = next_event(&mut subscription).await.unwrap_err();
    assert!(
        matches!(
            &error,
            SdkError::Server {
                code: ErrorCode::Unauthorized,
                ..
            }
        ),
        "{error:?}"
    );
    let after = tokio::time::timeout(Duration::from_millis(WAIT_MS), subscription.next())
        .await
        .expect("a refused subscription should end, not wait");
    assert!(after.is_none(), "{after:?}");
    drop(subscription);
    drop(client);
    fixture.server.shutdown();
}

/// A timed flush on an idle publisher sends nothing, and uses no seq.
#[tokio::test]
async fn sending_nothing_sends_nothing() {
    let fixture = start();
    let client = fixture.client().await;
    let config = PublisherConfig {
        producer_id: Some(7),
        first_seq: 5,
        ..PublisherConfig::default()
    };
    let mut publisher = client.publisher("idle", config).unwrap();
    assert!(publisher.send().await.unwrap().is_empty());
    assert!(publisher.flush().await.unwrap().is_empty());
    publisher.add(0, b"x".to_vec());
    let acks = publisher.flush().await.unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].producer_seq, 5);
    drop((publisher, client));
    fixture.server.shutdown();
}

/// A `send` cancelled while it waits for room has lost nothing: the next one goes on from there.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_send_keeps_what_it_took() {
    let mut fake = scripted().await;
    let client = Client::connect(ClientConfig::new(&fake.url, &fake.ca_pem, "t"))
        .await
        .unwrap();
    let config = PublisherConfig {
        max_inflight_bytes: 100,
        ..PublisherConfig::default()
    };
    let mut publisher = client.publisher("x", config).unwrap();
    publisher.add(0, vec![1; 50]);
    let first = publisher.send().await.unwrap();
    let (_, handle, _, seq) = fake.next_publish().await;
    assert_eq!(seq, 1);

    publisher.add(0, vec![2; 50]);
    let cancelled = tokio::time::timeout(Duration::from_millis(50), publisher.send()).await;
    assert!(cancelled.is_err(), "there's no room for this one yet");
    fake.nothing_for_a_moment().await;

    fake.replies
        .send(Reply::Ack {
            handle,
            seq,
            first_id: 1,
        })
        .unwrap();
    for wait in first {
        wait.wait().await.unwrap();
    }
    let waits = publisher.send().await.unwrap();
    assert_eq!(waits.len(), 1);
    let (_, _, _, seq) = fake.next_publish().await;
    assert_eq!(seq, 2);
    fake.nothing_for_a_moment().await;
}

/// Section 4's "from any point to any point": the range ends by itself, inside a record too, and
/// also when a filter leaves nothing to show for the end of the range.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_with_an_end_stops_there() {
    let fixture = start();
    let client = fixture.client().await;
    let mut publisher = client
        .publisher("range", PublisherConfig::default())
        .unwrap();
    for index in 0..10u8 {
        publisher.add(1, vec![index]);
    }
    let first_id = publisher.flush().await.unwrap()[0].first_id;

    let mut spec = Subscribe::new("range", first_id + 1);
    spec.until = Some(first_id + 4);
    let mut subscription = client.subscribe(spec).unwrap();
    let mut got = Vec::new();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(event) = subscription.next().await {
            if let Event::Items(items) = event.unwrap() {
                got.extend(items);
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "it never ended, with {got:?}");
    assert!(subscription.reached_until());
    assert_eq!(bytes_of(&got), [vec![2u8], vec![3], vec![4]]);

    // Nothing matches, so `PROGRESS` is what reaches the end.
    let mut spec = Subscribe::new("range", 0);
    spec.until = Some(first_id + 9);
    spec.author = Some(987_654);
    let mut subscription = client.subscribe(spec).unwrap();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(event) = subscription.next().await {
            assert!(!matches!(event.unwrap(), Event::Items(_)));
        }
    })
    .await;
    assert!(ended.is_ok(), "a filter that matches nothing never ended");
    fixture.server.shutdown();
}
