//! Sinks (SPEC section 9), on a node with a scripted clock and real sockets to a small receiver
//! that the test scripts. The real binary is killed in `sink_crash.rs`; a stalled sink's cost is
//! measured in `sink_stall.rs`.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use hmac::{Hmac, Mac};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use proptest::prelude::*;
use server::{
    Client, ManualClock, Node, NodeConfig, RecoverConfig, RetentionClass, SinkAction, SinkHook,
    SinkOp, SinkReport, SinkResolver, SinkSpec, SinkStart, SinkStep, StateChange, StateOp,
    StreamConfig, Subscription,
};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

const STREAM: &str = "sink.source";
const SECRET: &str = "the sink's key";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Answer {
    Status(u16),
    /// Doesn't answer until the receiver is released.
    Hold,
}

#[derive(Clone, Debug)]
struct Seen {
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Seen {
    fn triple(&self) -> (u64, u64, u64) {
        let number = |name: &str| self.headers[name].parse::<u64>().unwrap();
        (
            number("x-sink-id"),
            number("x-first-id"),
            number("x-last-id"),
        )
    }

    fn data(&self) -> Vec<u8> {
        let records = parse_records(&self.body);
        assert_eq!(records.len(), 1, "one record per delivery");
        records[0].3.clone()
    }
}

/// A small HTTP/1.1 server. Every request it sees goes in `seen`, whatever it answers.
#[derive(Clone)]
struct Receiver {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    answer: Arc<Mutex<Answer>>,
    released: Arc<Notify>,
    in_flight: Arc<AtomicU64>,
    peak_in_flight: Arc<AtomicU64>,
    release_all: Arc<AtomicBool>,
}

async fn serve_one<S: AsyncRead + AsyncWrite + Unpin>(mut socket: S, receiver: Receiver) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
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
    let mut headers = HashMap::new();
    for line in head.split("\r\n").skip(1) {
        if let Some((name, value)) = line.split_once(": ") {
            headers.insert(name.to_ascii_lowercase(), value.to_owned());
        }
    }
    let len: usize = headers["content-length"].parse().unwrap();
    while buffer.len() < head_end + len {
        match socket.read(&mut chunk).await {
            Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
            _ => return,
        }
    }
    let answer = *receiver.answer.lock().unwrap();
    let in_flight = receiver.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    receiver
        .peak_in_flight
        .fetch_max(in_flight, Ordering::SeqCst);
    receiver.seen.lock().unwrap().push(Seen {
        headers,
        body: buffer[head_end..head_end + len].to_vec(),
    });
    let status = match answer {
        Answer::Status(status) => status,
        Answer::Hold => {
            while !receiver.release_all.load(Ordering::SeqCst) {
                tokio::select! {
                    () = receiver.released.notified() => {}
                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
            200
        }
    };
    receiver.in_flight.fetch_sub(1, Ordering::SeqCst);
    let reply = format!("HTTP/1.1 {status} Status\r\nContent-Length: 0\r\n\r\n");
    let _ = socket.write_all(reply.as_bytes()).await;
    let _ = socket.flush().await;
}

impl Receiver {
    async fn start() -> Receiver {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver = Receiver {
            address: listener.local_addr().unwrap(),
            seen: Arc::default(),
            answer: Arc::new(Mutex::new(Answer::Status(200))),
            released: Arc::new(Notify::new()),
            in_flight: Arc::default(),
            peak_in_flight: Arc::default(),
            release_all: Arc::default(),
        };
        let serving = receiver.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve_one(socket, serving.clone()));
            }
        });
        receiver
    }

    fn url(&self) -> String {
        format!("http://{}/hook", self.address)
    }

    fn answer(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }

    fn release(&self) {
        self.release_all.store(true, Ordering::SeqCst);
        self.released.notify_waiters();
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }

    async fn wait_for_count(&self, count: usize) -> Vec<Seen> {
        wait_until("requests to arrive", || self.count() >= count).await;
        self.seen()
    }

    /// Nothing more arrives for a while.
    async fn stays_at(&self, count: usize) {
        tokio::time::sleep(Duration::from_millis(QUIET_MS * 2)).await;
        assert_eq!(self.count(), count, "more requests arrived than expected");
    }
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(
            started.elapsed() < Duration::from_millis(WAIT_MS),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn config() -> NodeConfig {
    let mut config = manual_config(2);
    config.sink.allow_private_addresses = true;
    config.sink.tick_ms = 2;
    config.sink.paused_check_ms = 5;
    config.sink.retry_initial_ms = 1_000;
    config.sink.retry_max_ms = 8_000;
    config.sink.request_timeout_ms = 20_000;
    config
}

fn deliver_spec(url: &str) -> SinkSpec {
    SinkSpec {
        kind: "webhook.deliver".to_owned(),
        url: Some(url.to_owned()),
        secret: Some(SECRET.to_owned()),
        on_expire: "stop".to_owned(),
        concurrency: 1,
        ..SinkSpec::default()
    }
}

fn forward_spec(target: &str) -> SinkSpec {
    SinkSpec {
        kind: "forward".to_owned(),
        target: Some(target.to_owned()),
        on_expire: "stop".to_owned(),
        concurrency: 1,
        ..SinkSpec::default()
    }
}

async fn put_sink(
    client: &mut Client,
    stream: &str,
    sink_id: u64,
    spec: SinkSpec,
    start: SinkStart,
) {
    client
        .sink_op(
            stream,
            SinkOp::Put {
                sink_id,
                spec: Box::new(spec),
                start,
            },
        )
        .await
        .unwrap();
}

/// Publishes one record at level 1 and waits for its ack. Answers with its id.
async fn send(client: &mut Client, stream: &str, data: &[u8], seq: u64) -> u64 {
    let records = record_with(1, data);
    client
        .publish(publish(stream, &records, seq, 1))
        .await
        .unwrap();
    expect_ack(client).await.1
}

async fn sink_of(client: &mut Client, stream: &str, sink_id: u64) -> (SinkSpec, u64) {
    let info = client.stream_info(stream).await.unwrap();
    let (_, spec, cursor) = info
        .sinks
        .into_iter()
        .find(|(id, _, _)| *id == sink_id)
        .expect("the sink is listed");
    (spec, cursor.unwrap_or(0))
}

async fn wait_for_cursor(client: &mut Client, stream: &str, sink_id: u64, at_least: u64) {
    let started = Instant::now();
    loop {
        let (_, cursor) = sink_of(client, stream, sink_id).await;
        if cursor >= at_least {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_millis(WAIT_MS),
            "the cursor stayed at {cursor}, below {at_least}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn wait_for_pause(client: &mut Client, stream: &str, sink_id: u64) -> String {
    let started = Instant::now();
    loop {
        let (spec, _) = sink_of(client, stream, sink_id).await;
        if spec.paused {
            return spec.pause_reason.unwrap_or_default();
        }
        assert!(
            started.elapsed() < Duration::from_millis(WAIT_MS),
            "the sink never paused"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The bytes a reader gets of everything in `stream`.
async fn stored_bytes(node: &Node, stream: &str, up_to_id: u64, include_system: bool) -> Vec<u8> {
    let mut connection = node.connection(0).await.unwrap();
    let mut subscription = Subscription::new(1, stream, 0);
    subscription.include_system = include_system;
    connection.subscribe(subscription);
    let mut out = Vec::new();
    let mut through = 0;
    while through < up_to_id {
        let event = connection
            .recv_within(WAIT_MS)
            .await
            .expect("the records arrive");
        if let server::Event::Data { records, .. } = event {
            let bytes = records.to_vec();
            for record in format::Records::new(&bytes) {
                through = through.max(record.unwrap().header.last_id());
            }
            out.extend_from_slice(&bytes);
        }
    }
    out
}

async fn target_records(node: &Node, stream: &str, count: usize) -> Vec<(u64, u64, u64, Vec<u8>)> {
    let mut connection = node.connection(0).await.unwrap();
    connection.subscribe(Subscription::new(1, stream, 0));
    let mut heard = Heard::default();
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= count
    })
    .await;
    // Anything past the count would be a duplicate, so give it time to show.
    hear_for(&mut connection, &mut heard, QUIET_MS).await;
    heard.records(1)
}

// ---------- at-least-once ----------

/// Kills the task the first time it passes `step` for a sink, or as the script says.
#[derive(Default)]
struct KillScript {
    /// `step → kills left`.
    kills: Mutex<Vec<(SinkStep, u32)>>,
    kill_count: AtomicU64,
    peak_held_bytes: AtomicU64,
}

impl KillScript {
    fn with(kills: &[(SinkStep, u32)]) -> Arc<KillScript> {
        Arc::new(KillScript {
            kills: Mutex::new(kills.to_vec()),
            ..KillScript::default()
        })
    }
}

impl SinkHook for KillScript {
    fn at(&self, _sink_id: u64, step: SinkStep) -> SinkAction {
        let mut kills = self.kills.lock().unwrap();
        for (kill_step, left) in kills.iter_mut() {
            if *kill_step == step && *left > 0 {
                *left -= 1;
                self.kill_count.fetch_add(1, Ordering::SeqCst);
                return SinkAction::Kill;
            }
        }
        SinkAction::Continue
    }

    fn held(&self, _sink_id: u64, held_bytes: u64) {
        self.peak_held_bytes.fetch_max(held_bytes, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_between_delivery_and_cursor_it_redelivers_the_same_triple() {
    let receiver = Receiver::start().await;
    let hook = KillScript::with(&[(SinkStep::AfterDelivery, 1)]);
    let mut config = config();
    config.sink.hook = Some(hook.clone());
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut records = Vec::new();
    add_record(&mut records, 1, b"grouped", 5);
    client
        .publish(publish(STREAM, &records, 1, 1))
        .await
        .unwrap();
    let (_, first_id, _) = expect_ack(&mut client).await;
    put_sink(
        &mut client,
        STREAM,
        7,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    // More arrives while it is down, which must not change what it sends again.
    send(&mut client, STREAM, b"later", 2).await;
    let seen = receiver.wait_for_count(3).await;
    assert_eq!(hook.kill_count.load(Ordering::SeqCst), 1);
    let want = (7, first_id, first_id + 4);
    assert_eq!(seen[0].triple(), want);
    assert_eq!(seen[1].triple(), want, "redelivered as the same delivery");
    assert_eq!(seen[0].body, seen[1].body);
    assert_ne!(seen[2].triple(), want);
    drop(client);
    harness.finish();
}

// ---------- ordered ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_delivery_is_in_flight_by_default() {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Hold);
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    let mut ids = Vec::new();
    for seq in 1..=4 {
        ids.push(send(&mut client, STREAM, format!("{seq}").as_bytes(), seq).await);
    }
    receiver.wait_for_count(1).await;
    receiver.stays_at(1).await;
    assert_eq!(
        sink_of(&mut client, STREAM, 1).await.1,
        0,
        "nothing acked yet"
    );
    receiver.answer(Answer::Status(200));
    receiver.release();
    let seen = receiver.wait_for_count(4).await;
    let firsts: Vec<u64> = seen.iter().map(|seen| seen.triple().1).collect();
    assert_eq!(firsts, ids, "in id order");
    assert_eq!(receiver.peak_in_flight.load(Ordering::SeqCst), 1);
    wait_for_cursor(&mut client, STREAM, 1, ids[3]).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_concurrency_respects_its_limit_and_the_cursor_waits_for_the_oldest() {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Hold);
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.concurrency = 3;
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    let mut ids = Vec::new();
    for seq in 1..=8 {
        ids.push(send(&mut client, STREAM, b"x", seq).await);
    }
    receiver.wait_for_count(3).await;
    receiver.stays_at(3).await;
    assert_eq!(receiver.peak_in_flight.load(Ordering::SeqCst), 3);
    receiver.answer(Answer::Status(200));
    receiver.release();
    let seen = receiver.wait_for_count(8).await;
    assert!(receiver.peak_in_flight.load(Ordering::SeqCst) <= 3);
    let mut firsts: Vec<u64> = seen.iter().map(|seen| seen.triple().1).collect();
    firsts.sort_unstable();
    assert_eq!(firsts, ids, "each exactly once");
    wait_for_cursor(&mut client, STREAM, 1, ids[7]).await;
    drop(client);
    harness.finish();
}

// ---------- failures ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retryable_errors_back_off_exponentially_then_too_many_pause() {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Status(503));
    let mut config = config();
    config.sink.max_failure_count = 6;
    let harness = Harness::new(config);
    let node = harness.node();
    let clock = harness.clock.clone();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"x", 1).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    receiver.wait_for_count(1).await;
    // 1, 2, 4, 8, then the limit of 8 again, on the scripted clock alone.
    for (done, wait_ms) in [1000u64, 2000, 4000, 8000, 8000].into_iter().enumerate() {
        wait_until("the failure to be reported", || {
            receiver.count() == done + 1
        })
        .await;
        tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
        clock.advance(wait_ms - 1);
        receiver.stays_at(done + 1).await;
        clock.advance(1);
        receiver.wait_for_count(done + 2).await;
    }
    let reason = wait_for_pause(&mut client, STREAM, 1).await;
    assert!(reason.contains("6 failures"), "{reason}");
    assert!(reason.contains("503"), "{reason}");
    clock.advance(1_000_000);
    receiver.stays_at(6).await;
    let (spec, cursor) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(spec.failure_count, 6);
    assert_eq!(cursor, 0);
    let same: Vec<_> = receiver.seen().iter().map(Seen::triple).collect();
    assert!(same.windows(2).all(|pair| pair[0] == pair[1]));

    // A resume starts over, and delivers once the destination is back.
    receiver.answer(Answer::Status(200));
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: false,
            },
        )
        .await
        .unwrap();
    receiver.wait_for_count(7).await;
    let (spec, _) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(spec.pause_reason, None);
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_errors_pause_at_once_and_a_redirect_is_not_followed() {
    for status in [404, 400, 302] {
        let receiver = Receiver::start().await;
        receiver.answer(Answer::Status(status));
        let harness = Harness::new(config());
        let node = harness.node();
        let mut client = node.client(0).await;
        make_stream(&mut client, STREAM).await;
        send(&mut client, STREAM, b"x", 1).await;
        put_sink(
            &mut client,
            STREAM,
            1,
            deliver_spec(&receiver.url()),
            SinkStart::Earliest,
        )
        .await;
        let reason = wait_for_pause(&mut client, STREAM, 1).await;
        assert!(reason.contains(&status.to_string()), "{reason}");
        receiver.stays_at(1).await;
        drop(client);
        harness.finish();
    }
}

// ---------- webhook.deliver ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliver_signs_stored_bytes_and_skips_system_records_and_filtered_ones() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.filter = Some("eq:255:1".to_owned());
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;

    // Options 1 matches and 2 doesn't, and bytes the server must not look into go through.
    let opaque: Vec<u8> = (0..=255).collect();
    let mut seq = 0;
    let mut last = 0;
    for (options, data) in [
        (1u64, &opaque[..]),
        (2, b"no"),
        (1, b"yes"),
        (1, b"padded."),
    ] {
        seq += 1;
        let records = record_with(options, data);
        client
            .publish(publish(STREAM, &records, seq, 1))
            .await
            .unwrap();
        last = expect_ack(&mut client).await.1;
    }
    // A state change adds a system record, which no sink sends on (D28).
    let doc = cbor(&map(&[("a", int(1))]));
    let state_records = record_with(1, b"with state");
    let mut with_state = publish(STREAM, &state_records, seq + 1, 2);
    with_state.state = Some(StateChange {
        expect_ver: 0,
        op: StateOp::Set,
        doc,
    });
    client.publish(with_state).await.unwrap();
    let (_, state_first, count) = expect_ack(&mut client).await;
    assert_eq!(count, 2);
    last = last.max(state_first);

    let seen = receiver.wait_for_count(4).await;
    receiver.stays_at(4).await;
    let stored = stored_bytes(node, STREAM, last, false).await;
    let mut wanted = Vec::new();
    for record in format::Records::new(&stored) {
        let record = record.unwrap();
        assert_ne!(record.header.author, 0, "this read excludes system records");
        if record.header.options == 1 {
            let mut bytes = record.header.to_bytes().to_vec();
            bytes.extend_from_slice(record.data);
            bytes.resize(bytes.len().next_multiple_of(8), 0);
            wanted.push(bytes);
        }
    }
    assert_eq!(wanted.len(), 4);
    for (seen, wanted) in seen.iter().zip(&wanted) {
        assert_eq!(&seen.body, wanted, "the delivery is the stored bytes");
        let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
        mac.update(&seen.body);
        let hex: String = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(seen.headers["x-signature"], hex);
        assert_eq!(seen.headers["x-sink-id"], "1");
        let header = format::RecordHeader::read(&seen.body).unwrap();
        assert_eq!(seen.triple(), (1, header.id, header.last_id()));
    }
    assert_eq!(seen[0].data(), opaque);
    // The filter's skipped records and the system record still move the cursor, in the end.
    wait_for_cursor(&mut client, STREAM, 1, seen[3].triple().2).await;
    drop(client);
    harness.finish();
}

struct FixedDns(HashMap<String, Vec<SocketAddr>>);

impl SinkResolver for FixedDns {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        _port: u16,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<Vec<SocketAddr>>> + Send + 'a>,
    > {
        let found = self
            .0
            .get(host)
            .cloned()
            .ok_or_else(|| std::io::Error::other("no such name"));
        Box::pin(async move { found })
    }
}

fn dns(entries: &[(&str, &[&str])]) -> Arc<FixedDns> {
    let mut names = HashMap::new();
    for (name, addresses) in entries {
        let addresses = addresses
            .iter()
            .map(|address| address.parse().unwrap())
            .collect();
        names.insert((*name).to_owned(), addresses);
    }
    Arc::new(FixedDns(names))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_addresses_are_blocked_by_default() {
    let receiver = Receiver::start().await;
    let port = receiver.address.port();
    let mut config = config();
    config.sink.allow_private_addresses = false;
    config.sink.resolver = Some(dns(&[
        ("rebound.example", &["10.0.0.5:80"]),
        ("meta.example", &["169.254.169.254:80"]),
        ("v6.example", &["[fd00::1]:80"]),
        ("mapped.example", &["[::ffff:127.0.0.1]:80"]),
        // One private answer in several is enough, as a connection could land on it.
        ("mixed.example", &["8.8.8.8:80", "192.168.0.1:80"]),
    ]));
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"x", 1).await;
    let urls = [
        format!("http://127.0.0.1:{port}/"),
        format!("http://[::1]:{port}/"),
        format!("http://[::ffff:127.0.0.1]:{port}/"),
        "http://10.1.2.3/".to_owned(),
        "http://rebound.example/".to_owned(),
        "http://meta.example/".to_owned(),
        "http://v6.example/".to_owned(),
        "http://mapped.example/".to_owned(),
        "http://mixed.example/".to_owned(),
    ];
    for (at, url) in urls.iter().enumerate() {
        let sink_id = at as u64 + 1;
        put_sink(
            &mut client,
            STREAM,
            sink_id,
            deliver_spec(url),
            SinkStart::Earliest,
        )
        .await;
        let reason = wait_for_pause(&mut client, STREAM, sink_id).await;
        assert!(reason.contains("private address"), "{url}: {reason}");
    }
    receiver.stays_at(0).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_name_for_a_loopback_address_is_blocked_too() {
    let receiver = Receiver::start().await;
    let mut config = config();
    config.sink.allow_private_addresses = false;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"x", 1).await;
    let url = format!("http://localhost:{}/", receiver.address.port());
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&url),
        SinkStart::Earliest,
    )
    .await;
    let reason = wait_for_pause(&mut client, STREAM, 1).await;
    assert!(reason.contains("private address"), "{reason}");
    receiver.stays_at(0).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_node_setting_allows_private_addresses_and_names() {
    let receiver = Receiver::start().await;
    let at = receiver.address;
    let mut config = config();
    config.sink.resolver = Some(dns(&[("inside.example", &[&at.to_string()[..]])]));
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"x", 1).await;
    let url = format!("http://inside.example:{}/", at.port());
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&url),
        SinkStart::Earliest,
    )
    .await;
    receiver.wait_for_count(1).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhooks_work_over_tls_with_a_trusted_certificate_and_fail_without_one() {
    let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    let tls = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.der().clone()],
        rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
    )
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    let plain = Receiver::start().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let serving = plain.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let (acceptor, serving) = (acceptor.clone(), serving.clone());
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(socket).await {
                    serve_one(tls, serving).await;
                }
            });
        }
    });
    let url = format!("https://localhost:{port}/secure");

    let mut trusting = config();
    trusting.sink.extra_roots_pem = Some(cert.pem().into_bytes());
    let harness = Harness::new(trusting);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"over tls", 1).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&url),
        SinkStart::Earliest,
    )
    .await;
    let seen = plain.wait_for_count(1).await;
    assert_eq!(seen[0].data(), b"over tls");
    drop(client);
    harness.finish();

    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"over tls", 1).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&url),
        SinkStart::Earliest,
    )
    .await;
    let started = Instant::now();
    loop {
        let (spec, _) = sink_of(&mut client, STREAM, 1).await;
        if spec.failure_count > 0 {
            assert!(spec.last_error.is_some());
            break;
        }
        assert!(started.elapsed() < Duration::from_millis(WAIT_MS));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    plain.stays_at(1).await;
    drop(client);
    harness.finish();
}

// ---------- webhook.notify ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_sends_its_three_fields_and_honours_its_rate_under_a_burst() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let node = harness.node();
    let clock = harness.clock.clone();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let spec = SinkSpec {
        kind: "webhook.notify".to_owned(),
        url: Some(receiver.url()),
        interval_ms: 1_000,
        on_expire: "stop".to_owned(),
        concurrency: 1,
        ..SinkSpec::default()
    };
    put_sink(&mut client, STREAM, 3, spec, SinkStart::Earliest).await;

    let mut last = send(&mut client, STREAM, b"first", 1).await;
    receiver.wait_for_count(1).await;

    // A burst, all inside one interval: nothing more goes until the clock says so.
    let burst = 500u64;
    for seq in 2..2 + burst {
        last = send(&mut client, STREAM, b"burst", seq).await;
    }
    clock.advance(999);
    receiver.stays_at(1).await;
    clock.advance(1);
    receiver.wait_for_count(2).await;
    let mut sends = 2;
    let mut total;
    // The burst read in pieces can take a few more sends, each one interval apart.
    loop {
        let seen = receiver.wait_for_count(sends).await;
        total = seen
            .iter()
            .map(|seen| json_number(&seen.body, "unread"))
            .sum::<u64>();
        if total == burst + 1 {
            break;
        }
        clock.advance(999);
        receiver.stays_at(sends).await;
        clock.advance(1);
        sends += 1;
    }
    assert_eq!(total, burst + 1, "every item counted exactly once");
    assert!(sends <= 4, "{sends} sends for one burst");

    let seen = receiver.seen();
    let body = String::from_utf8(seen[sends - 1].body.clone()).unwrap();
    assert!(body.contains(&format!("\"stream\":\"{STREAM}\"")), "{body}");
    assert!(body.contains(&format!("\"head\":\"{last}\"")), "{body}");
    assert_eq!(seen[sends - 1].headers["content-type"], "application/json");
    assert_eq!(seen[sends - 1].triple().2, last);
    wait_for_cursor(&mut client, STREAM, 3, last).await;
    drop(client);
    harness.finish();
}

fn json_number(body: &[u8], name: &str) -> u64 {
    let text = String::from_utf8_lossy(body);
    let after = text.split(&format!("\"{name}\":")).nth(1).unwrap();
    after
        .trim_matches('"')
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

// ---------- forward ----------

struct ForwardRun {
    sent: BTreeMap<u64, (u64, Vec<u8>)>,
    last_id: u64,
}

/// A few records, one of them several items and a system record among them.
async fn publish_for_forward(client: &mut Client, stream: &str, count: u64) -> ForwardRun {
    let mut sent = BTreeMap::new();
    let mut last_id = 0;
    for seq in 1..=count {
        let data = format!("{stream} {seq}").into_bytes();
        let mut records = Vec::new();
        add_record(&mut records, seq, &data, if seq % 3 == 0 { 4 } else { 1 });
        client
            .publish(publish(stream, &records, seq, 2))
            .await
            .unwrap();
        let (_, first_id, item_count) = expect_ack(client).await;
        sent.insert(first_id, (u64::from(item_count), data));
        last_id = first_id + u64::from(item_count) - 1;
    }
    let change = client
        .state_op(server::StateOpRequest {
            stream,
            req: 1,
            author: NonZeroAuthor::OTHER,
            expect_ver: 0,
            op: StateOp::Set,
            doc: cbor(&map(&[("n", int(1))])),
        })
        .await;
    change.unwrap();
    let _ = expect_reply(client).await;
    ForwardRun { sent, last_id }
}

struct NonZeroAuthor;

impl NonZeroAuthor {
    const OTHER: std::num::NonZeroU64 = std::num::NonZeroU64::new(5).unwrap();
}

/// Each source record appears once in `target`, with its author and data, in the source's order.
async fn check_forwarded(node: &Node, target: &str, run: &ForwardRun) {
    let got = target_records(node, target, run.sent.len()).await;
    assert_eq!(got.len(), run.sent.len(), "no record missing or duplicated");
    let stored = stored_bytes(node, target, got[got.len() - 1].1, false).await;
    let headers: Vec<_> = format::Records::new(&stored)
        .map(|record| record.unwrap())
        .collect();
    assert_eq!(headers.len(), run.sent.len());
    for (record, (_, (item_count, data))) in headers.iter().zip(&run.sent) {
        assert_eq!(record.data, &data[..]);
        assert_eq!(record.header.author, AUTHOR.get(), "the original author");
        assert_eq!(u64::from(record.header.count), *item_count);
    }
    assert!(got.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

fn check_wal_fields(harness: &Harness, target: &str, sink_id: u64, run: &ForwardRun) {
    let entries = harness.wal.stream_entries(target);
    let mut seqs: Vec<u64> = entries.iter().map(|entry| entry.producer_seq).collect();
    seqs.dedup();
    assert!(entries.iter().all(|entry| entry.producer_id == sink_id));
    let wanted: Vec<u64> = run.sent.keys().copied().collect();
    assert_eq!(seqs, wanted, "producer_seq is the source first_id");
}

const TARGET: &str = "sink.target";

async fn forward_with_kills(kills: Vec<(SinkStep, u32)>, count: u64) {
    let hook = KillScript::with(&kills);
    let mut config = config();
    config.sink.hook = Some(hook.clone());
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let run = publish_for_forward(&mut client, STREAM, count).await;
    put_sink(
        &mut client,
        STREAM,
        44,
        forward_spec(TARGET),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, STREAM, 44, run.last_id).await;
    check_forwarded(node, TARGET, &run).await;
    check_wal_fields(&harness, TARGET, 44, &run);
    // A step is passed once per record at least, and a kill past that may never come. Every step
    // named has fired, though, so the run did get killed there.
    let mut steps: Vec<String> = kills.iter().map(|(step, _)| format!("{step:?}")).collect();
    steps.sort();
    steps.dedup();
    assert!(hook.kill_count.load(Ordering::SeqCst) >= steps.len() as u64);
    drop(client);
    harness.finish();
}

fn kill_step() -> impl Strategy<Value = SinkStep> {
    prop_oneof![
        Just(SinkStep::BeforeDelivery),
        Just(SinkStep::AfterPublish),
        Just(SinkStep::AfterDelivery),
        Just(SinkStep::AfterCursor),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Killed at any step, any number of times: no duplicate in the target and none missing.
    #[test]
    fn forward_killed_at_any_step_gives_no_duplicate(
        kills in proptest::collection::vec((kill_step(), 1u32..3), 1..5),
        count in 1u64..6,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(forward_with_kills(kills, count));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_a_lost_ack_is_dropped_by_normal_idempotency() {
    let hook = KillScript::with(&[(SinkStep::AfterPublish, 3)]);
    let mut config = config();
    config.sink.hook = Some(hook.clone());
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let run = publish_for_forward(&mut client, STREAM, 2).await;
    put_sink(
        &mut client,
        STREAM,
        44,
        forward_spec(TARGET),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, STREAM, 44, run.last_id).await;
    check_forwarded(node, TARGET, &run).await;
    assert!(total_stat(node, |stats| stats.duplicate_count).await >= 3);
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn several_streams_fan_in_to_one() {
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    let mut wanted = Vec::new();
    for at in 0..4u64 {
        let stream = format!("fan.source.{at}");
        make_stream(&mut client, &stream).await;
        let run = publish_for_forward(&mut client, &stream, 3).await;
        put_sink(
            &mut client,
            &stream,
            100 + at,
            forward_spec("fan.all"),
            SinkStart::Earliest,
        )
        .await;
        wait_for_cursor(&mut client, &stream, 100 + at, run.last_id).await;
        wanted.extend(run.sent.values().map(|(_, data)| data.clone()));
    }
    let got = target_records(node, "fan.all", wanted.len()).await;
    let mut got_data: Vec<Vec<u8>> = got.iter().map(|record| record.3.clone()).collect();
    got_data.sort();
    wanted.sort();
    assert_eq!(got_data, wanted);
    assert!(got.windows(2).all(|pair| pair[0].0 < pair[1].0));
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_sources_with_one_sink_id_on_one_target_cannot_lose_records_to_each_other() {
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, "same.a").await;
    make_stream(&mut client, "same.b").await;
    put_sink(
        &mut client,
        "same.a",
        9,
        forward_spec("same.all"),
        SinkStart::Earliest,
    )
    .await;
    let id = send(&mut client, "same.a", b"a", 1).await;
    wait_for_cursor(&mut client, "same.a", 9, id).await;
    put_sink(
        &mut client,
        "same.b",
        9,
        forward_spec("same.all"),
        SinkStart::Earliest,
    )
    .await;
    let reason = wait_for_pause(&mut client, "same.b", 9).await;
    assert!(reason.contains("duplicates"), "{reason}");
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_into_the_servers_own_stream_pauses() {
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"x", 1).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        forward_spec(server::MANIFEST_STREAM),
        SinkStart::Earliest,
    )
    .await;
    let reason = wait_for_pause(&mut client, STREAM, 1).await;
    assert!(reason.contains("refused"), "{reason}");
    drop(client);
    harness.finish();
}

// ---------- archive ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_objects_hold_byte_identical_records_on_a_local_file_system() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let mut config = config();
    config.sink.archive_store = Some(store.clone());
    config.sink.archive_window_ms = 1_000;
    let harness = Harness::new(config);
    let node = harness.node();
    let clock = harness.clock.clone();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let spec = SinkSpec {
        kind: "archive".to_owned(),
        on_expire: "stop".to_owned(),
        concurrency: 1,
        ..SinkSpec::default()
    };
    put_sink(&mut client, STREAM, 5, spec, SinkStart::Earliest).await;

    // Three windows' worth, with data bytes the server has no business reading.
    let mut last = 0;
    let mut seq = 0;
    for _window in 0..3 {
        for at in 0..5u8 {
            seq += 1;
            let data: Vec<u8> = (0..=at).map(|byte| byte ^ 0xA5).collect();
            last = send(&mut client, STREAM, &data, seq).await;
        }
        clock.advance(1_000);
    }
    wait_for_cursor(&mut client, STREAM, 5, last).await;

    let prefix = object_store::path::Path::from_iter(["archive", STREAM, "5"]);
    let listed = store.list_with_delimiter(Some(&prefix)).await.unwrap();
    let mut names: Vec<_> = listed
        .objects
        .iter()
        .map(|object| object.location.clone())
        .collect();
    names.sort();
    assert_eq!(names.len(), 3, "one object per window: {names:?}");
    let mut archived = Vec::new();
    for name in &names {
        let bytes = store.get(name).await.unwrap().bytes().await.unwrap();
        let ids: Vec<u64> = parse_records(&bytes)
            .iter()
            .map(|record| record.0)
            .collect();
        let file = name.filename().unwrap();
        assert_eq!(
            file,
            format!("{:020}-{:020}.records", ids[0], ids[ids.len() - 1]),
            "the name is the delivery's first and last id"
        );
        archived.extend_from_slice(&bytes);
    }
    let stored = stored_bytes(node, STREAM, last, false).await;
    assert_eq!(archived, stored, "the same bytes as the stream holds");
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_killed_after_its_put_doesnt_write_a_second_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let hook = KillScript::with(&[(SinkStep::AfterDelivery, 2)]);
    let mut config = config();
    config.sink.archive_store = Some(store.clone());
    config.sink.archive_window_ms = 1_000;
    config.sink.hook = Some(hook.clone());
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let last = send(&mut client, STREAM, b"one", 1).await;
    harness.clock.advance(1_000);
    let spec = SinkSpec {
        kind: "archive".to_owned(),
        on_expire: "stop".to_owned(),
        concurrency: 1,
        ..SinkSpec::default()
    };
    put_sink(&mut client, STREAM, 5, spec, SinkStart::Earliest).await;
    wait_for_cursor(&mut client, STREAM, 5, last).await;
    assert_eq!(hook.kill_count.load(Ordering::SeqCst), 2);
    let prefix = object_store::path::Path::from_iter(["archive", STREAM, "5"]);
    let listed = store.list_with_delimiter(Some(&prefix)).await.unwrap();
    assert_eq!(listed.objects.len(), 1);
    drop(client);
    harness.finish();
}

// ---------- budgets ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sink_stays_within_its_byte_budget_and_loses_nothing_past_it() {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Hold);
    let hook = KillScript::with(&[]);
    let mut config = config();
    config.sink.hook = Some(hook.clone());
    config.subscriber_queue_bytes = 64 * 1024;
    config.connection_window_bytes = 64 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.budget_bytes = 16 * 1024;
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    let mut ids = Vec::new();
    let data = vec![7u8; 2048];
    for seq in 1..=300 {
        ids.push(send(&mut client, STREAM, &data, seq).await);
    }
    receiver.wait_for_count(1).await;
    tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
    let peak = hook.peak_held_bytes.load(Ordering::SeqCst);
    // One frame can pass it (D191), and a frame is at most the fair share.
    assert!(peak <= 16 * 1024 + 256 * 1024, "held {peak} bytes");
    assert!(peak >= 16 * 1024, "held {peak} bytes");
    receiver.answer(Answer::Status(200));
    receiver.release();
    let seen = receiver.wait_for_count(300).await;
    let firsts: Vec<u64> = seen.iter().map(|seen| seen.triple().1).collect();
    assert_eq!(firsts, ids, "in order, with nothing lost past the budget");
    drop(client);
    harness.finish();
}

// ---------- on_expire ----------

const DAY_MS: u64 = 24 * 3_600_000;

async fn stuck_sink(on_expire: &str) -> (Harness, Receiver, Client, u64) {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Status(503));
    let mut config = config();
    config.sink.expire_warn_ms = 3_600_000;
    config.sink.retry_max_ms = 1_000;
    config.sink.max_failure_count = u32::MAX;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let first = send(&mut client, STREAM, b"old", 1).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.on_expire = on_expire.to_owned();
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    receiver.wait_for_count(1).await;
    (harness, receiver, client, first)
}

async fn wait_for_warning(client: &mut Client) -> u64 {
    let started = Instant::now();
    loop {
        let (spec, _) = sink_of(client, STREAM, 1).await;
        if let Some(id) = spec.expiry_warning_id {
            return id;
        }
        assert!(
            started.elapsed() < Duration::from_millis(WAIT_MS),
            "no warning"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_expire_stop_warns_then_halts() {
    let (harness, receiver, mut client, first) = stuck_sink("stop").await;
    // A little before the warning is due, none comes.
    harness.clock.advance(DAY_MS - 3_600_000 - 1_000);
    tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
    assert_eq!(
        sink_of(&mut client, STREAM, 1).await.0.expiry_warning_id,
        None
    );
    harness.clock.advance(1_000);
    assert_eq!(wait_for_warning(&mut client).await, first);
    assert!(!sink_of(&mut client, STREAM, 1).await.0.paused);

    harness.clock.advance(3_600_000);
    let reason = wait_for_pause(&mut client, STREAM, 1).await;
    assert!(reason.contains("expired"), "{reason}");
    let (spec, cursor) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(cursor, 0, "nothing passed");
    assert_eq!(spec.gap_count, 0);
    let count = receiver.count();
    harness.clock.advance(60_000);
    receiver.stays_at(count).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_expire_skip_continues_and_records_the_gap() {
    let (harness, receiver, mut client, first) = stuck_sink("skip").await;
    harness.clock.advance(DAY_MS - 3_600_000);
    assert_eq!(wait_for_warning(&mut client).await, first);
    harness.clock.advance(3_600_000);
    let started = Instant::now();
    loop {
        let (spec, cursor) = sink_of(&mut client, STREAM, 1).await;
        if spec.gap_count == 1 && cursor == first {
            assert_eq!(spec.gaps.len(), 1);
            assert_eq!(
                (spec.gaps[0].first_id, spec.gaps[0].last_id),
                (first, first)
            );
            assert!(!spec.paused);
            break;
        }
        assert!(started.elapsed() < Duration::from_millis(WAIT_MS));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // And it goes on with what is new.
    receiver.answer(Answer::Status(200));
    let new = send(&mut client, STREAM, b"new", 2).await;
    wait_for_cursor(&mut client, STREAM, 1, new).await;
    assert_eq!(receiver.seen().last().unwrap().data(), b"new");
    assert_eq!(
        sink_of(&mut client, STREAM, 1).await.0.expiry_warning_id,
        None
    );
    drop(client);
    harness.finish();
}

/// A read that the server itself refuses with error 7, rather than a record seen to be old.
async fn behind_retention(on_expire: &str) -> (Harness, Receiver, Client, u64, u64) {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let old = send(&mut client, STREAM, b"old", 1).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.on_expire = on_expire.to_owned();
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Id(old - 1)).await;
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: true,
            },
        )
        .await
        .unwrap();
    // A delivery may have been on its way when the pause arrived, which is the point of "finishes
    // first", so set the sink back to before it.
    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 1,
                start: SinkStart::Id(old - 1),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        harness.node().seal().await,
        server::SealOutcome::Uploaded { .. }
    ));
    wait_for_memory_to_empty(harness.node(), STREAM).await;
    harness.clock.advance(2 * DAY_MS);
    let new = send(&mut client, STREAM, b"new", 2).await;
    let (_, cursor) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(cursor, old - 1);
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: false,
            },
        )
        .await
        .unwrap();
    (harness, receiver, client, old, new)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_refused_as_expired_stops_a_stop_sink() {
    let (harness, receiver, mut client, _, _) = behind_retention("stop").await;
    let reason = wait_for_pause(&mut client, STREAM, 1).await;
    assert!(reason.contains("expired"), "{reason}");
    receiver.stays_at(0).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_refused_as_expired_is_a_gap_to_a_skip_sink() {
    let (harness, receiver, mut client, old, new) = behind_retention("skip").await;
    let seen = receiver.wait_for_count(1).await;
    assert_eq!(seen[0].data(), b"new");
    wait_for_cursor(&mut client, STREAM, 1, new).await;
    let (spec, _) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(spec.gap_count, 1);
    assert_eq!(spec.gaps[0].first_id, old);
    assert_eq!(spec.gaps[0].last_id, new - 1);
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_sink_warns_too_and_a_skip_acts_on_resume() {
    let receiver = Receiver::start().await;
    let mut config = config();
    config.sink.expire_warn_ms = 3_600_000;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.on_expire = "skip".to_owned();
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    let delivered = send(&mut client, STREAM, b"delivered", 1).await;
    wait_for_cursor(&mut client, STREAM, 1, delivered).await;
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: true,
            },
        )
        .await
        .unwrap();
    let unread = send(&mut client, STREAM, b"unread", 2).await;
    harness.clock.advance(DAY_MS - 3_600_000);
    assert_eq!(wait_for_warning(&mut client).await, delivered);

    harness.clock.advance(3_600_000 + 1);
    let count = receiver.count();
    receiver.stays_at(count).await;
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: false,
            },
        )
        .await
        .unwrap();
    wait_for_cursor(&mut client, STREAM, 1, unread).await;
    let (spec, _) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(spec.gap_count, 1);
    assert_eq!(spec.gaps[0].last_id, unread);
    drop(client);
    harness.finish();
}

// ---------- lifecycle ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_starts_at_now_earliest_and_an_id() {
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let first = send(&mut client, STREAM, b"1", 1).await;
    let second = send(&mut client, STREAM, b"2", 2).await;

    let (now, earliest, at_id) = (
        Receiver::start().await,
        Receiver::start().await,
        Receiver::start().await,
    );
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&now.url()),
        SinkStart::Now,
    )
    .await;
    put_sink(
        &mut client,
        STREAM,
        2,
        deliver_spec(&earliest.url()),
        SinkStart::Earliest,
    )
    .await;
    put_sink(
        &mut client,
        STREAM,
        3,
        deliver_spec(&at_id.url()),
        SinkStart::Id(first),
    )
    .await;
    let third = send(&mut client, STREAM, b"3", 3).await;

    let data = |seen: Vec<Seen>| seen.iter().map(Seen::data).collect::<Vec<_>>();
    assert_eq!(data(now.wait_for_count(1).await), [b"3"]);
    assert_eq!(data(earliest.wait_for_count(3).await), [b"1", b"2", b"3"]);
    assert_eq!(data(at_id.wait_for_count(2).await), [b"2", b"3"]);
    for sink_id in 1..=3 {
        wait_for_cursor(&mut client, STREAM, sink_id, third).await;
    }
    let _ = second;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_finishes_what_is_in_flight_keeps_the_cursor_and_starts_at_the_next_record() {
    let (old, new) = (Receiver::start().await, Receiver::start().await);
    old.answer(Answer::Hold);
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&old.url()),
        SinkStart::Earliest,
    )
    .await;
    let first = send(&mut client, STREAM, b"1", 1).await;
    let second = send(&mut client, STREAM, b"2", 2).await;
    old.wait_for_count(1).await;

    // It carries on as it was until the old request ends.
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&new.url()),
        SinkStart::Earliest,
    )
    .await;
    new.stays_at(0).await;
    assert_eq!(sink_of(&mut client, STREAM, 1).await.1, 0);
    old.answer(Answer::Status(200));
    old.release();

    let seen = new.wait_for_count(1).await;
    assert_eq!(
        seen[0].data(),
        b"2",
        "the next record, not the finished one again"
    );
    assert_eq!(old.count(), 1, "nothing else goes to the old address");
    wait_for_cursor(&mut client, STREAM, 1, second).await;
    assert!(second > first);
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_resume_reset_and_delete() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    let first = send(&mut client, STREAM, b"1", 1).await;
    receiver.wait_for_count(1).await;
    wait_for_cursor(&mut client, STREAM, 1, first).await;

    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: true,
            },
        )
        .await
        .unwrap();
    let second = send(&mut client, STREAM, b"2", 2).await;
    receiver.stays_at(1).await;
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(receiver.wait_for_count(2).await[1].data(), b"2");
    wait_for_cursor(&mut client, STREAM, 1, second).await;

    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 1,
                start: SinkStart::Earliest,
            },
        )
        .await
        .unwrap();
    let seen = receiver.wait_for_count(4).await;
    assert_eq!(seen[2].data(), b"1");
    assert_eq!(seen[3].data(), b"2");
    wait_for_cursor(&mut client, STREAM, 1, second).await;

    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 1,
                start: SinkStart::Id(first),
            },
        )
        .await
        .unwrap();
    assert_eq!(receiver.wait_for_count(5).await[4].data(), b"2");
    wait_for_cursor(&mut client, STREAM, 1, second).await;

    client
        .sink_op(STREAM, SinkOp::Delete { sink_id: 1 })
        .await
        .unwrap();
    send(&mut client, STREAM, b"3", 3).await;
    receiver.stays_at(5).await;
    assert!(client.stream_info(STREAM).await.unwrap().sinks.is_empty());
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_during_a_delivery_is_not_undone_when_that_delivery_ends() {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Hold);
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let first = send(&mut client, STREAM, b"1", 1).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    receiver.wait_for_count(1).await;
    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 1,
                start: SinkStart::Earliest,
            },
        )
        .await
        .unwrap();
    receiver.answer(Answer::Status(200));
    receiver.release();
    let seen = receiver.wait_for_count(2).await;
    assert_eq!(
        seen[1].triple(),
        seen[0].triple(),
        "the reset went back before it"
    );
    wait_for_cursor(&mut client, STREAM, 1, first).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_stream_stops_its_sinks() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    send(&mut client, STREAM, b"1", 1).await;
    receiver.wait_for_count(1).await;
    assert!(client.delete_stream(STREAM).await);
    // Made again under its old name, it has no sink.
    send(&mut client, STREAM, b"2", 2).await;
    receiver.stays_at(1).await;
    drop(client);
    harness.finish();
}

// ---------- moving with the shard ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sink_moves_with_its_shard() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    let shard = server::shard_of(STREAM, node.virtual_shard_count());
    let mut ids = Vec::new();
    for round in 0..8u64 {
        for seq in 0..10u64 {
            ids.push(send(&mut client, STREAM, b"x", round * 10 + seq + 1).await);
        }
        // Each move lands wherever the sink has got to.
        assert!(node.move_shard(shard, (round % 2) as usize).await);
        let (spec, _) = sink_of(&mut client, STREAM, 1).await;
        assert_eq!(
            spec.kind, "webhook.deliver",
            "the sink went with the stream"
        );
    }
    let seen = receiver.wait_for_count(ids.len()).await;
    let firsts: Vec<u64> = seen.iter().map(|seen| seen.triple().1).collect();
    assert_eq!(firsts, ids, "no gap, duplicate or reordering across moves");
    wait_for_cursor(&mut client, STREAM, 1, *ids.last().unwrap()).await;

    // Lifecycle ops find it on its new core, and so does a move while it is paused.
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: true,
            },
        )
        .await
        .unwrap();
    assert!(node.move_shard(shard, 1).await);
    let mid = send(&mut client, STREAM, b"paused", 1000).await;
    receiver.stays_at(ids.len()).await;
    assert!(node.move_shard(shard, 0).await);
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: false,
            },
        )
        .await
        .unwrap();
    wait_for_cursor(&mut client, STREAM, 1, mid).await;
    drop(client);
    harness.finish();
}

// ---------- restarts ----------

struct Disk {
    dir: tempfile::TempDir,
    store: Arc<ScriptStore>,
    clock: ManualClock,
}

impl Disk {
    fn new() -> Disk {
        Disk {
            dir: tempfile::tempdir().unwrap(),
            store: Arc::new(ScriptStore::new()),
            clock: ManualClock::new(START_MS),
        }
    }

    fn start(&self, config: NodeConfig) -> Node {
        Node::recover(
            config,
            RecoverConfig {
                wal_dir: self.dir.path().join("wal"),
                metadata_dir: self.dir.path().join("metadata"),
                wal_preallocate_bytes: 1024 * 1024,
            },
            self.store.clone(),
            Arc::new(self.clock.clone()),
        )
        .unwrap()
        .0
    }
}

/// Everything a restart must bring back, after a crash-like stop with no last seal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_restores_every_sink_and_where_it_was() {
    let disk = Disk::new();
    let (running, paused, stopped, fresh) = (
        Receiver::start().await,
        Receiver::start().await,
        Receiver::start().await,
        Receiver::start().await,
    );
    let node = disk.start(config());
    let mut client = node.client(0).await;
    make_stream(&mut client, "keep.a").await;
    make_stream(&mut client, "keep.b").await;
    make_stream(&mut client, "keep.gone").await;
    let first = send(&mut client, "keep.a", b"1", 1).await;

    let mut filtered = deliver_spec(&running.url());
    filtered.filter = Some("any:1".to_owned());
    put_sink(&mut client, "keep.a", 1, filtered, SinkStart::Earliest).await;
    put_sink(
        &mut client,
        "keep.a",
        2,
        deliver_spec(&paused.url()),
        SinkStart::Now,
    )
    .await;
    client
        .sink_op(
            "keep.a",
            SinkOp::SetPaused {
                sink_id: 2,
                paused: true,
            },
        )
        .await
        .unwrap();
    // One paused by itself, whose reason comes back too.
    stopped.answer(Answer::Status(404));
    let reset_at = send(&mut client, "keep.b", b"b1", 1).await;
    let second = send(&mut client, "keep.b", b"b2", 2).await;
    put_sink(
        &mut client,
        "keep.b",
        3,
        deliver_spec(&stopped.url()),
        SinkStart::Earliest,
    )
    .await;
    let reason = wait_for_pause(&mut client, "keep.b", 3).await;
    // And a reset, which is held by the checkpoint as the lifecycle's other ops are.
    client
        .sink_op(
            "keep.b",
            SinkOp::Reset {
                sink_id: 3,
                start: SinkStart::Id(reset_at),
            },
        )
        .await
        .unwrap();
    // A gap reported, and a stream that has since lost its sink, which stays gone.
    client
        .sink_op(
            "keep.b",
            SinkOp::Report {
                sink_id: 3,
                report: SinkReport::Gap {
                    first_id: 5,
                    last_id: 6,
                },
            },
        )
        .await
        .unwrap();
    put_sink(
        &mut client,
        "keep.gone",
        4,
        deliver_spec(&fresh.url()),
        SinkStart::Now,
    )
    .await;
    client
        .sink_op("keep.gone", SinkOp::Delete { sink_id: 4 })
        .await
        .unwrap();
    running.wait_for_count(1).await;
    wait_for_cursor(&mut client, "keep.a", 1, first).await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    drop(node.shutdown());

    // The sinks start by themselves: nothing touches keep.a's or keep.b's streams.
    let node = disk.start(config());
    tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
    let mut client = node.client(0).await;
    let (spec, cursor) = sink_of(&mut client, "keep.a", 1).await;
    assert_eq!(spec.url.as_deref(), Some(&running.url()[..]));
    assert_eq!(spec.filter.as_deref(), Some("any:1"));
    assert_eq!(spec.secret.as_deref(), Some(SECRET));
    assert!(!spec.paused);
    assert_eq!(cursor, first);
    let (spec, cursor) = sink_of(&mut client, "keep.a", 2).await;
    assert!(spec.paused);
    assert_eq!(spec.pause_reason, None);
    assert_eq!(cursor, first, "a sink made at now");
    let (spec, cursor) = sink_of(&mut client, "keep.b", 3).await;
    assert!(spec.paused);
    assert_eq!(spec.pause_reason.as_deref(), Some(&reason[..]));
    assert_eq!(cursor, reset_at);
    assert_eq!(spec.gap_count, 1);
    assert_eq!(spec.gaps[0].first_id, 5);
    assert!(
        client
            .stream_info("keep.gone")
            .await
            .unwrap()
            .sinks
            .is_empty()
    );

    // A sink from before the restart is delivering without being asked.
    running.stays_at(1).await;
    let new = send(&mut client, "keep.a", b"2", 2).await;
    assert_eq!(running.wait_for_count(2).await[1].data(), b"2");
    wait_for_cursor(&mut client, "keep.a", 1, new).await;
    paused.stays_at(0).await;
    stopped.answer(Answer::Status(200));
    client
        .sink_op(
            "keep.b",
            SinkOp::SetPaused {
                sink_id: 3,
                paused: false,
            },
        )
        .await
        .unwrap();
    let seen = stopped.wait_for_count(2).await;
    assert_eq!(
        seen.last().unwrap().triple().1,
        second,
        "it resumes after its reset"
    );
    drop(client);
    drop(node.shutdown());
}

/// Without a last seal: a lifecycle change is answered only once a checkpoint holds it (D196).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lifecycle_change_outlives_a_stop_with_no_seal() {
    let disk = Disk::new();
    let receiver = Receiver::start().await;
    let node = disk.start(config());
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.on_expire = "skip".to_owned();
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    drop(client);
    drop(node.shutdown());

    let node = disk.start(config());
    let mut client = node.client(0).await;
    assert_eq!(sink_of(&mut client, STREAM, 1).await.0.on_expire, "skip");
    let id = send(&mut client, STREAM, b"after", 1).await;
    receiver.wait_for_count(1).await;
    wait_for_cursor(&mut client, STREAM, 1, id).await;
    drop(client);
    drop(node.shutdown());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_across_a_restart_with_a_stale_cursor_writes_no_duplicate() {
    let disk = Disk::new();
    let node = disk.start(config());
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let run = publish_for_forward(&mut client, STREAM, 3).await;
    put_sink(
        &mut client,
        STREAM,
        44,
        forward_spec(TARGET),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, STREAM, 44, run.last_id).await;
    // The cursor moved after the sink's last checkpoint, so the restart goes back to `earliest`
    // (D143), and every record is forwarded again.
    drop(client);
    drop(node.shutdown());

    let node = disk.start(config());
    let mut client = node.client(0).await;
    let (_, cursor) = sink_of(&mut client, STREAM, 44).await;
    assert!(cursor < run.last_id, "this test needs a cursor gone back");
    wait_for_cursor(&mut client, STREAM, 44, run.last_id).await;
    check_forwarded(&node, TARGET, &run).await;
    assert!(total_stat(&node, |stats| stats.duplicate_count).await >= 1);
    drop(client);
    drop(node.shutdown());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_config_is_read_for_the_expiry_check() {
    let (harness, _receiver, mut client, first) = stuck_sink("stop").await;
    client
        .configure(
            STREAM,
            StreamConfig {
                retention_ms: RetentionClass::Days7.length_ms(),
                ..StreamConfig::default()
            },
        )
        .await;
    tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
    harness.clock.advance(1_100);
    tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
    harness.clock.advance(DAY_MS + 1);
    tokio::time::sleep(Duration::from_millis(QUIET_MS)).await;
    let (spec, cursor) = sink_of(&mut client, STREAM, 1).await;
    assert!(
        !spec.paused,
        "seven days aren't up: {:?}",
        spec.pause_reason
    );
    assert_eq!(cursor, 0);
    assert_eq!(spec.expiry_warning_id, None);
    let _ = first;
    drop(client);
    harness.finish();
}

// ---------- regressions from the M9 review ----------

fn archive_spec(filter: Option<&str>) -> SinkSpec {
    SinkSpec {
        kind: "archive".to_owned(),
        on_expire: "stop".to_owned(),
        concurrency: 1,
        filter: filter.map(str::to_owned),
        ..SinkSpec::default()
    }
}

/// `(object name, the data of its records)`, in name order.
async fn archive_objects(
    store: &LocalFileSystem,
    stream: &str,
    sink_id: u64,
) -> Vec<(String, Vec<Vec<u8>>)> {
    let prefix = object_store::path::Path::from_iter(["archive", stream, &sink_id.to_string()]);
    let listed = store.list_with_delimiter(Some(&prefix)).await.unwrap();
    let mut objects = Vec::new();
    for object in listed.objects {
        let bytes = store
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let data = parse_records(&bytes)
            .into_iter()
            .map(|record| record.3)
            .collect();
        objects.push((object.location.to_string(), data));
    }
    objects.sort();
    objects
}

/// However the first read was framed, a repeat has the same `{first_id, last_id}`, so it lands
/// on the same name and every record is in one object.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_killed_after_its_put_repeats_the_same_batch_for_many_records() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let hook = KillScript::with(&[(SinkStep::AfterDelivery, 1)]);
    let mut config = config();
    config.sink.archive_store = Some(store.clone());
    config.sink.archive_window_ms = 1_000;
    config.sink.hook = Some(hook.clone());
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        5,
        archive_spec(None),
        SinkStart::Earliest,
    )
    .await;
    let payload = vec![7u8; 32 * 1024];
    let mut last = 0;
    for seq in 1..=150 {
        last = send(&mut client, STREAM, &payload, seq).await;
    }
    harness.clock.advance(1_000);
    wait_for_cursor(&mut client, STREAM, 5, last).await;
    assert_eq!(hook.kill_count.load(Ordering::SeqCst), 1);
    let objects = archive_objects(&store, STREAM, 5).await;
    let total: usize = objects.iter().map(|(_, data)| data.len()).sum();
    assert_eq!(
        (objects.len(), total),
        (1, 150),
        "{:?}",
        objects
            .iter()
            .map(|(name, data)| (name, data.len()))
            .collect::<Vec<_>>()
    );
    drop(client);
    harness.finish();
}

/// A window the clock has closed isn't closed for this sink until it has read all of it, however
/// a catch-up happens to be framed. No kill here: the batches themselves must not depend on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_catching_up_waits_to_read_the_whole_window() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let mut config = config();
    config.sink.archive_store = Some(store.clone());
    config.sink.archive_window_ms = 1_000;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let payload = vec![7u8; 4 * 1024];
    let mut last = 0;
    for seq in 1..=1_500 {
        last = send(&mut client, STREAM, &payload, seq).await;
    }
    harness.clock.advance(1_000);
    put_sink(
        &mut client,
        STREAM,
        5,
        archive_spec(None),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, STREAM, 5, last).await;
    let objects = archive_objects(&store, STREAM, 5).await;
    assert_eq!(objects.len(), 1, "{:?}", objects[0].0);
    assert_eq!(objects[0].1.len(), 1_500);
    drop(client);
    harness.finish();
}

/// The name holds the ends and not the filter, so a wider read can land on a name that holds
/// fewer records. What it can't do is pass as delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_name_taken_by_other_bytes_pauses_and_loses_nothing_silently() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let mut config = config();
    config.sink.archive_store = Some(store.clone());
    config.sink.archive_window_ms = 1_000;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut ids = Vec::new();
    for (seq, (options, data)) in [(1u64, b"r1"), (2, b"r2"), (1, b"r3")]
        .into_iter()
        .enumerate()
    {
        let records = record_with(options, data);
        client
            .publish(publish(STREAM, &records, seq as u64 + 1, 1))
            .await
            .unwrap();
        ids.push(expect_ack(&mut client).await.1);
    }
    harness.clock.advance(1_000);
    put_sink(
        &mut client,
        STREAM,
        5,
        archive_spec(Some("any:1")),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, STREAM, 5, ids[2]).await;
    put_sink(
        &mut client,
        STREAM,
        5,
        archive_spec(None),
        SinkStart::Earliest,
    )
    .await;
    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 5,
                start: SinkStart::Earliest,
            },
        )
        .await
        .unwrap();
    let reason = wait_for_pause(&mut client, STREAM, 5).await;
    assert!(reason.contains("already holds"), "{reason}");
    let (_, cursor) = sink_of(&mut client, STREAM, 5).await;
    assert!(cursor < ids[2], "the cursor passed what wasn't archived");
    drop(client);
    harness.finish();
}

/// A dedupe entry at the target outlasts a pause, so the claim on its producer id does too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_forward_keeps_its_claim_on_its_sink_id() {
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, "c.a").await;
    make_stream(&mut client, "c.b").await;
    put_sink(
        &mut client,
        "c.a",
        9,
        forward_spec("c.t"),
        SinkStart::Earliest,
    )
    .await;
    let a1 = send(&mut client, "c.a", b"a1", 1).await;
    wait_for_cursor(&mut client, "c.a", 9, a1).await;
    client
        .sink_op(
            "c.a",
            SinkOp::SetPaused {
                sink_id: 9,
                paused: true,
            },
        )
        .await
        .unwrap();
    harness.clock.advance(5);
    send(&mut client, "c.b", b"b1", 1).await;
    put_sink(
        &mut client,
        "c.b",
        9,
        forward_spec("c.t"),
        SinkStart::Earliest,
    )
    .await;
    let reason = wait_for_pause(&mut client, "c.b", 9).await;
    assert!(reason.contains("duplicates"), "{reason}");
    drop(client);
    harness.finish();
}

/// So does a delete: nothing else stops a later source's records being taken as duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_forward_keeps_its_claim_on_its_sink_id() {
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, "re.a").await;
    let b_id = send(&mut client, "re.b", b"from b", 1).await;
    harness.clock.advance(5);
    let a_id = send(&mut client, "re.a", b"from a", 1).await;
    put_sink(
        &mut client,
        "re.a",
        9,
        forward_spec("re.all"),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, "re.a", 9, a_id).await;
    client
        .sink_op("re.a", SinkOp::Delete { sink_id: 9 })
        .await
        .unwrap();
    put_sink(
        &mut client,
        "re.b",
        9,
        forward_spec("re.all"),
        SinkStart::Earliest,
    )
    .await;
    let reason = wait_for_pause(&mut client, "re.b", 9).await;
    assert!(reason.contains("duplicates"), "{reason}");
    let (_, cursor) = sink_of(&mut client, "re.b", 9).await;
    assert!(cursor < b_id, "the cursor passed a record that never went");
    drop(client);
    harness.finish();
}

/// The target says its latest seq for this producer is one this source can't have sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_whose_producer_id_was_used_by_a_publisher_pauses() {
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, "p.src").await;
    let poison = record_with(1, b"poison");
    let mut message = publish("p.dst", &poison, u64::MAX, 2);
    message.producer_id = 9;
    client.publish(message).await.unwrap();
    expect_ack(&mut client).await;
    put_sink(
        &mut client,
        "p.src",
        9,
        forward_spec("p.dst"),
        SinkStart::Earliest,
    )
    .await;
    let real = send(&mut client, "p.src", b"real", 1).await;
    let reason = wait_for_pause(&mut client, "p.src", 9).await;
    assert!(reason.contains("producer id"), "{reason}");
    let (_, cursor) = sink_of(&mut client, "p.src", 9).await;
    assert!(cursor < real, "the cursor passed a record that never went");
    drop(client);
    harness.finish();
}

/// Once it has seen its own seqs land, a seq above them answered as older can't be its own, even
/// when the other publisher's seq is below the source's head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_using_a_running_forwards_producer_id_pauses_it() {
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, "q.src").await;
    put_sink(
        &mut client,
        "q.src",
        9,
        forward_spec("q.dst"),
        SinkStart::Earliest,
    )
    .await;
    let first = send(&mut client, "q.src", b"first", 1).await;
    wait_for_cursor(&mut client, "q.src", 9, first).await;
    // A little above the next record, which will be two ids on, once the clock has moved.
    harness.clock.advance(5);
    let other = record_with(1, b"other");
    let mut message = publish("q.dst", &other, first + (10 << 16), 2);
    message.producer_id = 9;
    client.publish(message).await.unwrap();
    expect_ack(&mut client).await;
    let second = send(&mut client, "q.src", b"second", 2).await;
    harness.clock.advance(20);
    send(&mut client, "q.src", b"above it", 3).await;
    let reason = wait_for_pause(&mut client, "q.src", 9).await;
    assert!(reason.contains("producer id"), "{reason}");
    let (_, cursor) = sink_of(&mut client, "q.src", 9).await;
    assert!(
        cursor < second,
        "the cursor passed a record that never went"
    );
    drop(client);
    harness.finish();
}

/// A sink that has committed nothing still has a position: what retention held when it began.
async fn cursor_zero_sink_past_expiry(
    on_expire: &str,
    start: SinkStart,
) -> (Harness, Receiver, Client) {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.on_expire = on_expire.to_owned();
    put_sink(&mut client, STREAM, 1, spec, start).await;
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: true,
            },
        )
        .await
        .unwrap();
    let (_, cursor) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(cursor, 0);
    send(&mut client, STREAM, b"old", 1).await;
    assert!(matches!(
        harness.node().seal().await,
        server::SealOutcome::Uploaded { .. }
    ));
    wait_for_memory_to_empty(harness.node(), STREAM).await;
    harness.clock.advance(2 * DAY_MS);
    send(&mut client, STREAM, b"new", 2).await;
    client
        .sink_op(
            STREAM,
            SinkOp::SetPaused {
                sink_id: 1,
                paused: false,
            },
        )
        .await
        .unwrap();
    (harness, receiver, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_expire_stop_holds_at_cursor_zero_too() {
    for start in [SinkStart::Now, SinkStart::Earliest] {
        let (harness, receiver, mut client) = cursor_zero_sink_past_expiry("stop", start).await;
        let reason = wait_for_pause(&mut client, STREAM, 1).await;
        assert!(reason.contains("expired"), "{reason}");
        receiver.stays_at(0).await;
        drop(client);
        harness.finish();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_expire_skip_records_the_gap_at_cursor_zero_too() {
    let (harness, receiver, mut client) =
        cursor_zero_sink_past_expiry("skip", SinkStart::Earliest).await;
    let seen = receiver.wait_for_count(1).await;
    assert_eq!(seen[0].data(), b"new");
    let (spec, _) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(spec.gap_count, 1);
    drop(client);
    harness.finish();
}

/// What was gone before the sink began isn't something it missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn earliest_starts_at_what_is_left_and_calls_nothing_before_it_a_gap() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"old", 1).await;
    assert!(matches!(
        harness.node().seal().await,
        server::SealOutcome::Uploaded { .. }
    ));
    wait_for_memory_to_empty(harness.node(), STREAM).await;
    harness.clock.advance(2 * DAY_MS);
    let new = send(&mut client, STREAM, b"new", 2).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Earliest,
    )
    .await;
    wait_for_cursor(&mut client, STREAM, 1, new).await;
    let (spec, _) = sink_of(&mut client, STREAM, 1).await;
    assert!(!spec.paused, "{:?}", spec.pause_reason);
    assert_eq!(spec.gap_count, 0);
    assert_eq!(receiver.seen().len(), 1);
    drop(client);
    harness.finish();
}

/// Section 5.1 gives `u64::MAX` a meaning, and a sink's start or reset follows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_or_reset_at_u64_max_means_live_only() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"before", 1).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&receiver.url()),
        SinkStart::Id(u64::MAX),
    )
    .await;
    let live = send(&mut client, STREAM, b"live", 2).await;
    let seen = receiver.wait_for_count(1).await;
    assert_eq!(seen[0].data(), b"live");
    wait_for_cursor(&mut client, STREAM, 1, live).await;
    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 1,
                start: SinkStart::Id(u64::MAX),
            },
        )
        .await
        .unwrap();
    let (_, cursor) = sink_of(&mut client, STREAM, 1).await;
    assert_eq!(cursor, live);
    let again = send(&mut client, STREAM, b"again", 3).await;
    receiver.wait_for_count(2).await;
    wait_for_cursor(&mut client, STREAM, 1, again).await;
    drop(client);
    harness.finish();
}

fn notify_spec(url: String, interval_ms: u64) -> SinkSpec {
    SinkSpec {
        kind: "webhook.notify".to_owned(),
        url: Some(url),
        interval_ms,
        on_expire: "stop".to_owned(),
        concurrency: 1,
        ..SinkSpec::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_retries_keep_to_its_rate() {
    let receiver = Receiver::start().await;
    *receiver.answer.lock().unwrap() = Answer::Status(503);
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        3,
        notify_spec(receiver.url(), 60_000),
        SinkStart::Earliest,
    )
    .await;
    send(&mut client, STREAM, b"one", 1).await;
    receiver.wait_for_count(1).await;
    for _ in 0..30 {
        harness.clock.advance(1_000);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(receiver.count(), 1, "the interval allows one in 30 s");
    harness.clock.advance(30_000);
    receiver.wait_for_count(2).await;
    drop(client);
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_keeps_to_its_rate_across_an_update() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        3,
        notify_spec(receiver.url(), 60_000),
        SinkStart::Earliest,
    )
    .await;
    for seq in 1..=5 {
        send(&mut client, STREAM, b"one", seq).await;
        receiver.wait_for_count(1).await;
        put_sink(
            &mut client,
            STREAM,
            3,
            notify_spec(receiver.url(), 60_000),
            SinkStart::Earliest,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(receiver.count(), 1, "the clock hasn't moved");
    drop(client);
    harness.finish();
}

#[test]
fn only_global_unicast_passes_the_private_address_check() {
    for private in [
        "::ffff:0:127.0.0.1",
        "::ffff:0:a00:1",
        "::ffff:0:8.8.8.8",
        "64:ff9b:1::a00:1",
        "64:ff9b:1::7f00:1",
        "64:ff9b:1:a00:1::",
        "2001::1",
        "2001:2::1",
        "2001:10::1",
        "5f00::1",
        "3fff::1",
        "4000::1",
        "192.88.99.1",
    ] {
        let address: std::net::IpAddr = private.parse().unwrap();
        assert!(server::is_private_address(address), "{private}");
    }
    for public in [
        "2606:4700::1",
        "2a00:1450::1",
        "64:ff9b::808:808",
        "8.8.8.8",
    ] {
        let address: std::net::IpAddr = public.parse().unwrap();
        assert!(!server::is_private_address(address), "{public}");
    }
}

// ---------- second M9 review ----------

/// A key anyone can guess signs nothing, so a sink that got one by another way than HTTP stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliver_with_an_empty_secret_pauses_and_sends_nothing() {
    let receiver = Receiver::start().await;
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    send(&mut client, STREAM, b"one", 1).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.secret = Some(String::new());
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    let reason = wait_for_pause(&mut client, STREAM, 1).await;
    assert!(reason.contains("secret"), "{reason}");
    receiver.stays_at(0).await;
    drop(client);
    harness.finish();
}

/// A warning is about a position. A reset leaves that position, so a paused sink warns anew.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_sink_warns_about_its_new_position_after_a_reset() {
    let (harness, _receiver, mut client, first) = stuck_sink("stop").await;
    harness.clock.advance(DAY_MS - 3_600_000 + 1_000);
    assert_eq!(wait_for_warning(&mut client).await, first);
    set_paused(&mut client, STREAM, 1, true).await;
    let second = send(&mut client, STREAM, b"second", 2).await;
    send(&mut client, STREAM, b"third", 3).await;
    client
        .sink_op(
            STREAM,
            SinkOp::Reset {
                sink_id: 1,
                start: SinkStart::Id(second),
            },
        )
        .await
        .unwrap();
    harness.clock.advance(DAY_MS - 3_600_000 + 1_000);
    wait_until_warning_is(&mut client, second).await;
    drop(client);
    harness.finish();
}

async fn set_paused(client: &mut Client, stream: &str, sink_id: u64, paused: bool) {
    client
        .sink_op(stream, SinkOp::SetPaused { sink_id, paused })
        .await
        .unwrap();
}

async fn wait_until_warning_is(client: &mut Client, id: u64) {
    let started = Instant::now();
    loop {
        let (spec, _) = sink_of(client, STREAM, 1).await;
        if spec.expiry_warning_id == Some(id) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_millis(WAIT_MS),
            "the warning stayed at {:?}, not {id}",
            spec.expiry_warning_id
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The old sink's delivery ends after the new one exists. Its commit is the old sink's, even
/// with both made in one millisecond.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sink_recreated_in_the_same_millisecond_ignores_the_old_ones_commit() {
    let (old, new) = (Receiver::start().await, Receiver::start().await);
    old.answer(Answer::Hold);
    let harness = Harness::new(config());
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&old.url()),
        SinkStart::Earliest,
    )
    .await;
    send(&mut client, STREAM, b"a", 1).await;
    old.wait_for_count(1).await;
    client
        .sink_op(STREAM, SinkOp::Delete { sink_id: 1 })
        .await
        .unwrap();
    put_sink(
        &mut client,
        STREAM,
        1,
        deliver_spec(&new.url()),
        SinkStart::Earliest,
    )
    .await;
    old.answer(Answer::Status(200));
    old.release();
    let seen = new.wait_for_count(1).await;
    assert_eq!(seen[0].data(), b"a", "earliest includes it");
    drop(client);
    harness.finish();
}

/// Whatever a sink asks for, it can't hold more than the node allows: its copies are outside the
/// memory cap, so a higher figure would hand one stream's admin the node's memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sinks_budget_cant_pass_the_nodes() {
    let receiver = Receiver::start().await;
    receiver.answer(Answer::Hold);
    let hook = KillScript::with(&[]);
    let mut config = config();
    config.sink.hook = Some(hook.clone());
    config.sink.budget_bytes = 1024 * 1024;
    config.subscriber_queue_bytes = 64 * 1024;
    config.connection_window_bytes = 64 * 1024;
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut spec = deliver_spec(&receiver.url());
    spec.budget_bytes = u64::MAX;
    put_sink(&mut client, STREAM, 1, spec, SinkStart::Earliest).await;
    let data = vec![7u8; 64 * 1024];
    for seq in 1..=100 {
        send(&mut client, STREAM, &data, seq).await;
    }
    receiver.wait_for_count(1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let peak = hook.peak_held_bytes.load(Ordering::SeqCst);
    // One frame can pass it (D191), and a frame is at most the fair share.
    assert!(peak <= 1024 * 1024 + 256 * 1024, "held {peak} bytes");
    receiver.answer(Answer::Status(200));
    receiver.release();
    receiver.wait_for_count(100).await;
    drop(client);
    harness.finish();
}
