//! Real binaries, killed where a sink is most exposed (SPEC section 9). `SILLYSTREAM_CRASH_AT`
//! ends the process with no destructors, so the sink's cursor is what the last seal saved, and
//! the receiver and the target show what a restart makes of that.

mod api;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use api::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

const BIN: &str = env!("CARGO_BIN_EXE_sillystream-server");
const AUTHOR: u64 = 77;
const WAIT: Duration = Duration::from_secs(30);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

struct Files {
    dir: tempfile::TempDir,
    der: Vec<u8>,
}

impl Files {
    fn new() -> Files {
        let dir = tempfile::tempdir().unwrap();
        let cert = test_cert();
        std::fs::write(dir.path().join("cert.pem"), &cert.cert_pem).unwrap();
        std::fs::write(dir.path().join("key.pem"), &cert.key_pem).unwrap();
        std::fs::write(dir.path().join("secret"), SECRET).unwrap();
        // The all-zero key, which `NodeConfig::new` has too.
        std::fs::write(dir.path().join("stream-key"), "0".repeat(64)).unwrap();
        Files { dir, der: cert.der }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
}

/// Killed if the test ends early, so no process outlives it.
struct Running {
    child: Child,
    port: Option<u16>,
    _stderr: PathBuf,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(files: &Files, name: &str, crash_at: Option<&str>) -> Running {
    let path = |name: &str| files.path(name).to_str().unwrap().to_owned();
    let stderr = files.path(&format!("{name}.stderr"));
    let mut command = Command::new(BIN);
    command
        .args(["--listen", "127.0.0.1:0"])
        .args(["--wal-dir", &path("wal")])
        .args(["--data-dir", &path("data")])
        .args(["--cert", &path("cert.pem")])
        .args(["--key", &path("key.pem")])
        .args(["--token-secret-file", &path("secret")])
        .args(["--stream-key-file", &path("stream-key")])
        .args(["--object-store", &format!("local:{}", path("objects"))])
        .args(["--cores", "2"])
        .args(["--seal-window-ms", "1000"])
        .args(["--wal-preallocate-bytes", "1048576"])
        .args(["--sink-allow-private-addresses", "true"])
        .stdout(Stdio::piped())
        .stderr(File::create(&stderr).unwrap());
    if let Some(point) = crash_at {
        command.env("SILLYSTREAM_CRASH_AT", point);
    }
    let mut child = command.spawn().unwrap();
    let mut line = String::new();
    let out = child.stdout.take().unwrap();
    BufReader::new(out).read_line(&mut line).unwrap();
    let port = line
        .trim()
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok());
    Running {
        child,
        port,
        _stderr: stderr,
    }
}

fn wait_for_abort(running: &mut Running) {
    use std::os::unix::process::ExitStatusExt;
    let started = Instant::now();
    loop {
        if let Some(status) = running.child.try_wait().unwrap() {
            assert_eq!(status.signal(), Some(6), "it should die at the crash point");
            return;
        }
        assert!(started.elapsed() < WAIT, "the crash point never fired");
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct Admin {
    connector: TlsConnector,
    token: String,
}

impl Admin {
    fn new(files: &Files) -> Admin {
        let config = tls_client_config(&files.der, &[&rustls::version::TLS13]);
        Admin {
            connector: TlsConnector::from(Arc::new(config)),
            token: token_at(now_ms() + 3_600_000, AUTHOR, &[("*", ALL)]),
        }
    }

    async fn call(&self, port: u16, method: &str, path: &str, body: &[u8]) -> HttpReply {
        let socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut tls = self
            .connector
            .connect("localhost".try_into().unwrap(), socket)
            .await
            .unwrap();
        let request = Http::new(method, path, &self.token).body(body.to_vec());
        tls.write_all(&request.bytes()).await.unwrap();
        tls.flush().await.unwrap();
        read_http_reply(&mut tls).await
    }

    /// Sends and doesn't wait: the server may die before it answers.
    async fn send_only(&self, port: u16, method: &str, path: &str, body: &[u8]) {
        let socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut tls = self
            .connector
            .connect("localhost".try_into().unwrap(), socket)
            .await
            .unwrap();
        let request = Http::new(method, path, &self.token).body(body.to_vec());
        tls.write_all(&request.bytes()).await.unwrap();
        tls.flush().await.unwrap();
        let mut sink = Vec::new();
        let _ = tls.read_to_end(&mut sink).await;
    }

    async fn ok(&self, port: u16, method: &str, path: &str, body: &[u8]) -> HttpReply {
        let reply = self.call(port, method, path, body).await;
        assert!(reply.status < 300, "{method} {path}: {}", reply.text());
        reply
    }

    async fn publish(&self, port: u16, stream: &str, data: &[u8], seq: u64) {
        let path = format!("/v1/stream/{stream}/publish?ack=2");
        let seq_text = seq.to_string();
        let request = Http::new("POST", &path, &self.token)
            .header("Content-Type", "application/x-sillystream-records")
            .header("X-Producer-Id", "7")
            .header("X-Producer-Seq", &seq_text)
            .body(record(seq, data));
        let socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut tls = self
            .connector
            .connect("localhost".try_into().unwrap(), socket)
            .await
            .unwrap();
        tls.write_all(&request.bytes()).await.unwrap();
        tls.flush().await.unwrap();
        let reply = read_http_reply(&mut tls).await;
        assert!(reply.status < 300, "{}", reply.text());
    }

    /// `(author, data)` of everything in `stream`, in order.
    async fn read_all(&self, port: u16, stream: &str) -> Vec<(u64, Vec<u8>)> {
        let path = format!("/v1/stream/{stream}/read?from=0&limit=100000");
        let reply = self.call(port, "GET", &path, b"").await;
        assert_eq!(reply.status, 200, "{}", reply.text());
        format::Records::new(&reply.body)
            .map(|record| {
                let record = record.unwrap();
                (record.header.author, record.data.to_vec())
            })
            .collect()
    }
}

type Seen = Arc<Mutex<Vec<(u64, u64, Vec<Vec<u8>>)>>>;

/// Answers 200 to everything, keeping `(first_id, last_id, each record's data)` of each request.
async fn receiver() -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen: Seen = Arc::default();
    let keep = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let keep = Arc::clone(&keep);
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
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
                    .filter_map(|line| line.split_once(": "))
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
                    .collect();
                let len: usize = headers["content-length"].parse().unwrap();
                while buffer.len() < head_end + len {
                    match socket.read(&mut chunk).await {
                        Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
                        _ => return,
                    }
                }
                let mut body = &buffer[head_end..head_end + len];
                let mut data = Vec::new();
                while !body.is_empty() {
                    let (record, used) = format::decode_record(body).unwrap();
                    data.push(record.data.to_vec());
                    body = &body[used..];
                }
                keep.lock().unwrap().push((
                    headers["x-first-id"].parse().unwrap(),
                    headers["x-last-id"].parse().unwrap(),
                    data,
                ));
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    (format!("http://{address}/hook"), seen)
}

async fn wait_for(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let started = Instant::now();
    while !done().await {
        assert!(started.elapsed() < WAIT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const COUNT: usize = 5;

async fn webhook_case(point: &str) {
    let files = Files::new();
    let admin = Admin::new(&files);
    let (url, seen) = receiver().await;
    let mut running = start(&files, "first", Some(point));
    let port = running.port.unwrap();
    admin.ok(port, "PUT", "/v1/stream/w", b"{}").await;
    for at in 0..COUNT {
        admin
            .publish(port, "w", format!("record {at}").as_bytes(), at as u64 + 1)
            .await;
    }
    // Fires on the first delivery, in the moment this sink starts.
    let body =
        format!(r#"{{"kind":"webhook.deliver","url":"{url}","secret":"k","start":"earliest"}}"#);
    admin
        .send_only(port, "PUT", "/v1/stream/w/sink/9", body.as_bytes())
        .await;
    wait_for_abort(&mut running);

    let before = seen.lock().unwrap().clone();
    let running = start(&files, "second", None);
    wait_for("every record to arrive", async || {
        let seen = seen.lock().unwrap();
        (0..COUNT).all(|at| {
            let want = format!("record {at}").into_bytes();
            seen.iter().any(|(_, _, data)| data.contains(&want))
        })
    })
    .await;
    let seen = seen.lock().unwrap().clone();
    if point == "sink.after_delivery" {
        assert!(!before.is_empty(), "it delivered, then died");
    }
    // Whatever was delivered and not committed comes again from the same first id, in a batch
    // that may hold more than before. After the commit, the cursor is saved at once (section
    // 5.1), so it comes again only if the kill beat that save.
    for (first_id, last_id, data) in &before {
        let again = seen
            .iter()
            .skip(before.len())
            .find(|(_, _, other)| other.starts_with(data));
        match again {
            Some(again) => {
                assert_eq!(again.0, *first_id);
                assert!(again.1 >= *last_id);
            }
            None => assert_eq!(
                point, "sink.after_cursor",
                "the record not committed comes again"
            ),
        }
    }
    let order: Vec<u64> = seen[before.len()..].iter().map(|seen| seen.0).collect();
    assert!(
        order.windows(2).all(|pair| pair[0] < pair[1]),
        "in id order"
    );
    drop(running);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_webhook_killed_before_delivering() {
    webhook_case("sink.before_delivery").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_webhook_killed_between_delivery_and_cursor() {
    webhook_case("sink.after_delivery").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_webhook_killed_after_its_cursor() {
    webhook_case("sink.after_cursor").await;
}

async fn forward_case(point: &str) {
    let files = Files::new();
    let admin = Admin::new(&files);
    let mut running = start(&files, "first", Some(point));
    let port = running.port.unwrap();
    admin.ok(port, "PUT", "/v1/stream/from", b"{}").await;
    let mut wanted = Vec::new();
    for at in 0..COUNT {
        let data = format!("record {at}").into_bytes();
        admin.publish(port, "from", &data, at as u64 + 1).await;
        wanted.push((AUTHOR, data));
    }
    let body = br#"{"kind":"forward","target":"to","start":"earliest"}"#;
    admin
        .send_only(port, "PUT", "/v1/stream/from/sink/9", body)
        .await;
    wait_for_abort(&mut running);

    let running = start(&files, "second", None);
    let port = running.port.unwrap();
    wait_for("the target to fill", async || {
        let reply = admin.call(port, "GET", "/v1/stream/to", b"").await;
        reply.status == 200
            && admin
                .call(port, "GET", "/v1/stream/from", b"")
                .await
                .text()
                .contains("\"cursor\"")
            && admin.read_all(port, "to").await.len() >= COUNT
    })
    .await;
    // Anything more would be a duplicate, so give the sink a while to send one.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(admin.read_all(port, "to").await, wanted);
    drop(running);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_killed_before_publishing() {
    forward_case("sink.before_delivery").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_killed_after_publishing_before_the_ack() {
    forward_case("sink.after_publish").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_killed_after_the_ack_before_the_cursor() {
    forward_case("sink.after_delivery").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_killed_after_the_cursor() {
    forward_case("sink.after_cursor").await;
}
