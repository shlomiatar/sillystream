//! The black-box suite's tools: the real binaries as child processes, and clients on real TLS
//! WebSocket and HTTP sockets. Nothing here links the server. Record bytes are built and parsed
//! here from SPEC section 2's table, so that a change of format fails rather than following.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ciborium::value::Value;
use futures_util::{SinkExt, StreamExt};
use proto::{AckLevel, ClientFrame, ErrorCode, ServerFrame, StateOp, WireClauses};
use sdk::http::{HttpClient, Response};
use sdk::{Client, ClientConfig, Event, Item, Subscription};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;

pub const HEADER_BYTES: usize = 32;
pub const RECORDS_TYPE: &str = "application/x-sillystream-records";
pub const ALL_PERMS: &str = "read,publish,state,admin";
pub const WAIT: Duration = Duration::from_secs(60);
const SECRET: &[u8] = b"a throwaway secret for the end to end suite, long enough";
const WRONG_SECRET: &[u8] = b"a different secret of the same sort, also long enough ok";

// ---------------------------------------------------------------------------------------------
// The binaries
// ---------------------------------------------------------------------------------------------

pub struct Binaries {
    pub dir: PathBuf,
    pub server: PathBuf,
    /// The same server with `test-hooks`, for the clock offset and crash points only.
    pub hooks_server: PathBuf,
    pub cli: PathBuf,
}

fn cargo_build(target_dir: &Path, root: &Path, args: &[&str]) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let status = Command::new(cargo)
        .current_dir(root)
        .env("CARGO_TARGET_DIR", target_dir)
        .arg("build")
        .args(args)
        .status()
        .expect("running cargo");
    assert!(status.success(), "cargo build {args:?} failed");
}

/// Builds both forms of the server here, because a test can't name another package's binary, and
/// `cargo test --workspace` would otherwise hand every test the one `test-hooks` turned on in.
/// The directory is the suite's own, so another build can't swap a binary during a run.
pub fn binaries() -> &'static Binaries {
    static BINARIES: OnceLock<Binaries> = OnceLock::new();
    BINARIES.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = root.canonicalize().expect("the workspace root");
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map_or_else(|| root.join("target"), PathBuf::from)
            .join("e2e");
        let dir = target.join("bin");
        std::fs::create_dir_all(dir.join("plain")).expect("the binary directory");
        std::fs::create_dir_all(dir.join("hooks")).expect("the binary directory");
        let keep = |name: &str, form: &str| {
            let to = dir.join(form).join(name);
            std::fs::copy(target.join("debug").join(name), &to).expect("copying a binary");
            to
        };
        cargo_build(&target, &root, &["-p", "server", "-p", "cli", "--bins"]);
        let server = keep("sillystream-server", "plain");
        let cli = keep("sillystream", "plain");
        cargo_build(
            &target,
            &root,
            &["-p", "server", "--bins", "--features", "test-hooks"],
        );
        let hooks_server = keep("sillystream-server", "hooks");
        Binaries {
            dir,
            server,
            hooks_server,
            cli,
        }
    })
}

/// Every server this run started and hasn't seen exit.
static PIDS: Mutex<Option<HashSet<u32>>> = Mutex::new(None);

fn note_pid(pid: u32, alive: bool) {
    let mut pids = PIDS.lock().unwrap_or_else(|error| error.into_inner());
    let pids = pids.get_or_insert_with(HashSet::new);
    if alive {
        pids.insert(pid);
    } else {
        pids.remove(&pid);
    }
}

fn command_line(pid: u32) -> String {
    let output = Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output()
        .expect("running ps");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// Signals one exact process, once `ps` confirms that it is a server this suite started.
pub fn signal_own_server(pid: u32, signal: &str) -> bool {
    let line = command_line(pid);
    if !line.starts_with(binaries().dir.to_str().expect("a UTF-8 path")) {
        return false;
    }
    Command::new("kill")
        .args([signal, &pid.to_string()])
        .status()
        .is_ok_and(|status| status.success())
}

/// For the suite's overall deadline: whatever this run started and still has.
pub fn kill_everything_started() {
    let pids: Vec<u32> = PIDS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .flatten()
        .copied()
        .collect();
    for pid in pids {
        signal_own_server(pid, "-KILL");
    }
}

/// Command lines of processes running a binary from the suite's directory. It only reads.
pub fn servers_running() -> Vec<String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .expect("running ps");
    let dir = binaries().dir.to_str().expect("a UTF-8 path").to_owned();
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains(&dir))
        .map(str::to_owned)
        .collect()
}

// ---------------------------------------------------------------------------------------------
// A node
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct NodeOptions {
    pub hooks: bool,
    pub seal_window_ms: u64,
    pub cores: u32,
    pub extra: Vec<String>,
}

impl Default for NodeOptions {
    fn default() -> NodeOptions {
        NodeOptions {
            hooks: false,
            seal_window_ms: 1_000,
            cores: 2,
            extra: Vec::new(),
        }
    }
}

struct Running {
    child: Child,
    _stdout: BufReader<ChildStdout>,
}

impl Drop for Running {
    fn drop(&mut self) {
        // `Child::kill` signals only a child not yet reaped, so this can't hit a reused PID.
        let pid = self.child.id();
        let _ = self.child.kill();
        let _ = self.child.wait();
        note_pid(pid, false);
    }
}

/// One server's disks, port and certificate, across its restarts.
pub struct Node {
    pub dir: tempfile::TempDir,
    pub options: NodeOptions,
    pub ca_pem: Vec<u8>,
    ca_der: Vec<u8>,
    pub port: u16,
    pub clock_offset_ms: u64,
    start_count: u32,
    running: Option<Running>,
}

impl Node {
    pub fn new(options: NodeOptions) -> Node {
        let dir = tempfile::tempdir().expect("a temp dir");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("a throwaway certificate");
        let ca_pem = cert.cert.pem().into_bytes();
        std::fs::write(dir.path().join("cert.pem"), &ca_pem).expect("writing");
        std::fs::write(
            dir.path().join("key.pem"),
            cert.key_pair.serialize_pem().as_bytes(),
        )
        .expect("writing");
        std::fs::write(dir.path().join("secret"), SECRET).expect("writing");
        std::fs::write(dir.path().join("wrong_secret"), WRONG_SECRET).expect("writing");
        // Free now. The port stays this node's across restarts, so clients need no new address.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        Node {
            ca_der: cert.cert.der().to_vec(),
            dir,
            options,
            ca_pem,
            port,
            clock_offset_ms: 0,
            start_count: 0,
            running: None,
        }
    }

    pub fn started(options: NodeOptions) -> Node {
        let mut node = Node::new(options);
        node.start();
        node
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn path_text(&self, name: &str) -> String {
        self.path(name).to_str().expect("a UTF-8 path").to_owned()
    }

    fn log_path(&self, start: u32) -> PathBuf {
        self.path(&format!("server-{start}.stderr"))
    }

    pub fn start(&mut self) {
        self.start_with_env(&[]);
    }

    pub fn start_with_env(&mut self, env: &[(&str, &str)]) {
        assert!(self.running.is_none(), "already running");
        self.start_count += 1;
        let bins = binaries();
        let bin = if self.options.hooks {
            &bins.hooks_server
        } else {
            &bins.server
        };
        let mut command = Command::new(bin);
        command
            .args(["--listen", &format!("127.0.0.1:{}", self.port)])
            .args(["--wal-dir", &self.path_text("wal")])
            .args(["--data-dir", &self.path_text("data")])
            .args(["--cert", &self.path_text("cert.pem")])
            .args(["--key", &self.path_text("key.pem")])
            .args(["--token-secret-file", &self.path_text("secret")])
            .args([
                "--object-store",
                &format!("local:{}", self.path_text("objects")),
            ])
            .args(["--cores", &self.options.cores.to_string()])
            .args(["--seal-window-ms", &self.options.seal_window_ms.to_string()])
            .args(["--wal-preallocate-bytes", "4194304"])
            .args(["--trace-catch-up", "true"])
            .args(["--sink-allow-private-addresses", "true"])
            .args(&self.options.extra)
            .stdout(Stdio::piped())
            .stderr(File::create(self.log_path(self.start_count)).expect("a log file"));
        if self.clock_offset_ms != 0 {
            assert!(
                self.options.hooks,
                "only a test-hooks build moves its clock"
            );
            command.env(
                "SILLYSTREAM_CLOCK_OFFSET_MS",
                self.clock_offset_ms.to_string(),
            );
        }
        command.envs(env.iter().copied());
        let mut child = command.spawn().expect("starting the server");
        note_pid(child.id(), true);
        let mut stdout = BufReader::new(child.stdout.take().expect("piped"));
        let mut line = String::new();
        stdout.read_line(&mut line).expect("reading its stdout");
        let running = Running {
            child,
            _stdout: stdout,
        };
        assert!(
            line.starts_with("listening on "),
            "the server said {line:?}: {}",
            self.log_text()
        );
        self.running = Some(running);
    }

    pub fn pid(&self) -> u32 {
        self.running.as_ref().expect("running").child.id()
    }

    /// `kill -9`, which only a process crash can be: the page cache survives it.
    pub fn kill9(&mut self) {
        let mut running = self.running.take().expect("running");
        let pid = running.child.id();
        assert!(
            signal_own_server(pid, "-KILL"),
            "pid {pid} isn't our server"
        );
        let status = running.child.wait().expect("waiting");
        assert!(!status.success(), "{status}");
        note_pid(pid, false);
    }

    /// SIGTERM, which seals what it can before it stops.
    pub fn stop(&mut self) {
        let mut running = self.running.take().expect("running");
        let pid = running.child.id();
        assert!(
            signal_own_server(pid, "-TERM"),
            "pid {pid} isn't our server"
        );
        let started = Instant::now();
        loop {
            if let Some(status) = running.child.try_wait().expect("waiting") {
                assert!(status.success(), "{status}: {}", self.log_text());
                note_pid(pid, false);
                return;
            }
            assert!(started.elapsed() < WAIT, "still running after SIGTERM");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.is_some()
    }

    /// What this start has said on stderr.
    pub fn log_text(&self) -> String {
        std::fs::read_to_string(self.log_path(self.start_count)).unwrap_or_default()
    }

    /// `(tier, bytes)` of each catch-up read of `stream` in this start.
    pub fn tiers_read(&self, stream: &str) -> Vec<(String, u64)> {
        let wanted = format!("stream={stream} ");
        self.log_text()
            .lines()
            .filter(|line| line.starts_with("trace catch_up ") && line.contains(&wanted))
            .map(|line| {
                let field = |name: &str| {
                    line.split(' ')
                        .find_map(|part| part.strip_prefix(name))
                        .expect("a trace line's field")
                        .to_owned()
                };
                (
                    field("tier="),
                    field("bytes=").parse().expect("a byte count"),
                )
            })
            .collect()
    }

    pub fn url(&self) -> String {
        format!("localhost:{}", self.port)
    }

    /// Every file under `objects/<root>`, relative to `objects/`.
    pub fn objects(&self, root: &str) -> HashSet<String> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                if entry.path().is_dir() {
                    walk(&entry.path(), out);
                } else {
                    out.push(entry.path());
                }
            }
        }
        let base = self.path("objects");
        let mut files = Vec::new();
        walk(&base.join(root), &mut files);
        files
            .iter()
            .filter_map(|file| file.strip_prefix(&base).ok())
            .filter_map(|file| file.to_str().map(str::to_owned))
            .collect()
    }

    /// `sillystream` with this node's address and certificate, which must succeed.
    pub fn cli(&self, token: &str, args: &[&str]) -> String {
        let output = Command::new(&binaries().cli)
            .args(args)
            .args(["--url", &self.url()])
            .args(["--ca", &self.path_text("cert.pem")])
            .args(["--token", token])
            .output()
            .expect("running sillystream");
        assert!(
            output.status.success(),
            "sillystream {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn issue(&self, secret: &str, author: u64, grants: &[&str], ttl_ms: u64) -> String {
        let mut command = Command::new(&binaries().cli);
        command
            .args(["token", "issue", "--secret-file", &self.path_text(secret)])
            .args(["--author", &author.to_string()])
            .args(["--ttl-ms", &ttl_ms.to_string()]);
        for grant in grants {
            command.args(["--grant", grant]);
        }
        let output = command.output().expect("running sillystream");
        assert!(
            output.status.success(),
            "token issue: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    /// A token from the CLI, as an issuer would make one.
    pub fn token(&self, author: u64, grants: &[&str], ttl_ms: u64) -> String {
        self.issue("secret", author, grants, ttl_ms)
    }

    pub fn token_signed_wrongly(&self, author: u64, grants: &[&str]) -> String {
        self.issue("wrong_secret", author, grants, 3_600_000)
    }

    pub fn admin_token(&self, author: u64) -> String {
        // Past any clock offset a test uses.
        let grant = format!("*:{ALL_PERMS}");
        self.token(author, &[&grant], 40 * 24 * 3_600_000)
    }

    pub async fn sdk(&self, token: &str) -> Client {
        Client::connect(ClientConfig::new(&self.url(), &self.ca_pem, token))
            .await
            .expect("connecting the SDK")
    }

    pub fn http(&self, token: &str) -> Http {
        Http {
            client: HttpClient::new(&self.url(), &self.ca_pem, token).expect("an HTTP client"),
        }
    }

    pub fn endpoint(&self) -> Endpoint {
        Endpoint {
            port: self.port,
            ca_der: self.ca_der.clone(),
        }
    }

    /// `tokio-tungstenite`, with `HELLO` done.
    pub async fn ws(&self, token: &str) -> Ws {
        self.endpoint().ws(token).await.expect("connecting")
    }
}

// ---------------------------------------------------------------------------------------------
// Record bytes
// ---------------------------------------------------------------------------------------------

/// One record as a producer sends it, with `id` and `author` left 0 for the owner to stamp.
pub fn record(options: u64, codec: u16, data: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 8];
    out.extend_from_slice(&options.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&codec.to_le_bytes());
    out.extend_from_slice(data);
    out.resize(out.len().next_multiple_of(8), 0);
    out
}

/// What a record must look like once stored: only `id` and `author` differ.
pub fn stamped(sent: &[u8], id: u64, author: u64) -> Vec<u8> {
    let mut out = sent.to_vec();
    out[0..8].copy_from_slice(&id.to_le_bytes());
    out[16..24].copy_from_slice(&author.to_le_bytes());
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub id: u64,
    pub options: u64,
    pub author: u64,
    pub count: u16,
    pub data: Vec<u8>,
    /// The whole record, as it came.
    pub bytes: Vec<u8>,
}

impl Stored {
    pub fn last_id(&self) -> u64 {
        self.id + u64::from(self.count) - 1
    }
}

/// Splits a run of records by SPEC section 2's table.
pub fn parse_records(bytes: &[u8]) -> Vec<Stored> {
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let len = u32::from_le_bytes(bytes[at + 24..at + 28].try_into().expect("4 bytes")) as usize;
        let count = u16::from_le_bytes(bytes[at + 28..at + 30].try_into().expect("2 bytes"));
        let end = at + HEADER_BYTES + len.next_multiple_of(8);
        out.push(Stored {
            id: u64_at(at),
            options: u64_at(at + 8),
            author: u64_at(at + 16),
            count,
            data: bytes[at + HEADER_BYTES..at + HEADER_BYTES + len].to_vec(),
            bytes: bytes[at..end].to_vec(),
        });
        at = end;
    }
    out
}

pub fn cbor(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).expect("encoding CBOR");
    out
}

pub fn cbor_map(pairs: &[(&str, i64)]) -> Vec<u8> {
    cbor(&Value::Map(
        pairs
            .iter()
            .map(|(key, value)| (Value::Text((*key).to_owned()), Value::from(*value)))
            .collect(),
    ))
}

/// `incr`'s doc: `{"path": [keys], "n": n}`.
pub fn incr_doc(path: &[&str], n: i64) -> Vec<u8> {
    cbor(&Value::Map(vec![
        (
            Value::Text("path".to_owned()),
            Value::Array(
                path.iter()
                    .map(|key| Value::Text((*key).to_owned()))
                    .collect(),
            ),
        ),
        (Value::Text("n".to_owned()), Value::from(n)),
    ]))
}

/// The integer at a path in a CBOR doc, or `None` if there is none.
pub fn cbor_int(doc: &[u8], path: &[&str]) -> Option<i64> {
    let mut value: Value = ciborium::de::from_reader(doc).ok()?;
    for key in path {
        let Value::Map(pairs) = value else {
            return None;
        };
        value = pairs
            .into_iter()
            .find(|(name, _)| name.as_text() == Some(key))?
            .1;
    }
    value.as_integer().and_then(|n| i64::try_from(n).ok())
}

// ---------------------------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------------------------

pub struct Http {
    pub client: HttpClient,
}

pub fn json_field(body: &str, name: &str) -> Option<String> {
    let key = format!("\"{name}\":");
    let rest = body[body.find(&key)? + key.len()..].trim_start();
    Some(match rest.strip_prefix('"') {
        Some(quoted) => quoted[..quoted.find('"')?].to_owned(),
        None => rest.split([',', '}']).next()?.trim().to_owned(),
    })
}

impl Http {
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Response {
        self.client
            .request(method, path, headers, body)
            .await
            .unwrap_or_else(|error| panic!("{method} {path}: {error}"))
    }

    /// Encoded records, at level 2. Returns `first_id`.
    pub async fn publish_records(&self, stream: &str, records: &[u8]) -> u64 {
        let path = format!("/v1/stream/{stream}/publish?ack=2");
        let reply = self
            .call("POST", &path, &[("content-type", RECORDS_TYPE)], records)
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text());
        json_field(&reply.text(), "first_id")
            .and_then(|id| id.parse().ok())
            .expect("first_id")
    }

    /// `(head, state_ver)`.
    pub async fn info(&self, stream: &str) -> Option<(u64, u64)> {
        let reply = self
            .call("GET", &format!("/v1/stream/{stream}"), &[], b"")
            .await;
        if reply.status == 404 {
            return None;
        }
        assert_eq!(reply.status, 200, "{}", reply.text());
        let text = reply.text();
        let number = |name: &str| {
            json_field(&text, name)
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("{name} in {text}"))
        };
        Some((number("head"), number("state_ver")))
    }

    /// The read route's raw bytes from a position, page by page, up to what there is.
    pub async fn read_bytes(&self, stream: &str, from: u64, filter: Option<&str>) -> Vec<u8> {
        let mut out = Vec::new();
        let mut from = from;
        loop {
            let mut path = format!("/v1/stream/{stream}/read?from={from}");
            if let Some(filter) = filter {
                path.push_str(&format!("&filter={filter}"));
            }
            let reply = self.call("GET", &path, &[], b"").await;
            assert_eq!(reply.status, 200, "{path}: {}", reply.text());
            out.extend_from_slice(&reply.body);
            let number = |name: &str| -> u64 {
                reply
                    .header(name)
                    .and_then(|id| id.parse().ok())
                    .expect(name)
            };
            let (through, head) = (number("x-through"), number("x-head"));
            if through >= head || through <= from {
                return out;
            }
            from = through;
        }
    }

    pub async fn read_all(&self, stream: &str) -> Vec<Stored> {
        parse_records(&self.read_bytes(stream, 0, None).await)
    }

    /// `(state, head, ver)`.
    pub async fn snapshot(&self, stream: &str) -> (Vec<u8>, u64, u64) {
        let reply = self
            .call("GET", &format!("/v1/stream/{stream}/state"), &[], b"")
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text());
        let number = |name: &str| -> u64 {
            reply
                .header(name)
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("{name}"))
        };
        (reply.body.clone(), number("x-head"), number("x-state-ver"))
    }
}

// ---------------------------------------------------------------------------------------------
// WebSocket, not through the SDK
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct Endpoint {
    pub port: u16,
    ca_der: Vec<u8>,
}

impl Endpoint {
    async fn socket(&self) -> Result<Ws, String> {
        let text = |error: &dyn std::fmt::Display| error.to_string();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(self.ca_der.clone().into())
            .map_err(|error| text(&error))?;
        let provider = rustls::crypto::ring::default_provider();
        let config = rustls::ClientConfig::builder_with_provider(provider.into())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|error| text(&error))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let socket = TcpStream::connect(("127.0.0.1", self.port))
            .await
            .map_err(|error| text(&error))?;
        let name = "localhost".try_into().map_err(|error| text(&error))?;
        let tls = TlsConnector::from(Arc::new(config))
            .connect(name, socket)
            .await
            .map_err(|error| text(&error))?;
        let url = format!("wss://localhost:{}/v1/ws", self.port);
        let (socket, _) = tokio_tungstenite::client_async(url, tls)
            .await
            .map_err(|error| text(&error))?;
        Ok(Ws { socket })
    }

    /// With `HELLO` done, or what it answered.
    pub async fn ws(&self, token: &str) -> Result<Ws, Owned> {
        let mut ws = self.socket().await.map_err(|_| Owned::Closed)?;
        ws.send(&ClientFrame::Hello { ver: 1, token }).await;
        match ws.recv_within(Duration::from_secs(10)).await {
            Some(Owned::Welcome) => Ok(ws),
            Some(other) => Err(other),
            None => Err(Owned::Closed),
        }
    }
}

pub struct Ws {
    socket: WebSocketStream<TlsStream<TcpStream>>,
}

/// A decoded server frame that outlives its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owned {
    Welcome,
    Subscribed {
        sub: u32,
        head: u64,
        state_ver: u64,
        state: Vec<u8>,
    },
    Data {
        sub: u32,
        records: Vec<u8>,
    },
    Ack {
        handle: u32,
        producer_seq: u64,
        first_id: u64,
        count: u32,
    },
    State {
        sub: u32,
        ver: u64,
        doc: Vec<u8>,
    },
    Behind {
        sub: u32,
        resume_from: u64,
    },
    Progress {
        sub: u32,
        id: u64,
    },
    StateResult {
        req: u32,
        ok: bool,
        ver: u64,
    },
    Nack {
        handle: u32,
        producer_seq: u64,
        code: ErrorCode,
    },
    Error {
        reference: u32,
        code: ErrorCode,
    },
    Pong {
        nonce: u32,
    },
    Closed,
}

pub struct PublishFrame<'a> {
    pub handle: u32,
    pub producer_id: u64,
    pub seq: u64,
    pub ack: AckLevel,
    pub state: Option<(u64, StateOp, &'a [u8])>,
    pub records: &'a [u8],
}

impl Ws {
    /// A send to a socket that has gone is found out on the next read.
    pub async fn send(&mut self, frame: &ClientFrame<'_>) {
        let mut out = Vec::new();
        frame.encode(&mut out).expect("encoding a frame");
        let _ = self.socket.send(Message::Binary(out)).await;
    }

    pub async fn open(&mut self, handle: u32, stream: &str) {
        self.send(&ClientFrame::Open { handle, stream }).await;
    }

    pub async fn publish(&mut self, frame: &PublishFrame<'_>) {
        self.send(&ClientFrame::Publish(proto::Publish {
            handle: frame.handle,
            producer_id: frame.producer_id,
            producer_seq: frame.seq,
            ack: frame.ack,
            state: frame
                .state
                .map(|(expect_ver, op, doc)| proto::PublishState {
                    expect_ver,
                    op,
                    doc,
                }),
            records: frame.records,
        }))
        .await;
    }

    pub async fn subscribe(&mut self, sub: u32, stream: &str, from: u64, include_system: bool) {
        self.send(&ClientFrame::Subscribe(proto::Subscribe {
            sub,
            from,
            linger_ms: 0,
            author: None,
            clauses: WireClauses::new(&[]).expect("no clauses"),
            stream,
            name: None,
            with_state: false,
            state_only: false,
            include_system,
        }))
        .await;
    }

    /// The next frame, `Closed` if the socket ended, or `None` if nothing came in time.
    pub async fn recv_within(&mut self, wait: Duration) -> Option<Owned> {
        loop {
            let message = tokio::time::timeout(wait, self.socket.next()).await.ok()?;
            let message = match message {
                None | Some(Err(_)) => return Some(Owned::Closed),
                Some(Ok(message)) => message,
            };
            let bytes = match message {
                Message::Binary(bytes) => bytes,
                Message::Close(_) => return Some(Owned::Closed),
                _ => continue,
            };
            let frame = proto::decode_server(&bytes, usize::MAX).expect("a frame that decodes");
            return Some(match frame {
                ServerFrame::Welcome { .. } => Owned::Welcome,
                ServerFrame::Subscribed {
                    sub,
                    head,
                    state_ver,
                    state,
                } => Owned::Subscribed {
                    sub,
                    head,
                    state_ver,
                    state: state.to_vec(),
                },
                ServerFrame::Data { sub, records } => Owned::Data {
                    sub,
                    records: records.to_vec(),
                },
                ServerFrame::Ack {
                    handle,
                    producer_seq,
                    first_id,
                    count,
                } => Owned::Ack {
                    handle,
                    producer_seq,
                    first_id,
                    count,
                },
                ServerFrame::State { sub, ver, doc } => Owned::State {
                    sub,
                    ver,
                    doc: doc.to_vec(),
                },
                ServerFrame::Behind { sub, resume_from } => Owned::Behind { sub, resume_from },
                ServerFrame::Progress { sub, id } => Owned::Progress { sub, id },
                ServerFrame::StateResult { req, ok, ver } => Owned::StateResult { req, ok, ver },
                ServerFrame::Nack {
                    handle,
                    producer_seq,
                    code,
                    ..
                } => Owned::Nack {
                    handle,
                    producer_seq,
                    code,
                },
                ServerFrame::Error {
                    reference, code, ..
                } => Owned::Error { reference, code },
                ServerFrame::Pong { nonce } => Owned::Pong { nonce },
                ServerFrame::Redirect { .. } => panic!("REDIRECT is for multiple nodes"),
            });
        }
    }

    pub async fn recv(&mut self) -> Owned {
        self.recv_within(WAIT).await.expect("a frame should come")
    }

    /// The raw `DATA` bytes of a subscription up to `through`, from a fresh subscribe.
    pub async fn read_through(
        &mut self,
        sub: u32,
        stream: &str,
        from: u64,
        include_system: bool,
        through: u64,
    ) -> Vec<u8> {
        self.subscribe(sub, stream, from, include_system).await;
        let mut out = Vec::new();
        let mut at = from;
        while at < through {
            match self.recv().await {
                Owned::Data { records, sub: got } => {
                    assert_eq!(got, sub);
                    if let Some(last) = parse_records(&records).last() {
                        at = last.last_id();
                    }
                    out.extend_from_slice(&records);
                }
                Owned::Progress { id, .. } => at = at.max(id),
                Owned::Subscribed { .. } => {}
                other => panic!("reading {stream}: {other:?}"),
            }
        }
        self.send(&ClientFrame::Unsubscribe { sub }).await;
        out
    }
}

// ---------------------------------------------------------------------------------------------
// A producer that survives the server
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Published {
    pub seq: u64,
    /// 0 where only section 3's "already committed" came back.
    pub first_id: u64,
    pub options: u64,
    pub payload: Vec<u8>,
    pub has_state: bool,
}

pub struct ProducerSpec {
    pub endpoint: Endpoint,
    pub token: String,
    pub stream: String,
    pub producer_id: u64,
    /// `None` runs until stopped.
    pub count: Option<u64>,
    pub window: usize,
    /// Every this many publishes also carries an `incr` of `state.by.<producer_id>`.
    pub state_every: Option<u64>,
    pub pace: Duration,
    pub options_of: fn(u64) -> u64,
}

pub struct Producer {
    stop: Arc<AtomicBool>,
    pub acked: Arc<Mutex<Vec<Published>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Producer {
    pub fn start(spec: ProducerSpec) -> Producer {
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn(run_producer(spec, Arc::clone(&stop), Arc::clone(&acked)));
        Producer { stop, acked, task }
    }

    pub fn acked_count(&self) -> usize {
        self.acked.lock().expect("not poisoned").len()
    }

    /// One with a count makes them all, and one without stops making more. Either way, this
    /// waits until everything made has been acked.
    pub async fn finish(self) -> Vec<Published> {
        self.stop.store(true, Ordering::SeqCst);
        tokio::time::timeout(WAIT * 2, self.task)
            .await
            .expect("a producer that finishes")
            .expect("a producer that doesn't panic");
        Arc::into_inner(self.acked)
            .expect("the only holder")
            .into_inner()
            .expect("not poisoned")
    }
}

/// A publish on its way, with what goes on the wire and what is noted once it is acked.
struct Inflight {
    records: Vec<u8>,
    published: Published,
}

fn no_more_to_make(spec: &ProducerSpec, stop: &AtomicBool, next_seq: u64) -> bool {
    match spec.count {
        Some(count) => next_seq > count,
        None => stop.load(Ordering::SeqCst),
    }
}

/// Pipelined, at level 2. Whatever a dead connection left unanswered goes again, in order and
/// under the same seq, which is how section 8.1 says producers recover.
async fn run_producer(
    spec: ProducerSpec,
    stop: Arc<AtomicBool>,
    acked: Arc<Mutex<Vec<Published>>>,
) {
    let mut next_seq = 1u64;
    let mut inflight: std::collections::VecDeque<Inflight> = std::collections::VecDeque::new();
    let state_doc = incr_doc(&["by", &spec.producer_id.to_string()], 1);
    let mut last_new = Instant::now();
    loop {
        let done_making = no_more_to_make(&spec, &stop, next_seq);
        if done_making && inflight.is_empty() {
            return;
        }
        let Ok(mut ws) = spec.endpoint.ws(&spec.token).await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        ws.open(1, &spec.stream).await;
        let send = async |ws: &mut Ws, entry: &Inflight| {
            ws.publish(&PublishFrame {
                handle: 1,
                producer_id: spec.producer_id,
                seq: entry.published.seq,
                ack: AckLevel::WalFsync,
                state: entry.published.has_state.then_some((
                    0,
                    StateOp::Incr,
                    state_doc.as_slice(),
                )),
                records: &entry.records,
            })
            .await;
        };
        for entry in &inflight {
            send(&mut ws, entry).await;
        }
        loop {
            let done_making = no_more_to_make(&spec, &stop, next_seq);
            if done_making && inflight.is_empty() {
                return;
            }
            let room = inflight.len() < spec.window;
            if !done_making && room && last_new.elapsed() >= spec.pace {
                let payload = format!("{}:{next_seq}", spec.producer_id).into_bytes();
                let options = (spec.options_of)(next_seq);
                let has_state = spec
                    .state_every
                    .is_some_and(|every| next_seq.is_multiple_of(every));
                let entry = Inflight {
                    records: record(options, 0, &payload),
                    published: Published {
                        seq: next_seq,
                        first_id: 0,
                        options,
                        payload,
                        has_state,
                    },
                };
                send(&mut ws, &entry).await;
                inflight.push_back(entry);
                next_seq += 1;
                last_new = Instant::now();
                continue;
            }
            let wait = if room && !done_making {
                spec.pace.max(Duration::from_millis(1))
            } else {
                Duration::from_secs(10)
            };
            match ws.recv_within(wait).await {
                None if room && !done_making => {}
                // Silence with an answer owed: a socket that hasn't noticed its server died.
                None => break,
                Some(Owned::Ack {
                    producer_seq,
                    first_id,
                    ..
                }) => {
                    let at = inflight
                        .iter()
                        .position(|entry| entry.published.seq == producer_seq)
                        .unwrap_or_else(|| panic!("an ack of {producer_seq} that isn't owed"));
                    // In order per producer, so there is nothing ahead of it to retry.
                    assert_eq!(at, 0, "ack of seq {producer_seq} before earlier ones");
                    let mut published = inflight.pop_front().expect("found above").published;
                    published.first_id = first_id;
                    acked.lock().expect("not poisoned").push(published);
                }
                Some(Owned::Closed) => break,
                Some(other @ Owned::Nack { .. }) => {
                    panic!("producer {} refused: {other:?}", spec.producer_id)
                }
                Some(other @ Owned::Error { .. }) => {
                    panic!("producer {} got {other:?}", spec.producer_id)
                }
                Some(_) => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Small things
// ---------------------------------------------------------------------------------------------

/// A small generator, with its seed printed so that a failing run can be looked at again.
pub struct Rng(u64);

impl Rng {
    pub fn seeded(name: &str) -> Rng {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after 1970")
            .as_nanos() as u64
            | 1;
        println!("    {name}: seed {seed}");
        Rng(seed)
    }

    pub fn below(&mut self, limit: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % limit
    }
}

pub async fn wait_until(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let started = Instant::now();
    while !done().await {
        assert!(started.elapsed() < WAIT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// SDK items until `count` have come, then a quiet spell in which anything more would show.
pub async fn collect_items(subscription: &mut Subscription, count: usize) -> Vec<Item> {
    let mut items = Vec::new();
    let started = Instant::now();
    loop {
        let left = if items.len() < count {
            assert!(started.elapsed() < WAIT, "{} of {count} items", items.len());
            Duration::from_secs(10)
        } else {
            Duration::from_millis(300)
        };
        match tokio::time::timeout(left, subscription.next()).await {
            Err(_) if items.len() >= count => return items,
            Err(_) => {}
            Ok(None) => panic!("the client stopped with {} of {count} items", items.len()),
            Ok(Some(Err(error))) => panic!("subscription: {error}"),
            Ok(Some(Ok(Event::Items(more)))) => items.extend(more),
            Ok(Some(Ok(_))) => {}
        }
    }
}

/// Until `SUBSCRIBED`, so that what is published next is seen live.
pub async fn until_subscribed(subscription: &mut Subscription) -> (u64, u64, Vec<u8>) {
    loop {
        match tokio::time::timeout(WAIT, subscription.next()).await {
            Ok(Some(Ok(Event::Subscribed {
                head,
                state_ver,
                state,
            }))) => {
                return (head, state_ver, state);
            }
            Ok(Some(Ok(_))) => {}
            other => panic!("no SUBSCRIBED: {other:?}"),
        }
    }
}
