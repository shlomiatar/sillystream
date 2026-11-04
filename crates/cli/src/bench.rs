//! The load generator: the real server binary, over real TLS and WebSocket, through the SDK.
//! It prints Markdown tables for docs/BENCH.md.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use sdk::{
    AckLevel, AckWait, BatchConfig, Client, ClientConfig, Event, FROM_LIVE, PublisherConfig,
    SdkError, Subscribe, build_records,
};
use tokio::sync::Semaphore;

use crate::Args;

const SEAL_WINDOW_MS: u64 = 1_000;

/// A server on another machine, set once from the flags. Only benches that need nothing of the
/// server's files run against one, and each client names its streams apart with its prefix.
struct Remote {
    url: String,
    ca_pem: Vec<u8>,
    token: String,
    stream_prefix: String,
    rss_command: Option<String>,
}

static REMOTE: OnceLock<Remote> = OnceLock::new();

/// `--wal-root`: where a local server's WAL goes instead of its temporary directory, so the
/// WAL can have a disk of its own.
static WAL_ROOT: OnceLock<PathBuf> = OnceLock::new();

/// The WAL directory of a local server whose other files are in `dir`.
fn wal_dir(dir: &Path) -> PathBuf {
    match WAL_ROOT.get() {
        Some(root) => root.join(dir.file_name().unwrap_or_default()),
        None => dir.join("wal"),
    }
}

fn stream_name(name: &str) -> String {
    match REMOTE.get() {
        Some(remote) => format!("{}{name}", remote.stream_prefix),
        None => name.to_owned(),
    }
}

/// The server as a child process in a temporary directory, killed when this goes.
struct ServerChild {
    /// `None` when the server is another machine's (`--remote-url`).
    child: Option<Child>,
    bin: PathBuf,
    settings: ServerSettings,
    url: String,
    ca_pem: Vec<u8>,
    token: String,
    dir: tempfile::TempDir,
}

impl Drop for ServerChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
            if WAL_ROOT.get().is_some() {
                let _ = std::fs::remove_dir_all(wal_dir(self.dir.path()));
            }
        }
    }
}

#[derive(Clone, Copy)]
struct ServerSettings {
    cores: u64,
    seal_window_ms: u64,
    active_stream_limit: Option<u64>,
    memory_cap_bytes: Option<u64>,
}

fn server_bin(args: &Args) -> Result<PathBuf, String> {
    if let Some(file) = args.one("--server-bin") {
        return Ok(PathBuf::from(file));
    }
    let me = std::env::current_exe().map_err(|error| error.to_string())?;
    let sibling = me.with_file_name("sillystream-server");
    if sibling.exists() {
        Ok(sibling)
    } else {
        Err(format!(
            "{} isn't there: build the server, or pass --server-bin",
            sibling.display()
        ))
    }
}

fn start_server(bin: &Path, cores: u64) -> Result<ServerChild, String> {
    start_server_with(
        bin,
        ServerSettings {
            cores,
            seal_window_ms: SEAL_WINDOW_MS,
            active_stream_limit: None,
            memory_cap_bytes: None,
        },
    )
}

fn start_server_with(bin: &Path, settings: ServerSettings) -> Result<ServerChild, String> {
    let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
    if let Some(remote) = REMOTE.get() {
        return Ok(ServerChild {
            child: None,
            bin: bin.to_owned(),
            settings,
            url: remote.url.clone(),
            ca_pem: remote.ca_pem.clone(),
            token: remote.token.clone(),
            dir,
        });
    }
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .map_err(|error| error.to_string())?;
    let ca_pem = certified.cert.pem().into_bytes();
    let write = |name: &str, bytes: &[u8]| -> Result<(), String> {
        std::fs::write(dir.path().join(name), bytes).map_err(|error| error.to_string())
    };
    // As text: the server trims whitespace from the end of its secret file, and raw bytes end
    // in whitespace often enough to fail a run with a wrong signature.
    let random_hex = |words: usize| -> Vec<u8> {
        use std::hash::{BuildHasher, Hasher};
        (0..words)
            .map(|_| {
                let word = std::collections::hash_map::RandomState::new()
                    .build_hasher()
                    .finish();
                format!("{word:016x}")
            })
            .collect::<String>()
            .into_bytes()
    };
    let secret = random_hex(8);
    write("cert.pem", &ca_pem)?;
    write("key.pem", certified.key_pair.serialize_pem().as_bytes())?;
    write("secret", &secret)?;
    write("stream-key", &random_hex(4))?;
    let (child, port) = spawn_server(bin, settings, dir.path())?;
    let token = auth::Token {
        expiry_ms: u64::MAX,
        author: std::num::NonZeroU64::MIN,
        grants: vec![auth::Grant {
            pattern: "*".to_owned(),
            perms: auth::PERM_READ | auth::PERM_PUBLISH | auth::PERM_STATE | auth::PERM_ADMIN,
        }],
    }
    .sign(&secret)
    .map_err(|error| format!("{error:?}"))?;
    Ok(ServerChild {
        child: Some(child),
        bin: bin.to_owned(),
        settings,
        url: format!("localhost:{port}"),
        ca_pem,
        token,
        dir,
    })
}

/// Starts it on the files in `dir`, on any free port, with its stderr in `server.stderr`.
fn spawn_server(
    bin: &Path,
    settings: ServerSettings,
    dir: &Path,
) -> Result<(Child, String), String> {
    let text = |error: std::io::Error| error.to_string();
    let path = |name: &str| dir.join(name).to_str().unwrap_or_default().to_owned();
    let stderr = std::fs::File::create(dir.join("server.stderr")).map_err(text)?;
    let mut command = Command::new(bin);
    command
        .args(["--listen", "127.0.0.1:0", "--cert", &path("cert.pem")])
        .args(["--key", &path("key.pem")])
        .args(["--token-secret-file", &path("secret")])
        .args(["--stream-key-file", &path("stream-key")])
        .arg("--wal-dir")
        .arg(wal_dir(dir))
        .args(["--data-dir", &path("data")])
        .arg("--object-store")
        .arg(format!("local:{}", path("objects")))
        .args(["--cores", &settings.cores.to_string()])
        .args(["--seal-window-ms", &settings.seal_window_ms.to_string()])
        .args(["--trace-catch-up", "true"])
        .stdout(Stdio::piped())
        .stderr(stderr);
    if let Some(limit) = settings.active_stream_limit {
        command.args(["--active-stream-limit", &limit.to_string()]);
    }
    if let Some(cap_bytes) = settings.memory_cap_bytes {
        command.args(["--memory-cap-bytes", &cap_bytes.to_string()]);
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("{}: {error}", bin.display()))?;
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let mut line = String::new();
    let mut reader = BufReader::new(stdout);
    reader.read_line(&mut line).map_err(text)?;
    let Some(address) = line.trim().strip_prefix("listening on ") else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("the server said {line:?}"));
    };
    let port = address.rsplit(':').next().unwrap_or_default().to_owned();
    Ok((child, port))
}

impl ServerChild {
    async fn client(&self) -> Result<Client, String> {
        Client::connect(ClientConfig::new(&self.url, &self.ca_pem, &self.token))
            .await
            .map_err(|error| error.to_string())
    }

    /// A clean stop, which seals, by this child's own PID, and a start on the same files.
    fn restart(&mut self, clear_cache: bool) -> Result<(), String> {
        let text = |error: std::io::Error| error.to_string();
        let child = self
            .child
            .as_mut()
            .ok_or("a remote server can't be restarted")?;
        let pid = child.id().to_string();
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .map_err(text)?;
        let status = child.wait().map_err(text)?;
        if !status.success() {
            return Err(format!("the server stopped with {status}"));
        }
        if clear_cache {
            std::fs::remove_dir_all(self.dir.path().join("data/cache")).map_err(text)?;
        }
        let (child, port) = spawn_server(&self.bin, self.settings, self.dir.path())?;
        self.child = Some(child);
        self.url = format!("localhost:{port}");
        Ok(())
    }

    /// Bytes this start read for catch-ups from each tier, by its own trace.
    fn tier_bytes(&self, stream: &str) -> Result<[u64; 3], String> {
        let log = std::fs::read_to_string(self.dir.path().join("server.stderr"))
            .map_err(|error| error.to_string())?;
        let mut out = [0; 3];
        for line in log.lines() {
            let mut parts = line.split(' ');
            if parts.next() != Some("trace") || parts.next() != Some("catch_up") {
                continue;
            }
            // `tier=<tier> bytes=<n> stream="<id>"` (D226).
            let (Some(tier), Some(bytes), Some(name)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            if name.strip_prefix("stream=") != Some(&format!("{stream:?}")) {
                continue;
            }
            let at = match tier {
                "tier=memory" => 0,
                "tier=local" => 1,
                _ => 2,
            };
            out[at] += bytes
                .strip_prefix("bytes=")
                .and_then(|text| text.parse::<u64>().ok())
                .unwrap_or(0);
        }
        Ok(out)
    }

    fn rss_bytes(&self) -> Result<u64, String> {
        let output = match (&self.child, REMOTE.get()) {
            (Some(child), _) => Command::new("ps")
                .args(["-o", "rss=", "-p", &child.id().to_string()])
                .output(),
            (
                None,
                Some(Remote {
                    rss_command: Some(command),
                    ..
                }),
            ) => Command::new("sh").args(["-c", command]).output(),
            _ => return Err("idle against a remote server needs --rss-command".to_owned()),
        }
        .map_err(|error| error.to_string())?;
        let text = String::from_utf8_lossy(&output.stdout);
        let kib: u64 = text
            .trim()
            .parse()
            .map_err(|_| format!("ps said {text:?}"))?;
        Ok(kib * 1024)
    }
}

/// New connections: TCP, the full TLS 1.3 handshake (no resumption), the upgrade and `HELLO`,
/// which is what every client pays again after a server restart.
async fn connect_rate(args: &Args, bin: &Path) -> Result<(), String> {
    let count = args.number("--connect-count")?.unwrap_or(5_000);
    let cores = args.number("--cores")?.unwrap_or(4);
    let server = start_server(bin, cores)?;
    println!("### Connection setup, TCP to `HELLO` answered, {cores} cores\n");
    println!("| at once | connections | failed | per second | p50 µs | p99 µs | p99.9 µs |");
    println!("|---|---|---|---|---|---|---|");
    for concurrency in number_list(args, "--connect-concurrency", "1,16,256")? {
        let (url, ca_pem, token) = (
            server.url.clone(),
            server.ca_pem.clone(),
            server.token.clone(),
        );
        let next = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..concurrency {
            let (url, ca_pem, token) = (url.clone(), ca_pem.clone(), token.clone());
            let (next, failed) = (next.clone(), failed.clone());
            tasks.push(tokio::spawn(async move {
                let mut times_us = Vec::new();
                while next.fetch_add(1, Ordering::Relaxed) < count {
                    let began = Instant::now();
                    match Client::connect(ClientConfig::new(&url, &ca_pem, &token)).await {
                        Ok(client) => {
                            times_us.push(began.elapsed().as_secs_f64() * 1e6);
                            drop(client);
                        }
                        Err(_) => {
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                times_us
            }));
        }
        let mut samples = Vec::new();
        for task in tasks {
            samples.extend(task.await.map_err(|error| error.to_string())?);
        }
        let seconds = started.elapsed().as_secs_f64();
        samples.sort_by(f64::total_cmp);
        if samples.is_empty() {
            return Err("no connection succeeded".to_owned());
        }
        println!(
            "| {concurrency} | {} | {} | {:.0} | {:.0} | {:.0} | {:.0} |",
            samples.len(),
            failed.load(Ordering::Relaxed),
            samples.len() as f64 / seconds,
            percentile(&samples, 0.50),
            percentile(&samples, 0.99),
            percentile(&samples, 0.999),
        );
    }
    println!();
    Ok(())
}

/// One encoded record of `data_bytes`, which don't compress, and aren't asked to.
fn record(data_bytes: usize) -> Result<Vec<u8>, String> {
    let data: Vec<u8> = (0..data_bytes).map(|at| (at * 31 % 251) as u8).collect();
    let raw = BatchConfig {
        zstd_level: None,
        ..BatchConfig::default()
    };
    let mut records = build_records(&[(1, &data)], &raw).map_err(|error| error.to_string())?;
    Ok(records.remove(0))
}

fn percentile(sorted_us: &[f64], fraction: f64) -> f64 {
    let at = ((sorted_us.len() as f64 * fraction).ceil() as usize).clamp(1, sorted_us.len());
    sorted_us[at - 1]
}

fn publisher_config(ack: AckLevel) -> PublisherConfig {
    PublisherConfig {
        ack,
        ..PublisherConfig::default()
    }
}

/// Closed loop, one at a time: each ack before the next publish.
async fn ack_latencies(client: &Client, ack: AckLevel, count: u64) -> Result<Vec<f64>, String> {
    let stream = stream_name(&format!("latency.{}", ack as u8));
    let mut publisher = client
        .publisher(&stream, publisher_config(ack))
        .map_err(|error| error.to_string())?;
    let body = record(256)?;
    let mut samples = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let started = Instant::now();
        let wait = publisher
            .send_records(&body, None)
            .await
            .map_err(|error| error.to_string())?;
        wait.wait().await.map_err(|error| error.to_string())?;
        samples.push(started.elapsed().as_secs_f64() * 1e6);
    }
    Ok(samples)
}

/// Level 3 takes a seal window, so one at a time would be a sample a second. These go out at a
/// steady rate across it instead, which is also what shows the half-a-window spread.
async fn level_3_latencies(client: &Client, count: u64) -> Result<Vec<f64>, String> {
    let mut publisher = client
        .publisher("latency.3", publisher_config(AckLevel::SegmentInS3))
        .map_err(|error| error.to_string())?;
    let body = record(256)?;
    let mut tasks = Vec::with_capacity(count as usize);
    let mut ticks = tokio::time::interval(Duration::from_millis(20));
    for _ in 0..count {
        ticks.tick().await;
        let started = Instant::now();
        let wait: AckWait = publisher
            .send_records(&body, None)
            .await
            .map_err(|error| error.to_string())?;
        tasks.push(tokio::spawn(async move {
            wait.wait().await?;
            Ok::<f64, SdkError>(started.elapsed().as_secs_f64() * 1e6)
        }));
    }
    let mut samples = Vec::with_capacity(tasks.len());
    for task in tasks {
        samples.push(
            task.await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(samples)
}

/// Level 0 has no ack to time, so this is publish to delivery on a subscriber of the same stream.
async fn delivery_latencies(server: &ServerChild, count: u64) -> Result<Vec<f64>, String> {
    let reader = server.client().await?;
    let writer = server.client().await?;
    let mut publisher = writer
        .publisher("latency.0", publisher_config(AckLevel::None))
        .map_err(|error| error.to_string())?;
    let body = record(256)?;
    // A stream has to exist to subscribe to it.
    publisher
        .send_records(&body, None)
        .await
        .map_err(|error| error.to_string())?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut subscription = reader
        .subscribe(Subscribe::new("latency.0", FROM_LIVE))
        .map_err(|error| error.to_string())?;
    let mut samples = Vec::with_capacity(count as usize);
    let timeout = Duration::from_secs(10);
    loop {
        match tokio::time::timeout(timeout, subscription.next()).await {
            Ok(Some(Ok(Event::Subscribed { .. }))) => break,
            Ok(Some(Ok(_))) => {}
            other => return Err(format!("no SUBSCRIBED: {other:?}")),
        }
    }
    for _ in 0..count {
        let started = Instant::now();
        publisher
            .send_records(&body, None)
            .await
            .map_err(|error| error.to_string())?;
        loop {
            match tokio::time::timeout(timeout, subscription.next()).await {
                Ok(Some(Ok(Event::Items(_)))) => break,
                Ok(Some(Ok(_))) => {}
                other => return Err(format!("no delivery: {other:?}")),
            }
        }
        samples.push(started.elapsed().as_secs_f64() * 1e6);
    }
    Ok(samples)
}

fn latency_row(label: &str, measures: &str, samples: &mut [f64]) -> String {
    samples.sort_by(f64::total_cmp);
    format!(
        "| {label} | {measures} | {} | {:.0} | {:.0} | {:.0} |",
        samples.len(),
        percentile(samples, 0.50),
        percentile(samples, 0.99),
        percentile(samples, 0.999),
    )
}

async fn latency(args: &Args, bin: &Path) -> Result<(), String> {
    let count = args.number("--count")?.unwrap_or(5_000);
    let cores = args.number("--cores")?.unwrap_or(4);
    let levels = args.one("--levels").unwrap_or("0,1,2,3");
    let server = start_server(bin, cores)?;
    let client = server.client().await?;
    println!("### Latency, 256-byte record, one publisher, {cores} cores\n");
    println!("| level | measures | samples | p50 µs | p99 µs | p99.9 µs |");
    println!("|---|---|---|---|---|---|");
    for level in levels.split(',') {
        let row = match level {
            "0" => {
                let mut samples = delivery_latencies(&server, count).await?;
                latency_row("0", "publish to delivery", &mut samples)
            }
            "1" => {
                let mut samples = ack_latencies(&client, AckLevel::Memory, count).await?;
                latency_row("1", "ack, closed loop", &mut samples)
            }
            "2" => {
                let mut samples = ack_latencies(&client, AckLevel::WalFsync, count).await?;
                latency_row("2", "ack, closed loop, local disk", &mut samples)
            }
            "3" => {
                let mut samples = level_3_latencies(&client, (count / 5).max(1)).await?;
                latency_row(
                    "3",
                    "ack at 50 per second, `LocalFileSystem`, 1 s window",
                    &mut samples,
                )
            }
            other => return Err(format!("{other} isn't a level")),
        };
        println!("{row}");
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
    }
    println!();
    Ok(())
}

/// As fast as the window allows, for `seconds`, on `connection_count` sockets spread over
/// `stream_count` streams.
async fn throughput_run(
    server: &ServerChild,
    ack: AckLevel,
    connection_count: u64,
    data_bytes: usize,
    seconds: u64,
    stream: &str,
    stream_count: u64,
) -> Result<(u64, f64), String> {
    let body = Arc::new(record(data_bytes)?);
    let acked = Arc::new(AtomicU64::new(0));
    // Up to 8 MiB in flight on each of a few connections, and 256 MiB in all past 32.
    let inflight_bytes = (256 * 1024 * 1024 / connection_count as usize)
        .clamp(data_bytes * 4, (8 * 1024 * 1024).max(data_bytes * 16));
    // Connected first, so that with thousands of connections the setup isn't timed.
    let mut publishers = Vec::new();
    for at in 0..connection_count {
        let client = server.client().await?;
        let publisher = client
            .publisher(
                &stream_name(&format!("{stream}.{}", at % stream_count)),
                PublisherConfig {
                    ack,
                    max_inflight_bytes: inflight_bytes,
                    ..PublisherConfig::default()
                },
            )
            .map_err(|error| error.to_string())?;
        publishers.push((client, publisher));
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for (client, mut publisher) in publishers {
        let body = Arc::clone(&body);
        let acked = Arc::clone(&acked);
        tasks.push(tokio::spawn(async move {
            let mut waits = Vec::new();
            let waiters = Arc::new(Semaphore::new(4096));
            while Instant::now() < deadline {
                let permit = Arc::clone(&waiters).acquire_owned().await.unwrap();
                let wait = publisher.send_records(&body, None).await?;
                let acked = Arc::clone(&acked);
                waits.push(tokio::spawn(async move {
                    let result = wait.wait().await;
                    drop(permit);
                    if result.is_ok() {
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                    result.map(|_| ())
                }));
                // Finished ones go, or a long run would hold a handle for every publish.
                if waits.len() >= 8192 {
                    waits.retain(|task: &tokio::task::JoinHandle<_>| !task.is_finished());
                }
            }
            for wait in waits {
                wait.await.map_err(|_| SdkError::Closed)??;
            }
            drop(client);
            Ok::<(), SdkError>(())
        }));
    }
    for task in tasks {
        task.await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
    }
    Ok((
        acked.load(Ordering::Relaxed),
        started.elapsed().as_secs_f64(),
    ))
}

async fn throughput(args: &Args, bin: &Path) -> Result<(), String> {
    let seconds = args.number("--seconds")?.unwrap_or(10);
    let cores = args.number("--cores")?.unwrap_or(4);
    let stream_count = args.number("--streams")?.unwrap_or(1).max(1);
    let connection_counts = number_list(args, "--connections", "1,4")?;
    let record_sizes = number_list(args, "--record-bytes", "1024,65536")?;
    let levels = args.one("--levels").unwrap_or("1,2");
    println!("### Throughput, {stream_count} streams, {cores} cores, {seconds} s each\n");
    println!("| ack | connections | record data bytes | publishes/s | MB/s of data |");
    println!("|---|---|---|---|---|");
    let mut run = 0;
    for (ack, name) in [(AckLevel::Memory, "1"), (AckLevel::WalFsync, "2")] {
        if !levels.split(',').any(|level| level == name) {
            continue;
        }
        for &connection_count in &connection_counts {
            for &data_bytes in &record_sizes {
                let data_bytes = data_bytes as usize;
                // A server per row, so that one row's memory cap doesn't slow the next.
                let server = start_server(bin, cores)?;
                run += 1;
                let stream = format!("throughput.{run}");
                let (acked, elapsed) = throughput_run(
                    &server,
                    ack,
                    connection_count,
                    data_bytes,
                    seconds,
                    &stream,
                    stream_count,
                )
                .await?;
                let per_second = acked as f64 / elapsed;
                println!(
                    "| {name} | {connection_count} | {data_bytes} | {per_second:.0} | {:.1} |",
                    per_second * data_bytes as f64 / 1e6
                );
                std::io::stdout()
                    .flush()
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    println!();
    Ok(())
}

async fn idle(args: &Args, bin: &Path) -> Result<(), String> {
    let count = args.number("--idle-count")?.unwrap_or(10_000);
    let cores = args.number("--cores")?.unwrap_or(4);
    let server = start_server(bin, cores)?;
    // A little work first, so that what is measured isn't first-use allocation of the runtime.
    let warm = server.client().await?;
    let mut publisher = warm
        .publisher(&stream_name("warm"), PublisherConfig::default())
        .map_err(|error| error.to_string())?;
    publisher.add(0, b"warm".to_vec());
    publisher.flush().await.map_err(|error| error.to_string())?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = server.rss_bytes()?;

    let gate = Arc::new(Semaphore::new(64));
    let mut tasks = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let config = ClientConfig {
            // Idle means idle: no pings in the time this takes.
            ping_interval_ms: 3_600_000,
            ..ClientConfig::new(&server.url, &server.ca_pem, &server.token)
        };
        let gate = Arc::clone(&gate);
        tasks.push(tokio::spawn(async move {
            let _permit = gate.acquire_owned().await.unwrap();
            let mut tries = 0;
            loop {
                match Client::connect(config.clone()).await {
                    Err(error) if tries < 100 => {
                        // Local ports still in TIME_WAIT from an earlier run.
                        tries += 1;
                        let _ = error;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    other => break other,
                }
            }
        }));
    }
    let mut clients = Vec::with_capacity(tasks.len());
    for task in tasks {
        clients.push(
            task.await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?,
        );
    }
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let after = server.rss_bytes()?;
    println!("### Memory per idle connection, {cores} cores\n");
    println!("| connections | server RSS before | server RSS after | bytes per connection |");
    println!("|---|---|---|---|");
    println!(
        "| {count} | {before} | {after} | {:.0} |",
        after.saturating_sub(before) as f64 / count as f64
    );
    println!();
    drop(clients);
    Ok(())
}

/// From `from = 0` to the head, through the SDK. Returns bytes of items and seconds.
async fn read_to_head(
    server: &ServerChild,
    stream: &str,
    count: u64,
) -> Result<(u64, f64), String> {
    let client = server.client().await?;
    let started = Instant::now();
    let mut subscription = client
        .subscribe(Subscribe::new(stream, 0))
        .map_err(|error| error.to_string())?;
    let (mut got_count, mut got_bytes) = (0, 0);
    while got_count < count {
        match tokio::time::timeout(Duration::from_secs(60), subscription.next()).await {
            Ok(Some(Ok(Event::Items(items)))) => {
                got_count += items.len() as u64;
                got_bytes += items
                    .iter()
                    .map(|item| item.bytes.len() as u64)
                    .sum::<u64>();
            }
            Ok(Some(Ok(_))) => {}
            other => return Err(format!("catching up: {other:?}")),
        }
    }
    Ok((got_bytes, started.elapsed().as_secs_f64()))
}

/// Section 5.1's tiers, each from `from = 0`, and each named by the server's own trace.
async fn catch_up(args: &Args, bin: &Path) -> Result<(), String> {
    let cores = args.number("--cores")?.unwrap_or(4);
    let total_bytes = args.number("--catch-up-mib")?.unwrap_or(64) * 1024 * 1024;
    const DATA_BYTES: u64 = 4096;
    let count = total_bytes / DATA_BYTES;
    let mut server = start_server_with(
        bin,
        ServerSettings {
            cores,
            // The longest window, so that the first reads find it all still in memory.
            seal_window_ms: 120_000,
            active_stream_limit: None,
            memory_cap_bytes: None,
        },
    )?;
    let stream = "catchup";
    {
        let client = server.client().await?;
        let mut publisher = client
            .publisher(stream, publisher_config(AckLevel::WalFsync))
            .map_err(|error| error.to_string())?;
        let body = record(DATA_BYTES as usize)?;
        let mut waits = Vec::new();
        for _ in 0..count {
            waits.push(
                publisher
                    .send_records(&body, None)
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
        for wait in waits {
            wait.wait().await.map_err(|error| error.to_string())?;
        }
    }
    println!("### Catch-up rate, {count} records of {DATA_BYTES} bytes, {cores} cores\n");
    println!(
        "| tier | run | MB/s of data | bytes from memory | bytes from local | bytes from object store |"
    );
    println!("|---|---|---|---|---|---|");
    for (tier, restart, clear_cache) in [
        ("memory", false, false),
        ("local disk", true, false),
        ("`LocalFileSystem` object store", true, true),
    ] {
        if restart {
            server.restart(clear_cache)?;
        }
        let mut before = server.tier_bytes(stream)?;
        for run in 1..=3 {
            let (bytes, seconds) = read_to_head(&server, stream, count).await?;
            let after = server.tier_bytes(stream)?;
            println!(
                "| {tier} | {run} | {:.0} | {} | {} | {} |",
                bytes as f64 / seconds / 1e6,
                after[0] - before[0],
                after[1] - before[1],
                after[2] - before[2],
            );
            before = after;
        }
    }
    println!();
    Ok(())
}

/// Many streams at once, each on its own connection: past one core's limit, where one stream
/// can't go (section 6.1).
async fn many_streams(args: &Args, bin: &Path) -> Result<(), String> {
    let seconds = args.number("--seconds")?.unwrap_or(10);
    let cores = args.number("--cores")?.unwrap_or(16);
    println!("### Throughput, many streams, {cores} cores, {seconds} s each\n");
    println!("| ack | streams | record data bytes | publishes/s | MB/s of data |");
    println!("|---|---|---|---|---|");
    for (ack, name) in [(AckLevel::Memory, "1"), (AckLevel::WalFsync, "2")] {
        for stream_count in [1u64, 16, 64] {
            let data_bytes = 1024usize;
            let server = Arc::new(start_server(bin, cores)?);
            let mut runs = Vec::new();
            for at in 0..stream_count {
                let server = Arc::clone(&server);
                runs.push(tokio::spawn(async move {
                    let stream = format!("many.{at}");
                    throughput_run(&server, ack, 1, data_bytes, seconds, &stream, 1).await
                }));
            }
            let mut per_second = 0.0;
            for run in runs {
                let (acked, elapsed) = run.await.map_err(|error| error.to_string())??;
                per_second += acked as f64 / elapsed;
            }
            println!(
                "| {name} | {stream_count} | {data_bytes} | {per_second:.0} | {:.1} |",
                per_second * data_bytes as f64 / 1e6
            );
            std::io::stdout()
                .flush()
                .map_err(|error| error.to_string())?;
        }
    }
    println!();
    Ok(())
}

fn count_files(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            if entry.path().is_dir() {
                count_files(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

/// Objects a node adds to the store per seal, with every stream changing in every window. On
/// `LocalFileSystem` a new file is one PUT.
async fn puts_per_seal(args: &Args, bin: &Path) -> Result<(), String> {
    let cores = args.number("--cores")?.unwrap_or(4);
    const WINDOW_COUNT: u64 = 8;
    println!(
        "### Objects per seal, {cores} cores, 1 s window, every stream written in every window\n"
    );
    println!(
        "| streams | windows | new objects in `sealed/` per window | other new objects per window |"
    );
    println!("|---|---|---|---|");
    for stream_count in [10u64, 1_000, 10_000] {
        let server = start_server(bin, cores)?;
        let client = server.client().await?;
        let body = record(64)?;
        let mut publishers = Vec::with_capacity(stream_count as usize);
        for at in 0..stream_count {
            publishers.push(
                client
                    .publisher(&format!("puts.{at}"), publisher_config(AckLevel::Memory))
                    .map_err(|error| error.to_string())?,
            );
        }
        let publish_all = async |publishers: &mut Vec<sdk::Publisher>| -> Result<(), String> {
            let mut waits = Vec::with_capacity(publishers.len());
            for publisher in publishers.iter_mut() {
                waits.push(
                    publisher
                        .send_records(&body, None)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
            for wait in waits {
                wait.wait().await.map_err(|error| error.to_string())?;
            }
            Ok(())
        };
        // Past the first window, whose objects are the ones that first create things.
        publish_all(&mut publishers).await?;
        tokio::time::sleep(Duration::from_millis(2 * SEAL_WINDOW_MS)).await;
        let objects = server.dir.path().join("objects");
        let sealed = objects.join("sealed");
        let before = (count_files(&sealed), count_files(&objects));
        let started = Instant::now();
        for round in 1..=WINDOW_COUNT {
            publish_all(&mut publishers).await?;
            let next = Duration::from_millis(round * SEAL_WINDOW_MS);
            tokio::time::sleep(next.saturating_sub(started.elapsed())).await;
        }
        let after = (count_files(&sealed), count_files(&objects));
        let windows = started.elapsed().as_secs_f64() * 1000.0 / SEAL_WINDOW_MS as f64;
        let in_sealed = (after.0 - before.0) as f64;
        let others = (after.1 - before.1) as f64 - in_sealed;
        println!(
            "| {stream_count} | {windows:.1} | {:.2} | {:.2} |",
            in_sealed / windows,
            others / windows,
        );
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
    }
    println!();
    Ok(())
}

/// RSS per stream with one small record each, nobody reading, kept active and then with a limit.
async fn idle_streams(args: &Args, bin: &Path) -> Result<(), String> {
    let cores = args.number("--cores")?.unwrap_or(4);
    let count = args.number("--idle-streams")?.unwrap_or(20_000);
    println!("### Memory per idle stream, {cores} cores\n");
    println!(
        "| streams | active limit | server RSS before | server RSS after | bytes per stream |"
    );
    println!("|---|---|---|---|---|");
    for limit in [None, Some(1_000u64)] {
        let server = start_server_with(
            bin,
            ServerSettings {
                cores,
                seal_window_ms: SEAL_WINDOW_MS,
                active_stream_limit: limit,
                memory_cap_bytes: None,
            },
        )?;
        let client = server.client().await?;
        let body = record(64)?;
        let mut warm = client
            .publisher("warm", publisher_config(AckLevel::WalFsync))
            .map_err(|error| error.to_string())?;
        warm.send_records(&body, None)
            .await
            .map_err(|error| error.to_string())?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        tokio::time::sleep(Duration::from_millis(2 * SEAL_WINDOW_MS)).await;
        let before = server.rss_bytes()?;
        let mut waits = Vec::with_capacity(count as usize);
        let mut publishers = Vec::with_capacity(count as usize);
        for at in 0..count {
            let mut publisher = client
                .publisher(&format!("idle.{at}"), publisher_config(AckLevel::WalFsync))
                .map_err(|error| error.to_string())?;
            waits.push(
                publisher
                    .send_records(&body, None)
                    .await
                    .map_err(|error| error.to_string())?,
            );
            publishers.push(publisher);
        }
        for wait in waits {
            wait.wait().await.map_err(|error| error.to_string())?;
        }
        drop(publishers);
        // Long enough for the records to seal and, with a limit, for streams to leave RAM.
        tokio::time::sleep(Duration::from_millis(6 * SEAL_WINDOW_MS)).await;
        let after = server.rss_bytes()?;
        let shown = limit.map_or("none".to_owned(), |limit| limit.to_string());
        println!(
            "| {count} | {shown} | {before} | {after} | {:.0} |",
            after.saturating_sub(before) as f64 / count as f64
        );
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
    }
    println!();
    Ok(())
}

/// Log-spaced buckets, 32 per doubling of µs, so that about 2% is the resolution and a sample
/// costs one atomic add, which is what lets a run take every delivery as a sample.
struct Histogram {
    buckets: Vec<AtomicU64>,
    sample_count: AtomicU64,
}

impl Histogram {
    fn new() -> Histogram {
        Histogram {
            buckets: (0..32 * 40).map(|_| AtomicU64::new(0)).collect(),
            sample_count: AtomicU64::new(0),
        }
    }

    fn add(&self, micros: f64) {
        let at = (micros.max(1.0).log2() * 32.0) as usize;
        self.buckets[at.min(self.buckets.len() - 1)].fetch_add(1, Ordering::Relaxed);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }

    /// The top of the bucket holding that sample, so slightly over and never under.
    fn percentile_us(&self, fraction: f64) -> f64 {
        let total = self.sample_count.load(Ordering::Relaxed);
        let rank = ((total as f64 * fraction).ceil() as u64).max(1);
        let mut seen = 0;
        for (at, bucket) in self.buckets.iter().enumerate() {
            seen += bucket.load(Ordering::Relaxed);
            if seen >= rank {
                return 2f64.powf((at + 1) as f64 / 32.0);
            }
        }
        f64::NAN
    }

    fn cells(&self) -> String {
        if self.sample_count.load(Ordering::Relaxed) == 0 {
            return "none | none | none".to_owned();
        }
        format!(
            "{:.0} | {:.0} | {:.0}",
            self.percentile_us(0.50),
            self.percentile_us(0.99),
            self.percentile_us(0.999),
        )
    }
}

/// The record's data is `DATA_OFFSET` into it, and starts with µs since `zero`, for the far end.
const DATA_OFFSET: usize = 32;

fn stamp_sent_at(body: &mut [u8], zero: Instant) {
    let micros = zero.elapsed().as_micros() as u64;
    body[DATA_OFFSET..DATA_OFFSET + 8].copy_from_slice(&micros.to_le_bytes());
}

fn take_sent_at(bytes: &[u8], zero: Instant) -> Result<f64, String> {
    let head: [u8; 8] = bytes
        .get(..8)
        .and_then(|head| head.try_into().ok())
        .ok_or("an item shorter than its time")?;
    let sent = u64::from_le_bytes(head);
    let now = zero.elapsed().as_micros() as u64;
    // A wrong offset would otherwise read as a very odd latency instead of as a bug here.
    if sent > now {
        return Err(format!("an item sent at {sent} µs, and it is {now} µs"));
    }
    Ok((now - sent) as f64)
}

struct LoadRun {
    sent_count: u64,
    acked_count: u64,
    failed_count: u64,
    seconds: f64,
    delivered_count: u64,
    delivered_seconds: f64,
    acks: Arc<Histogram>,
    deliveries: Arc<Histogram>,
}

#[derive(Clone, Copy)]
struct LoadShape {
    ack: AckLevel,
    stream_count: u64,
    /// Answers waited for per stream, and past it the sender waits.
    outstanding_count: usize,
    /// Publishes per second per stream, spaced evenly and not waiting for acks. 0 is as fast as
    /// `outstanding_count` lets it.
    rate_per_second: u64,
    data_bytes: usize,
    seconds: u64,
    /// A subscriber on each stream, for level 0, which has nothing else to show what arrived.
    with_subscriber: bool,
}

/// `stream_count` streams with a connection each, for `seconds`. At a set rate an ack is timed
/// from when its publish was due, so a stall counts against everything it delayed. In a closed
/// window it is timed from taking a slot: waiting for one is waiting for the publish before it,
/// and a stall can hide in the samples the window held back.
async fn load_run(server: &ServerChild, shape: LoadShape) -> Result<LoadRun, String> {
    let acks = Arc::new(Histogram::new());
    let deliveries = Arc::new(Histogram::new());
    let (sent, acked, failed) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    let delivered = Arc::new(AtomicU64::new(0));
    let zero = Instant::now();
    let mut body = record(shape.data_bytes.max(8))?;
    let bytes_each = body.len();
    let mut publishers = Vec::new();
    let mut readers = Vec::new();
    for at in 0..shape.stream_count {
        let client = server.client().await?;
        let stream = stream_name(&format!("load.{at}"));
        let mut publisher = client
            .publisher(
                &stream,
                PublisherConfig {
                    ack: shape.ack,
                    max_inflight_bytes: (64 * 1024 * 1024)
                        .max(bytes_each * shape.outstanding_count.max(16)),
                    ..PublisherConfig::default()
                },
            )
            .map_err(|error| error.to_string())?;
        // The stream has to exist before a subscriber can be on it. Level 1 for this one, since
        // level 0 wouldn't say when it does.
        let mut opener = client
            .publisher(&stream, publisher_config(AckLevel::Memory))
            .map_err(|error| error.to_string())?;
        stamp_sent_at(&mut body, zero);
        opener
            .send_records(&body, None)
            .await
            .map_err(|error| error.to_string())?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        drop(opener);
        if shape.with_subscriber {
            let reader = server.client().await?;
            let mut subscription = reader
                .subscribe(Subscribe::new(&stream, FROM_LIVE))
                .map_err(|error| error.to_string())?;
            wait_subscribed(&mut subscription).await?;
            let (delivered, deliveries) = (Arc::clone(&delivered), Arc::clone(&deliveries));
            readers.push(tokio::spawn(async move {
                let _reader = reader;
                while let Some(Ok(event)) = subscription.next().await {
                    if let Event::Items(items) = event {
                        for item in &items {
                            if let Ok(micros) = take_sent_at(&item.bytes, zero) {
                                deliveries.add(micros);
                            }
                        }
                        delivered.fetch_add(items.len() as u64, Ordering::Relaxed);
                    }
                }
            }));
        }
        let mut body = body.clone();
        let (sent, acked, failed, acks) = (
            Arc::clone(&sent),
            Arc::clone(&acked),
            Arc::clone(&failed),
            Arc::clone(&acks),
        );
        let slots = Arc::new(Semaphore::new(shape.outstanding_count));
        let seconds = shape.seconds;
        let rate = shape.rate_per_second;
        let track_acks = shape.ack != AckLevel::None;
        let outstanding_count = shape.outstanding_count as u32;
        publishers.push(tokio::spawn(async move {
            let _client = client;
            let started = Instant::now();
            let deadline = started + Duration::from_secs(seconds);
            let mut sent_here = 0u64;
            while Instant::now() < deadline {
                let due = (rate > 0)
                    .then(|| started + Duration::from_secs_f64(sent_here as f64 / rate as f64));
                if let Some(due) = due {
                    tokio::time::sleep_until(due.into()).await;
                }
                let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
                let begun = due.unwrap_or_else(Instant::now);
                stamp_sent_at(&mut body, zero);
                let wait = publisher
                    .send_records(&body, None)
                    .await
                    .map_err(|error| error.to_string())?;
                sent_here += 1;
                sent.fetch_add(1, Ordering::Relaxed);
                let (acked, failed, acks) =
                    (Arc::clone(&acked), Arc::clone(&failed), Arc::clone(&acks));
                tokio::spawn(async move {
                    let result = wait.wait().await;
                    drop(permit);
                    if result.is_err() {
                        failed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        acked.fetch_add(1, Ordering::Relaxed);
                        if track_acks {
                            acks.add(begun.elapsed().as_secs_f64() * 1e6);
                        }
                    }
                });
            }
            let _all_answered = slots.acquire_many(outstanding_count).await;
            Ok::<f64, String>(started.elapsed().as_secs_f64())
        }));
    }
    let mut seconds: f64 = 0.0;
    for task in publishers {
        seconds = seconds.max(task.await.map_err(|error| error.to_string())??);
    }
    // Whatever arrives after the last send still counts, until it has all come or it goes quiet.
    let delivered_seconds = if shape.with_subscriber {
        let want = sent.load(Ordering::Relaxed);
        let delivered_seconds = wait_delivered(&delivered, want, seconds).await;
        for reader in readers {
            reader.abort();
        }
        delivered_seconds
    } else {
        0.0
    };
    Ok(LoadRun {
        sent_count: sent.load(Ordering::Relaxed),
        acked_count: acked.load(Ordering::Relaxed),
        failed_count: failed.load(Ordering::Relaxed),
        seconds,
        delivered_count: delivered.load(Ordering::Relaxed),
        delivered_seconds,
        acks,
        deliveries,
    })
}

/// Until `delivered` reaches `want` or stays put for 45 s, which is past the stalls of an
/// overloaded loopback socket seen on macOS (docs/BENCH.md). Returns seconds from the load's start
/// to the last change seen, given that `load_seconds` of it have passed.
async fn wait_delivered(delivered: &AtomicU64, want: u64, load_seconds: f64) -> f64 {
    let started = Instant::now();
    let mut last_change = started;
    let mut last = delivered.load(Ordering::Relaxed);
    while last < want && last_change.elapsed() < Duration::from_secs(45) {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let now = delivered.load(Ordering::Relaxed);
        if now != last {
            last = now;
            last_change = Instant::now();
        }
    }
    load_seconds + last_change.duration_since(started).as_secs_f64()
}

async fn wait_subscribed(subscription: &mut sdk::Subscription) -> Result<(), String> {
    loop {
        match tokio::time::timeout(Duration::from_secs(120), subscription.next()).await {
            Ok(Some(Ok(Event::Subscribed { .. }))) => return Ok(()),
            Ok(Some(Ok(_))) => {}
            other => return Err(format!("no SUBSCRIBED: {other:?}")),
        }
    }
}

fn number_list(args: &Args, flag: &str, default: &str) -> Result<Vec<u64>, String> {
    args.one(flag)
        .unwrap_or(default)
        .split(',')
        .map(|text| {
            text.parse()
                .map_err(|_| format!("{flag}: {text} isn't a number"))
        })
        .collect()
}

/// Publishes per second and ack latency, for each level and seal window, unloaded and saturated.
async fn ack_matrix(args: &Args, bin: &Path) -> Result<(), String> {
    let cores = args.number("--cores")?.unwrap_or(8);
    let at_least_seconds = args.number("--seconds")?.unwrap_or(15);
    let levels = args.one("--levels").unwrap_or("0,1,2,3");
    const DATA_BYTES: usize = 256;
    const STREAM_COUNT: u64 = 4;
    println!(
        "### Ack levels by seal window, {DATA_BYTES}-byte record, {cores} cores, a new server per row\n"
    );
    println!(
        "| seal window ms | level | load | seconds | publishes/s | ack p50 µs | ack p99 µs | ack p99.9 µs | samples | delivered/s | delivery p50 µs | delivery p99 µs | delivery p99.9 µs | failed |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for window_ms in number_list(args, "--seal-windows", "1000,10000,60000")? {
        // Long enough to hold at least two seals of this window in every row.
        let seconds = at_least_seconds.max(window_ms * 5 / 2 / 1000);
        let shapes: [(&str, &str, LoadShape); 8] = {
            let one = |ack, stream_count, outstanding_count, rate_per_second, with_subscriber| {
                LoadShape {
                    ack,
                    stream_count,
                    outstanding_count,
                    rate_per_second,
                    data_bytes: DATA_BYTES,
                    seconds,
                    with_subscriber,
                }
            };
            [
                (
                    "0",
                    "1 stream, 100/s, with a subscriber",
                    one(AckLevel::None, 1, 1, 100, true),
                ),
                (
                    "0",
                    "4 streams, flat out, with a subscriber each",
                    one(AckLevel::None, STREAM_COUNT, 256, 0, true),
                ),
                (
                    "1",
                    "1 stream, 1 outstanding",
                    one(AckLevel::Memory, 1, 1, 0, false),
                ),
                (
                    "1",
                    "4 streams, 256 outstanding each",
                    one(AckLevel::Memory, STREAM_COUNT, 256, 0, false),
                ),
                (
                    "2",
                    "1 stream, 1 outstanding",
                    one(AckLevel::WalFsync, 1, 1, 0, false),
                ),
                (
                    "2",
                    "4 streams, 256 outstanding each",
                    one(AckLevel::WalFsync, STREAM_COUNT, 256, 0, false),
                ),
                (
                    "3",
                    "1 stream, 50/s",
                    one(AckLevel::SegmentInS3, 1, 65_536, 50, false),
                ),
                (
                    "3",
                    "4 streams, 65,536 outstanding each",
                    one(AckLevel::SegmentInS3, STREAM_COUNT, 65_536, 0, false),
                ),
            ]
        };
        for (level, load, shape) in shapes {
            if !levels.split(',').any(|wanted| wanted == level) {
                continue;
            }
            let server = start_server_with(
                bin,
                ServerSettings {
                    cores,
                    seal_window_ms: window_ms,
                    active_stream_limit: None,
                    memory_cap_bytes: None,
                },
            )?;
            let run = load_run(&server, shape).await?;
            let delivered = if shape.with_subscriber {
                format!(
                    "{:.0}",
                    run.delivered_count as f64 / run.delivered_seconds.max(f64::MIN_POSITIVE)
                )
            } else {
                "not run".to_owned()
            };
            let delivery_cells = if shape.with_subscriber {
                run.deliveries.cells()
            } else {
                "not run | not run | not run".to_owned()
            };
            let ack_cells = if shape.ack == AckLevel::None {
                "no ack | no ack | no ack".to_owned()
            } else {
                run.acks.cells()
            };
            println!(
                "| {window_ms} | {level} | {load} | {:.1} | {:.0} | {ack_cells} | {} | {delivered} | {delivery_cells} | {} |",
                run.seconds,
                run.sent_count as f64 / run.seconds,
                run.acked_count,
                run.failed_count,
            );
            std::io::stdout()
                .flush()
                .map_err(|error| error.to_string())?;
        }
    }
    println!();
    Ok(())
}

/// Connects with the gate and retries `idle` uses, since a run opens thousands at once.
async fn connect_many(server: &ServerChild, count: u64) -> Result<Vec<Client>, String> {
    let gate = Arc::new(Semaphore::new(64));
    let mut tasks = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let config = ClientConfig::new(&server.url, &server.ca_pem, &server.token);
        let gate = Arc::clone(&gate);
        tasks.push(tokio::spawn(async move {
            let _permit = gate.acquire_owned().await.unwrap();
            let mut tries = 0;
            loop {
                match Client::connect(config.clone()).await {
                    Err(_) if tries < 100 => {
                        tries += 1;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    other => break other,
                }
            }
        }));
    }
    let mut clients = Vec::with_capacity(tasks.len());
    for task in tasks {
        clients.push(
            task.await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(clients)
}

/// One stream to `subscriber_count` subscribers on `connection_count` connections.
async fn fan_out_row(
    server: &ServerChild,
    subscriber_count: u64,
    connection_count: u64,
    rate_per_second: u64,
    ack: AckLevel,
    seconds: u64,
) -> Result<(), String> {
    const DATA_BYTES: usize = 256;
    let zero = Instant::now();
    let writer = server.client().await?;
    let stream = stream_name("fan");
    let stream = stream.as_str();
    let mut opener = writer
        .publisher(stream, publisher_config(AckLevel::Memory))
        .map_err(|error| error.to_string())?;
    let mut body = record(DATA_BYTES)?;
    stamp_sent_at(&mut body, zero);
    opener
        .send_records(&body, None)
        .await
        .map_err(|error| error.to_string())?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    drop(opener);

    let readers = connect_many(server, connection_count).await?;
    let delivered = Arc::new(AtomicU64::new(0));
    let behind = Arc::new(AtomicU64::new(0));
    let bad = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Histogram::new());
    let mut tasks = Vec::with_capacity(subscriber_count as usize);
    let mut subscriptions = Vec::with_capacity(subscriber_count as usize);
    for at in 0..subscriber_count {
        let client = &readers[(at % connection_count) as usize];
        subscriptions.push(
            client
                .subscribe(Subscribe::new(stream, FROM_LIVE))
                .map_err(|error| error.to_string())?,
        );
    }
    for mut subscription in subscriptions {
        let (delivered, behind, bad, latencies) = (
            Arc::clone(&delivered),
            Arc::clone(&behind),
            Arc::clone(&bad),
            Arc::clone(&latencies),
        );
        let (ready, ready_wait) = tokio::sync::oneshot::channel();
        tasks.push((
            ready_wait,
            tokio::spawn(async move {
                let subscribed = wait_subscribed(&mut subscription).await;
                let _ = ready.send(subscribed);
                while let Some(Ok(event)) = subscription.next().await {
                    match event {
                        Event::Items(items) => {
                            for item in &items {
                                match take_sent_at(&item.bytes, zero) {
                                    Ok(micros) => latencies.add(micros),
                                    Err(_) => {
                                        bad.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            delivered.fetch_add(items.len() as u64, Ordering::Relaxed);
                        }
                        Event::Behind { .. } => {
                            behind.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
            }),
        ));
    }
    let mut handles = Vec::with_capacity(tasks.len());
    for (ready, handle) in tasks {
        ready
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| format!("{subscriber_count} subscribers: {error}"))?;
        handles.push(handle);
    }

    let outstanding_count = if rate_per_second > 0 { 65_536 } else { 64 };
    let mut publisher = writer
        .publisher(
            stream,
            PublisherConfig {
                ack,
                max_inflight_bytes: 64 * 1024 * 1024,
                ..PublisherConfig::default()
            },
        )
        .map_err(|error| error.to_string())?;
    let slots = Arc::new(Semaphore::new(outstanding_count));
    let started = Instant::now();
    let deadline = started + Duration::from_secs(seconds);
    let mut sent = 0u64;
    while Instant::now() < deadline {
        if rate_per_second > 0 {
            let due = started + Duration::from_secs_f64(sent as f64 / rate_per_second as f64);
            tokio::time::sleep_until(due.into()).await;
        }
        let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
        stamp_sent_at(&mut body, zero);
        let wait = publisher
            .send_records(&body, None)
            .await
            .map_err(|error| error.to_string())?;
        sent += 1;
        tokio::spawn(async move {
            let _ = wait.wait().await;
            drop(permit);
        });
    }
    let send_seconds = started.elapsed().as_secs_f64();
    let want = sent * subscriber_count;
    let total_seconds = wait_delivered(&delivered, want, send_seconds).await;
    let got = delivered.load(Ordering::Relaxed);
    println!(
        "| {subscriber_count} | {connection_count} | {} | {:.0} | {:.0} | {} | {} | {got} | {want} | {} |",
        if rate_per_second > 0 {
            rate_per_second.to_string()
        } else {
            "flat out".to_owned()
        },
        sent as f64 / send_seconds,
        got as f64 / total_seconds.max(f64::MIN_POSITIVE),
        latencies.cells(),
        behind.load(Ordering::Relaxed),
        bad.load(Ordering::Relaxed),
    );
    std::io::stdout()
        .flush()
        .map_err(|error| error.to_string())?;
    for handle in handles {
        handle.abort();
    }
    Ok(())
}

/// Delivered records per second and publish-to-delivery latency, by subscribers.
async fn fan_out(args: &Args, bin: &Path) -> Result<(), String> {
    let cores = args.number("--cores")?.unwrap_or(8);
    let seconds = args.number("--seconds")?.unwrap_or(15);
    let wanted = number_list(args, "--subscribers", "1,100,10000")?;
    println!(
        "### Fan-out, level 1, 256-byte record, one stream, {cores} cores, {seconds} s, a new server per row\n"
    );
    println!(
        "| subscribers | connections | publishes/s asked | publishes/s sent | delivered/s | p50 µs | p99 µs | p99.9 µs | BEHIND | delivered | expected | bad samples |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for subscriber_count in wanted {
        // (connections, rate), with 0 flat out.
        let rows: Vec<(u64, u64)> = match subscriber_count {
            1 => vec![(1, 1_000), (1, 0)],
            100 => vec![(100, 100), (100, 1_000), (100, 0), (4, 1_000)],
            10_000 => vec![
                (10_000, 1),
                (10_000, 10),
                (10_000, 30),
                (10_000, 100),
                (10_000, 0),
                (16, 10),
                (16, 100),
            ],
            other => vec![(other, 10)],
        };
        for (connection_count, rate) in rows {
            let server = start_server(bin, cores)?;
            fan_out_row(
                &server,
                subscriber_count,
                connection_count,
                rate,
                AckLevel::Memory,
                seconds,
            )
            .await?;
        }
    }
    println!();
    Ok(())
}

/// Every directory under `dir`, to `mode`, which is how a run makes uploads fail.
fn set_dir_modes(dir: &Path, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let text = |error: std::io::Error| error.to_string();
    // Children first going down to a closed mode, and the parent first coming back.
    if mode & 0o200 != 0 {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).map_err(text)?;
    }
    for entry in std::fs::read_dir(dir).map_err(text)? {
        let path = entry.map_err(text)?.path();
        if path.is_dir() {
            set_dir_modes(&path, mode)?;
        }
    }
    if mode & 0o200 == 0 {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).map_err(text)?;
    }
    Ok(())
}

fn replayed_entries(dir: &Path) -> Result<u64, String> {
    let log = std::fs::read_to_string(dir.join("server.stderr")).map_err(|e| e.to_string())?;
    let Some(line) = log.lines().rev().find(|line| line.contains("WAL entries")) else {
        return Err("no replay line in the server's stderr".to_owned());
    };
    line.split(" WAL files synced, ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| format!("can't read {line:?}"))
}

/// Time from starting the binary to its port, after `kill -9`, by how much WAL it has to replay.
///
/// Normally a seal empties the WAL. So that it holds a chosen amount the run closes the local
/// object store's directories to writes once the server is up, as an upload outage would, and
/// opens them again before the restart. Records are confirmed afterwards by reading them all.
async fn recovery(args: &Args, bin: &Path) -> Result<(), String> {
    const DATA_BYTES: u64 = 4096;
    const STREAM_COUNT: u64 = 16;
    let cores = args.number("--cores")?.unwrap_or(8);
    println!(
        "### Recovery after `kill -9`, {cores} cores, {STREAM_COUNT} streams, {DATA_BYTES}-byte records\n"
    );
    println!(
        "| WAL asked MiB | run | acked at level 2 | entries replayed | MiB of records replayed | start to port s | read back s | records read back |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for wal_mib in number_list(args, "--wal-mib", "0,16,256,1024,4096")? {
        for run in 1..=args.number("--runs")?.unwrap_or(3) {
            let mut server = start_server_with(
                bin,
                ServerSettings {
                    cores,
                    seal_window_ms: 120_000,
                    active_stream_limit: None,
                    // Above the largest size here, so that this isn't what stops a run.
                    memory_cap_bytes: Some(32 * 1024 * 1024 * 1024),
                },
            )?;
            let objects = server.dir.path().join("objects");
            set_dir_modes(&objects, 0o500)?;
            let per_stream = wal_mib * 1024 * 1024 / DATA_BYTES / STREAM_COUNT;
            let body = Arc::new(record(DATA_BYTES as usize)?);
            let mut tasks = Vec::new();
            let mut names = Vec::new();
            for at in 0..STREAM_COUNT {
                let client = server.client().await?;
                let stream = format!("recover.{at}");
                names.push(stream.clone());
                let body = Arc::clone(&body);
                tasks.push(tokio::spawn(async move {
                    let mut publisher = client
                        .publisher(&stream, publisher_config(AckLevel::WalFsync))
                        .map_err(|error| error.to_string())?;
                    let slots = Arc::new(Semaphore::new(256));
                    for _ in 0..per_stream {
                        let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
                        let wait = publisher
                            .send_records(&body, None)
                            .await
                            .map_err(|error| error.to_string())?;
                        tokio::spawn(async move {
                            let _ = wait.wait().await;
                            drop(permit);
                        });
                    }
                    let _all = slots.acquire_many(256).await;
                    Ok::<(), String>(())
                }));
            }
            for task in tasks {
                task.await.map_err(|error| error.to_string())??;
            }
            let acked = per_stream * STREAM_COUNT;

            // `Child::kill` is SIGKILL, to this child's own PID.
            let child = server
                .child
                .as_mut()
                .ok_or("recovery needs a local server")?;
            child.kill().map_err(|error| error.to_string())?;
            child.wait().map_err(|error| error.to_string())?;
            set_dir_modes(&objects, 0o700)?;

            let started = Instant::now();
            let (child, port) = spawn_server(bin, server.settings, server.dir.path())?;
            let start_seconds = started.elapsed().as_secs_f64();
            server.child = Some(child);
            server.url = format!("localhost:{port}");
            let replayed = replayed_entries(server.dir.path())?;

            let started = Instant::now();
            let mut read_back = 0;
            if per_stream > 0 {
                for name in &names {
                    read_to_head(&server, name, per_stream).await?;
                    read_back += per_stream;
                }
            }
            println!(
                "| {wal_mib} | {run} | {acked} | {replayed} | {:.0} | {start_seconds:.3} | {:.2} | {read_back} |",
                replayed as f64 * (DATA_BYTES as f64 + 32.0) / (1024.0 * 1024.0),
                started.elapsed().as_secs_f64(),
            );
            std::io::stdout()
                .flush()
                .map_err(|error| error.to_string())?;
        }
    }
    println!();
    Ok(())
}

pub(crate) async fn run(args: &Args, what: &str) -> Result<(), String> {
    args.only(&[
        "--server-bin",
        "--count",
        "--seconds",
        "--idle-count",
        "--levels",
        "--cores",
        "--catch-up-mib",
        "--idle-streams",
        "--seal-windows",
        "--subscribers",
        "--wal-mib",
        "--runs",
        "--timeout-seconds",
        "--remote-url",
        "--remote-ca",
        "--remote-token-file",
        "--stream-prefix",
        "--rss-command",
        "--connect-count",
        "--connect-concurrency",
        "--connections",
        "--record-bytes",
        "--streams",
        "--wal-root",
    ])?;
    if let Some(root) = args.one("--wal-root") {
        let _ = WAL_ROOT.set(PathBuf::from(root));
    }
    let bin = match args.one("--remote-url") {
        Some(url) => {
            set_remote(args, url, what)?;
            PathBuf::new()
        }
        None => server_bin(args)?,
    };
    // Dropping the future kills whichever server it holds, which a signal to this process
    // wouldn't. So this is what bounds a run.
    let limit = Duration::from_secs(args.number("--timeout-seconds")?.unwrap_or(24 * 3600));
    tokio::time::timeout(limit, run_one(args, what, &bin))
        .await
        .map_err(|_| format!("bench {what} ran out of time"))?
}

fn set_remote(args: &Args, url: &str, what: &str) -> Result<(), String> {
    if ![
        "latency",
        "throughput",
        "idle",
        "many-streams",
        "ack-matrix",
        "fan-out",
        "connect",
    ]
    .contains(&what)
    {
        return Err(format!(
            "bench {what} needs the server's files, so it can't run remotely"
        ));
    }
    let read = |flag: &str| -> Result<Vec<u8>, String> {
        let file = args
            .one(flag)
            .ok_or_else(|| format!("--remote-url needs {flag}"))?;
        std::fs::read(file).map_err(|error| format!("{file}: {error}"))
    };
    let token = String::from_utf8(read("--remote-token-file")?).map_err(|e| e.to_string())?;
    let remote = Remote {
        url: url.to_owned(),
        ca_pem: read("--remote-ca")?,
        token: token.trim().to_owned(),
        stream_prefix: args.one("--stream-prefix").unwrap_or("").to_owned(),
        rss_command: args.one("--rss-command").map(str::to_owned),
    };
    REMOTE
        .set(remote)
        .map_err(|_| "remote set twice".to_owned())
}

async fn run_one(args: &Args, what: &str, bin: &Path) -> Result<(), String> {
    let bin = bin.to_owned();
    match what {
        "latency" => latency(args, &bin).await,
        "throughput" => throughput(args, &bin).await,
        "idle" => idle(args, &bin).await,
        "catch-up" => catch_up(args, &bin).await,
        "many-streams" => many_streams(args, &bin).await,
        "puts" => puts_per_seal(args, &bin).await,
        "idle-streams" => idle_streams(args, &bin).await,
        "ack-matrix" => ack_matrix(args, &bin).await,
        "fan-out" => fan_out(args, &bin).await,
        "recovery" => recovery(args, &bin).await,
        "connect" => connect_rate(args, &bin).await,
        "all" => {
            latency(args, &bin).await?;
            throughput(args, &bin).await?;
            idle(args, &bin).await?;
            catch_up(args, &bin).await?;
            many_streams(args, &bin).await?;
            puts_per_seal(args, &bin).await?;
            idle_streams(args, &bin).await?;
            ack_matrix(args, &bin).await?;
            fan_out(args, &bin).await?;
            connect_rate(args, &bin).await?;
            recovery(args, &bin).await
        }
        other => Err(format!("bench {other}?")),
    }
}
