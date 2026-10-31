//! The load generator: the real server binary, over real TLS and WebSocket, through the SDK.
//! It prints Markdown tables for docs/BENCH.md.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sdk::{
    AckLevel, AckWait, BatchConfig, Client, ClientConfig, Event, FROM_LIVE, PublisherConfig,
    SdkError, Subscribe, build_records,
};
use tokio::sync::Semaphore;

use crate::Args;

const SEAL_WINDOW_MS: u64 = 1_000;

/// The server as a child process in a temporary directory, killed when this goes.
struct ServerChild {
    child: Child,
    bin: PathBuf,
    settings: ServerSettings,
    url: String,
    ca_pem: Vec<u8>,
    token: String,
    dir: tempfile::TempDir,
}

impl Drop for ServerChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone, Copy)]
struct ServerSettings {
    cores: u64,
    seal_window_ms: u64,
    active_stream_limit: Option<u64>,
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
        },
    )
}

fn start_server_with(bin: &Path, settings: ServerSettings) -> Result<ServerChild, String> {
    let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .map_err(|error| error.to_string())?;
    let ca_pem = certified.cert.pem().into_bytes();
    let write = |name: &str, bytes: &[u8]| -> Result<(), String> {
        std::fs::write(dir.path().join(name), bytes).map_err(|error| error.to_string())
    };
    // As text: the server trims whitespace from the end of its secret file, and raw bytes end
    // in whitespace often enough to fail a run with a wrong signature.
    let secret: Vec<u8> = {
        use std::hash::{BuildHasher, Hasher};
        (0..8)
            .map(|_| {
                let word = std::collections::hash_map::RandomState::new()
                    .build_hasher()
                    .finish();
                format!("{word:016x}")
            })
            .collect::<String>()
            .into_bytes()
    };
    write("cert.pem", &ca_pem)?;
    write("key.pem", certified.key_pair.serialize_pem().as_bytes())?;
    write("secret", &secret)?;
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
        child,
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
        .args(["--wal-dir", &path("wal"), "--data-dir", &path("data")])
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
        let pid = self.child.id().to_string();
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .map_err(text)?;
        let status = self.child.wait().map_err(text)?;
        if !status.success() {
            return Err(format!("the server stopped with {status}"));
        }
        if clear_cache {
            std::fs::remove_dir_all(self.dir.path().join("data/cache")).map_err(text)?;
        }
        let (child, port) = spawn_server(&self.bin, self.settings, self.dir.path())?;
        self.child = child;
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
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &self.child.id().to_string()])
            .output()
            .map_err(|error| error.to_string())?;
        let text = String::from_utf8_lossy(&output.stdout);
        let kib: u64 = text
            .trim()
            .parse()
            .map_err(|_| format!("ps said {text:?}"))?;
        Ok(kib * 1024)
    }
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
    let stream = format!("latency.{}", ack as u8);
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

/// As fast as the window allows, for `seconds`, on `connection_count` sockets to one stream.
async fn throughput_run(
    server: &ServerChild,
    ack: AckLevel,
    connection_count: u64,
    data_bytes: usize,
    seconds: u64,
    stream: &str,
) -> Result<(u64, f64), String> {
    let body = Arc::new(record(data_bytes)?);
    let acked = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..connection_count {
        let client = server.client().await?;
        let mut publisher = client
            .publisher(
                stream,
                PublisherConfig {
                    ack,
                    max_inflight_bytes: (8 * 1024 * 1024).max(data_bytes * 16),
                    ..PublisherConfig::default()
                },
            )
            .map_err(|error| error.to_string())?;
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
    println!("### Throughput, one stream, {cores} cores, {seconds} s each\n");
    println!("| ack | connections | record data bytes | publishes/s | MB/s of data |");
    println!("|---|---|---|---|---|");
    let mut run = 0;
    for (ack, name) in [(AckLevel::Memory, "1"), (AckLevel::WalFsync, "2")] {
        for connection_count in [1u64, 4] {
            for data_bytes in [1024usize, 64 * 1024] {
                // A server per row, so that one row's memory cap doesn't slow the next.
                let server = start_server(bin, cores)?;
                run += 1;
                let stream = format!("throughput.{run}");
                let (acked, elapsed) =
                    throughput_run(&server, ack, connection_count, data_bytes, seconds, &stream)
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
        .publisher("warm", PublisherConfig::default())
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
                    throughput_run(&server, ack, 1, data_bytes, seconds, &stream).await
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
    ])?;
    let bin = server_bin(args)?;
    match what {
        "latency" => latency(args, &bin).await,
        "throughput" => throughput(args, &bin).await,
        "idle" => idle(args, &bin).await,
        "catch-up" => catch_up(args, &bin).await,
        "many-streams" => many_streams(args, &bin).await,
        "puts" => puts_per_seal(args, &bin).await,
        "idle-streams" => idle_streams(args, &bin).await,
        "all" => {
            latency(args, &bin).await?;
            throughput(args, &bin).await?;
            idle(args, &bin).await?;
            catch_up(args, &bin).await?;
            many_streams(args, &bin).await?;
            puts_per_seal(args, &bin).await?;
            idle_streams(args, &bin).await
        }
        other => Err(format!("bench {other}?")),
    }
}
