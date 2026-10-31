//! The crash matrix (SPEC section 8.1): the real binary, killed by `abort` at each named point,
//! then restarted. Every acked record must be there once, unacked ones must be safe to retry,
//! and new ids must be above all earlier ones. `abort` proves a process crash only: what the
//! kernel holds survives it, so this says nothing about power loss.

mod api;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use api::{ALL, AUTHOR, Http, SECRET, parse_records, read_http_reply, tls_client_config, token_at};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const BIN: &str = env!("CARGO_BIN_EXE_sillystream-server");
const STREAM: &str = "crash.stream";
const PRODUCER: &str = "7";
const GIVE_UP: Duration = Duration::from_secs(90);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
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
        let cert = api::test_cert();
        std::fs::write(dir.path().join("cert.pem"), &cert.cert_pem).unwrap();
        std::fs::write(dir.path().join("key.pem"), &cert.key_pem).unwrap();
        std::fs::write(dir.path().join("secret"), SECRET).unwrap();
        Files { dir, der: cert.der }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn command(&self, stderr: &Path) -> Command {
        let mut command = Command::new(BIN);
        let path = |name: &str| self.path(name).to_str().unwrap().to_owned();
        command
            .args(["--listen", "127.0.0.1:0"])
            .args(["--wal-dir", &path("wal")])
            .args(["--data-dir", &path("data")])
            .args(["--cert", &path("cert.pem")])
            .args(["--key", &path("key.pem")])
            .args(["--token-secret-file", &path("secret")])
            .args(["--object-store", &format!("local:{}", path("objects"))])
            .args(["--cores", "2"])
            .args(["--seal-window-ms", "1000"])
            .args(["--wal-preallocate-bytes", "1048576"])
            .stdout(Stdio::piped())
            .stderr(File::create(stderr).unwrap());
        command
    }
}

/// Killed if the test ends early, so no process outlives it.
struct Running {
    child: Child,
    port: Option<u16>,
    stderr: PathBuf,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(files: &Files, name: &str, crash_at: Option<&str>) -> Running {
    let stderr = files.path(&format!("{name}.stderr"));
    let mut command = files.command(&stderr);
    if let Some(point) = crash_at {
        command.env("SILLYSTREAM_CRASH_AT", point);
    }
    let mut child = command.spawn().unwrap();
    let mut line = String::new();
    let out = child.stdout.take().unwrap();
    // EOF, with nothing said, if a crash point fired before it listened.
    BufReader::new(out).read_line(&mut line).unwrap();
    let port = line
        .trim()
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok());
    Running {
        child,
        port,
        stderr,
    }
}

struct Producer {
    connector: TlsConnector,
    token: String,
    next_seq: u64,
    /// `producer_seq -> first_id`.
    acked: BTreeMap<u64, u64>,
    /// Sent with no answer: may or may not be written.
    unknown: Vec<u64>,
}

impl Producer {
    fn new(files: &Files) -> Producer {
        let config = tls_client_config(&files.der, &[&rustls::version::TLS13]);
        Producer {
            connector: TlsConnector::from(Arc::new(config)),
            token: token_at(now_ms() + 3_600_000, AUTHOR, &[("*", ALL)]),
            next_seq: 1,
            acked: BTreeMap::new(),
            unknown: Vec::new(),
        }
    }

    async fn request(&self, port: u16, request: &Http<'_>) -> Option<api::HttpReply> {
        let exchange = async {
            let socket = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
            let mut tls = self
                .connector
                .connect("localhost".try_into().unwrap(), socket)
                .await
                .ok()?;
            tls.write_all(&request.bytes()).await.ok()?;
            tls.flush().await.ok()?;
            // A dying server ends this as a short read, which the helper turns into a panic.
            let read = tokio::spawn(async move { read_http_reply(&mut tls).await });
            read.await.ok()
        };
        tokio::time::timeout(Duration::from_secs(20), exchange)
            .await
            .ok()
            .flatten()
    }

    /// `Some(first_id)` if acked at level 2.
    async fn publish(&self, port: u16, seq: u64) -> Option<u64> {
        let path = format!("/v1/stream/{STREAM}/publish?ack=2");
        let seq_text = seq.to_string();
        let body = format!("record-{seq}").into_bytes();
        let request = Http::new("POST", &path, &self.token)
            .header("X-Producer-Id", PRODUCER)
            .header("X-Producer-Seq", &seq_text)
            .body(body);
        let reply = self.request(port, &request).await?;
        assert_eq!(reply.status, 200, "{}", reply.text());
        reply.field("first_id")?.parse().ok()
    }

    async fn publish_next(&mut self, port: u16) -> bool {
        let seq = self.next_seq;
        self.next_seq += 1;
        match self.publish(port, seq).await {
            Some(first_id) => {
                self.acked.insert(seq, first_id);
                true
            }
            None => {
                self.unknown.push(seq);
                false
            }
        }
    }

    /// `(id, data)` from id 0 on, page by page.
    async fn read_everything(&self, port: u16) -> Vec<(u64, Vec<u8>)> {
        let mut out = Vec::new();
        let mut from = 0;
        loop {
            let path = format!("/v1/stream/{STREAM}/read?from={from}");
            let reply = self
                .request(port, &Http::new("GET", &path, &self.token))
                .await
                .expect("a read of a live server");
            assert_eq!(reply.status, 200, "{}", reply.text());
            let page = parse_records(&reply.body);
            let Some(last) = page.last() else {
                return out;
            };
            from = last.0 + 1;
            out.extend(page.into_iter().map(|(id, _, _, data)| (id, data)));
        }
    }
}

fn wait_for_exit(running: &mut Running) -> std::process::ExitStatus {
    let started = Instant::now();
    loop {
        if let Some(status) = running.child.try_wait().unwrap() {
            return status;
        }
        assert!(started.elapsed() < Duration::from_secs(30), "still running");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// What must hold after any crash, at any moment.
async fn check(producer: &Producer, port: u16) -> u64 {
    let records = producer.read_everything(port).await;
    let ids: Vec<u64> = records.iter().map(|(id, _)| *id).collect();
    assert!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "ids out of order or repeated: {ids:?}"
    );
    let mut seen = BTreeMap::new();
    for (id, data) in &records {
        assert!(
            seen.insert(data.clone(), *id).is_none(),
            "{} is there twice",
            String::from_utf8_lossy(data)
        );
    }
    for (seq, first_id) in &producer.acked {
        let data = format!("record-{seq}").into_bytes();
        assert_eq!(seen.get(&data), Some(first_id), "acked seq {seq} was lost");
    }
    ids.last().copied().unwrap_or(0)
}

async fn run_matrix_case(point: &str) {
    let files = Files::new();
    let mut producer = Producer::new(&files);

    // Something to lose, from an earlier life that also ended without warning.
    let mut first = start(&files, "first", None);
    let port = first.port.expect("the first start listens");
    for _ in 0..20 {
        assert!(producer.publish_next(port).await);
    }
    // Long enough for a seal, a checkpoint, and so for files to recycle later.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for _ in 0..5 {
        assert!(producer.publish_next(port).await);
    }
    let _ = first.child.kill();
    let _ = first.child.wait();
    drop(first);

    let mut doomed = start(&files, "doomed", Some(point));
    let started = Instant::now();
    if let Some(port) = doomed.port {
        loop {
            if !producer.publish_next(port).await {
                break;
            }
            if doomed.child.try_wait().unwrap().is_some() {
                break;
            }
            assert!(
                started.elapsed() < GIVE_UP,
                "the crash point {point} never fired"
            );
        }
    }
    let status = wait_for_exit(&mut doomed);
    // The crash point itself, not some other way of dying.
    assert_eq!(status.signal(), Some(6), "{point}: {status}");
    drop(doomed);

    let survivor = start(&files, "survivor", None);
    let port = survivor.port.expect("a restart after a crash listens");
    let mut last = check(&producer, port).await;

    // Retries of whatever got no answer: its first ack if it had been written, else a new one.
    for seq in std::mem::take(&mut producer.unknown) {
        let first_id = producer.publish(port, seq).await.expect("a retry is acked");
        producer.acked.insert(seq, first_id);
    }
    last = last.max(check(&producer, port).await);
    // Retrying what was acked changes nothing and returns the same ack, for the latest.
    let (latest_seq, latest_id) = producer
        .acked
        .iter()
        .next_back()
        .map(|(s, i)| (*s, *i))
        .unwrap();
    assert_eq!(producer.publish(port, latest_seq).await, Some(latest_id));

    assert!(producer.publish_next(port).await);
    let newest = *producer.acked.values().next_back().unwrap();
    assert!(
        newest > last,
        "{point}: a new id {newest} isn't above {last}"
    );
    check(&producer, port).await;

    // And over one more restart, with the crash long past.
    drop(survivor);
    let again = start(&files, "again", None);
    let port = again.port.unwrap();
    check(&producer, port).await;
    let text = std::fs::read_to_string(&again.stderr).unwrap();
    assert!(!text.contains("warning"), "{text}");
}

macro_rules! matrix {
    ($($name:ident => $point:literal,)*) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                run_matrix_case($point).await;
            }
        )*
    };
}

matrix! {
    crash_before_the_wal_write => "wal.before_write:3",
    crash_before_the_fsync => "wal.before_fsync:3",
    crash_after_the_fsync_before_the_ack => "wal.after_fsync:3",
    crash_during_a_seal_after_rotation => "seal.after_rotate",
    crash_during_a_seal_before_the_upload => "seal.before_put",
    crash_after_the_upload_before_the_checkpoint => "seal.after_upload",
    crash_after_the_local_write_before_the_checkpoint => "checkpoint.before_put",
    crash_after_the_checkpoint_before_recycling => "checkpoint.after_put",
    crash_during_recycling_before_emptying => "recycle.before_empty",
    crash_during_recycling_after_emptying => "recycle.after_empty",
    crash_during_recycling_after_renaming => "recycle.after_rename",
    crash_before_the_ownership_write => "ownership.before_write",
    crash_after_the_ownership_write => "ownership.after_write",
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_warns_of_an_empty_wal_only_when_a_generation_exists() {
    let files = Files::new();
    let first = start(&files, "new", None);
    assert!(first.port.is_some());
    let text = std::fs::read_to_string(&first.stderr).unwrap();
    assert!(!text.contains("wal_dir is empty"), "{text}");
    drop(first);

    std::fs::remove_dir_all(files.path("wal")).unwrap();
    let second = start(&files, "lost", None);
    assert!(second.port.is_some());
    let text = std::fs::read_to_string(&second.stderr).unwrap();
    assert!(text.contains("wal_dir is empty"), "{text}");
}

#[test]
fn of_two_binaries_on_one_set_of_disks_one_stays_up() {
    let files = Files::new();
    let mut a = start(&files, "a", None);
    let mut b = start(&files, "b", None);
    // `start` returned for each: it listens or it has gone.
    assert_ne!(a.port.is_some(), b.port.is_some());
    let (loser, name) = if a.port.is_some() {
        (&mut b, "b")
    } else {
        (&mut a, "a")
    };
    assert!(!wait_for_exit(loser).success());
    let text = std::fs::read_to_string(files.path(&format!("{name}.stderr"))).unwrap();
    // One set of disks: it can't have the local store, and never reaches the ownership record.
    assert!(
        text.contains("sillystream-server: metadata store:"),
        "{text}"
    );
}

/// The race itself, on real processes: separate disks and one bucket, as machines would have.
/// Those that read after a claim go on with the next generation (section 8.1, Q31), so several
/// may stay up. What must hold is one process per generation, and that any that stopped lost a
/// conditional write.
#[test]
fn of_binaries_racing_on_one_bucket_each_generation_has_one_winner() {
    const PROCESS_COUNT: usize = 6;
    let mut lost_count = 0;
    for round in 0..10 {
        let files = Files::new();
        let mut children: Vec<(Child, PathBuf)> = Vec::new();
        let mut running: Vec<Running> = Vec::new();
        for at in 0..PROCESS_COUNT {
            let stderr = files.path(&format!("{at}.stderr"));
            let mut command = files.command(&stderr);
            let path = |name: String| files.path(&name).to_str().unwrap().to_owned();
            // The parser takes the last of a repeated option.
            command
                .args(["--wal-dir", &path(format!("wal.{at}"))])
                .args(["--data-dir", &path(format!("data.{at}"))]);
            children.push((command.spawn().unwrap(), stderr));
        }
        let mut stopped = Vec::new();
        for (mut child, stderr) in children {
            let mut line = String::new();
            let out = child.stdout.take().unwrap();
            BufReader::new(out).read_line(&mut line).unwrap();
            let port = line.trim().rsplit(':').next().and_then(|p| p.parse().ok());
            if port.is_none() {
                stopped.push(stderr.clone());
            }
            running.push(Running {
                child,
                port,
                stderr,
            });
        }
        let listening = running.iter().filter(|one| one.port.is_some()).count();
        let generations = std::fs::read_dir(files.path("objects/ownership"))
            .unwrap()
            .count();
        assert!(listening >= 1, "round {round}");
        assert_eq!(listening, generations, "round {round}");
        for stderr in &stopped {
            let text = std::fs::read_to_string(stderr).unwrap();
            assert!(text.contains("another process took generation"), "{text}");
        }
        lost_count += stopped.len();
        drop(running);
        if lost_count > 0 {
            return;
        }
    }
    panic!("no process lost the race in 10 rounds of {PROCESS_COUNT}");
}
