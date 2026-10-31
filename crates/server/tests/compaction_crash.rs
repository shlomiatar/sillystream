//! The real binary, killed by `abort` at each step of compaction and expiry, then restarted.
//! Whatever step it died at, every acked record is there once. `abort` proves a process crash
//! only, as in `crash_matrix.rs`. Real S3 and EBS aren't exercised.

mod api;

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use api::{ALL, AUTHOR, Http, SECRET, parse_records, read_http_reply, tls_client_config, token_at};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use storage::SegmentReader;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const BIN: &str = env!("CARGO_BIN_EXE_sillystream-server");
const STREAM: &str = "keep.week";
const THREE_DAYS_MS: u64 = 72 * 3_600_000;

struct Files {
    dir: tempfile::TempDir,
    connector: TlsConnector,
    token: String,
}

impl Files {
    fn new() -> Files {
        let dir = tempfile::tempdir().unwrap();
        let cert = api::test_cert();
        std::fs::write(dir.path().join("cert.pem"), &cert.cert_pem).unwrap();
        std::fs::write(dir.path().join("key.pem"), &cert.key_pem).unwrap();
        std::fs::write(dir.path().join("secret"), SECRET).unwrap();
        let config = tls_client_config(&cert.der, &[&rustls::version::TLS13]);
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Files {
            dir,
            connector: TlsConnector::from(Arc::new(config)),
            token: token_at(now_ms + THREE_DAYS_MS * 4, AUTHOR, &[("*", ALL)]),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn objects(&self, root: &str) -> BTreeSet<PathBuf> {
        fn walk(dir: &std::path::Path, out: &mut BTreeSet<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    walk(&entry.path(), out);
                } else {
                    out.insert(entry.path());
                }
            }
        }
        let mut out = BTreeSet::new();
        walk(&self.path("objects").join(root), &mut out);
        out
    }
}

/// A failing test shows what each server said, and what the store holds.
impl Drop for Files {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for name in ["first", "second", "third"] {
                let said = std::fs::read_to_string(self.path(&format!("{name}.stderr")));
                eprintln!("--- {name}:\n{}", said.unwrap_or_default());
            }
            for root in ["sealed", "7d"] {
                eprintln!("--- {root}: {:?}", self.objects(root));
            }
        }
    }
}

/// Killed if the test ends early, so no process outlives it.
struct Running {
    child: Child,
    port: u16,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(files: &Files, name: &str, crash_at: Option<&str>, offset_ms: u64) -> Running {
    let path = |name: &str| files.path(name).to_str().unwrap().to_owned();
    let mut command = Command::new(BIN);
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
        .args(["--compact-period-ms", "200"])
        .args(["--compact-merge-wait-ms", "0"])
        .args(["--expire-period-ms", "200"])
        .env("SILLYSTREAM_CLOCK_OFFSET_MS", offset_ms.to_string())
        .stdout(Stdio::piped())
        .stderr(File::create(files.path(&format!("{name}.stderr"))).unwrap());
    if let Some(point) = crash_at {
        command.env("SILLYSTREAM_CRASH_AT", point);
    }
    let mut child = command.spawn().unwrap();
    let mut line = String::new();
    let out = child.stdout.take().unwrap();
    BufReader::new(out).read_line(&mut line).unwrap();
    let port = line.trim().rsplit(':').next().unwrap().parse().unwrap();
    Running { child, port }
}

impl Files {
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
            // A dying server ends this as a short read, which the helper turns into a panic,
            // so it gets a task of its own.
            let read = tokio::spawn(async move { read_http_reply(&mut tls).await });
            read.await.ok()
        };
        tokio::time::timeout(Duration::from_secs(20), exchange)
            .await
            .ok()
            .flatten()
    }

    async fn publish(&self, port: u16, stream: &str, seq: u64) -> Option<u64> {
        let path = format!("/v1/stream/{stream}/publish?ack=2");
        let seq_text = seq.to_string();
        let request = Http::new("POST", &path, &self.token)
            .header("X-Producer-Id", "7")
            .header("X-Producer-Seq", &seq_text)
            .body(format!("record-{seq}").into_bytes());
        let reply = self.request(port, &request).await?;
        assert_eq!(reply.status, 200, "{}", reply.text());
        reply.field("first_id")?.parse().ok()
    }

    async fn read_everything(&self, port: u16, stream: &str) -> Vec<(u64, Vec<u8>)> {
        let mut out = Vec::new();
        let mut from = 0;
        loop {
            let path = format!("/v1/stream/{stream}/read?from={from}");
            let reply = self
                .request(port, &Http::new("GET", &path, &self.token))
                .await
                .expect("a read of a live server");
            assert_eq!(reply.status, 200, "{}", reply.text());
            let page = parse_records(&reply.body);
            let Some(last) = page.last() else {
                return out;
            };
            from = last.0;
            out.extend(page.into_iter().map(|(id, _, _, data)| (id, data)));
        }
    }

    async fn set_class(&self, port: u16, stream: &str, retention: &str) {
        let path = format!("/v1/stream/{stream}");
        let body = format!("{{\"retention\":\"{retention}\"}}").into_bytes();
        let reply = self
            .request(port, &Http::new("PUT", &path, &self.token).body(body))
            .await
            .unwrap();
        assert_eq!(reply.status, 200, "{}", reply.text());
    }

    /// Whether every acked record is in an object under `7d/`, which needs each manifest read.
    async fn all_compacted(&self, stream: &str, acked: &[(u64, u64)]) -> bool {
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(self.path("objects")).unwrap());
        let mut ranges = Vec::new();
        for file in self.objects("7d") {
            let relative = file.strip_prefix(self.path("objects")).unwrap();
            let path = Path::from(relative.to_str().unwrap());
            let reader = SegmentReader::new(Arc::clone(&store), path);
            // One a crash left torn is nobody's.
            let Ok(top) = reader.read_top_index().await else {
                continue;
            };
            let Ok(extents) = reader.all_extents(&top).await else {
                continue;
            };
            ranges.extend(
                extents
                    .iter()
                    .filter(|extent| &*extent.stream == stream)
                    .map(|extent| (extent.id_first, extent.id_last)),
            );
        }
        acked
            .iter()
            .all(|(_, id)| ranges.iter().any(|(first, last)| first <= id && id <= last))
    }

    /// Every acked record once, in order, with nothing else. Unacked ones may be there too.
    async fn check(&self, port: u16, stream: &str, acked: &[(u64, u64)]) {
        let got = self.read_everything(port, stream).await;
        let ids: Vec<u64> = got.iter().map(|record| record.0).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(ids, sorted, "out of order or doubled");
        for (seq, id) in acked {
            let at = got
                .iter()
                .find(|record| record.0 == *id)
                .unwrap_or_else(|| panic!("record {seq} ({id}) is missing"));
            assert_eq!(at.1, format!("record-{seq}").into_bytes());
        }
    }
}

fn wait_for_abort(running: &mut Running) {
    let started = Instant::now();
    loop {
        if let Some(status) = running.child.try_wait().unwrap() {
            assert_eq!(status.signal(), Some(libc_sigabrt()), "{status:?}");
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the crash point didn't fire"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn libc_sigabrt() -> i32 {
    6
}

async fn wait_until(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let started = Instant::now();
    while !done().await {
        assert!(started.elapsed() < Duration::from_secs(60), "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A crash at `point` while records are compacted, a restart, and the job finishes.
async fn crash_while_compacting(point: &str) {
    let files = Files::new();
    let mut acked = Vec::new();
    let mut running = start(&files, "first", Some(point), 0);
    files.set_class(running.port, STREAM, "7d").await;
    let mut seq = 0;
    // Publishes until the point fires, which a seal and a compaction period bring about.
    let started = Instant::now();
    loop {
        if running.child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{point} didn't fire"
        );
        seq += 1;
        match files.publish(running.port, STREAM, seq).await {
            Some(id) => acked.push((seq, id)),
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    wait_for_abort(&mut running);
    drop(running);

    let running = start(&files, "second", None, 0);
    files.check(running.port, STREAM, &acked).await;
    seq += 1;
    let id = files.publish(running.port, STREAM, seq).await.unwrap();
    acked.push((seq, id));
    // Everything arrives in `7d` and is still there once, however many outputs the crash left
    // unreferenced. The record just published can only get there by a worker running again.
    wait_until("compaction didn't finish after the restart", async || {
        files.all_compacted(STREAM, &acked).await
    })
    .await;
    files.check(running.port, STREAM, &acked).await;
    let reply = files
        .request(
            running.port,
            &Http::new(
                "GET",
                &format!("/v1/stream/{}", server::MANIFEST_STREAM),
                &files.token,
            ),
        )
        .await
        .unwrap();
    assert_eq!(reply.status, 200, "{}", reply.text());
    drop(running);

    // Once more on the same disks, all of it from the store.
    let running = start(&files, "third", None, 0);
    files.check(running.port, STREAM, &acked).await;
}

// These share a target directory's binary but no state: each has its own files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_before_a_manifest_is_published_loses_no_compaction() {
    // The second: a crash before the first seal saved anything also loses the class (D143).
    crash_while_compacting("manifest.before_publish:2").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_the_output_before_any_extent_changes() {
    crash_while_compacting("compact.after_output").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_extents_change_before_a_checkpoint() {
    crash_while_compacting("compact.after_extents").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_the_checkpoint_before_the_cursor() {
    crash_while_compacting("compact.before_cursor").await;
}

/// With the clock moved on, the binary expires `sealed/` once compaction has passed it. A crash
/// between the checkpoint and the deletes costs nothing, and `1d` data really goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiry_on_the_real_binary_survives_a_crash_before_its_deletes() {
    let files = Files::new();
    let running = start(&files, "first", None, 0);
    files.set_class(running.port, STREAM, "7d").await;
    let mut acked = Vec::new();
    for seq in 1..=5 {
        let id = files.publish(running.port, STREAM, seq).await.unwrap();
        acked.push((seq, id));
        files.publish(running.port, "keep.day", seq).await.unwrap();
    }
    wait_until("compaction didn't run", async || {
        !files.objects("7d").is_empty()
    })
    .await;
    let sealed_before = files.objects("sealed");
    drop(running);

    let mut running = start(
        &files,
        "second",
        Some("expire.before_delete"),
        THREE_DAYS_MS,
    );
    wait_for_abort(&mut running);
    assert!(files.objects("sealed").is_superset(&sealed_before));
    drop(running);

    let running = start(&files, "third", None, THREE_DAYS_MS);
    wait_until("sealed/ didn't expire", async || {
        files.objects("sealed").is_disjoint(&sealed_before)
    })
    .await;
    files.check(running.port, STREAM, &acked).await;
    assert_eq!(
        files.read_everything(running.port, "keep.day").await,
        vec![]
    );
    assert!(!files.objects("7d").is_empty());
}
