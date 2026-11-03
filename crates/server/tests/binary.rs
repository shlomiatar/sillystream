//! The server binary as an operator runs it: it starts, says where, serves, and stops on SIGTERM.

mod api;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_sillystream-server");

fn write_files(dir: &std::path::Path, secret: &[u8]) -> Vec<String> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    std::fs::write(dir.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), cert.key_pair.serialize_pem()).unwrap();
    std::fs::write(dir.join("secret"), secret).unwrap();
    std::fs::write(dir.join("stream-key"), "0".repeat(64)).unwrap();
    let path = |name: &str| dir.join(name).to_str().unwrap().to_owned();
    [
        "--listen".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--wal-dir".to_owned(),
        path("wal"),
        "--data-dir".to_owned(),
        path("data"),
        "--cert".to_owned(),
        path("cert.pem"),
        "--key".to_owned(),
        path("key.pem"),
        "--token-secret-file".to_owned(),
        path("secret"),
        "--stream-key-file".to_owned(),
        path("stream-key"),
        "--object-store".to_owned(),
        format!("local:{}", path("objects")),
        "--cores".to_owned(),
        "1".to_owned(),
        "--wal-preallocate-bytes".to_owned(),
        "1048576".to_owned(),
    ]
    .to_vec()
}

#[test]
fn it_starts_says_where_and_stops_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(BIN)
        .args(write_files(dir.path(), &[7u8; 32]))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert!(line.starts_with("listening on 127.0.0.1:"), "{line}");
    let address = line.trim().rsplit(' ').next().unwrap();
    std::net::TcpStream::connect(address).unwrap();

    let killed = Command::new("kill")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(killed.success());
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{status}");
            break;
        }
        if started.elapsed() > Duration::from_secs(20) {
            let _ = child.kill();
            panic!("still running 20 s after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Every stream is stored under a hash made with it, so there is no starting without it
/// (section 13).
#[test]
fn it_refuses_to_start_without_a_stream_key() {
    let dir = tempfile::tempdir().unwrap();
    let args = write_files(dir.path(), &[7u8; 32]);
    let at = args
        .iter()
        .position(|arg| arg == "--stream-key-file")
        .unwrap();
    let mut without = args.clone();
    without.drain(at..at + 2);
    let output = Command::new(BIN).args(without).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--stream-key-file is required"), "{stderr}");

    for bad in ["00", &"g".repeat(64), &"é".repeat(32)] {
        std::fs::write(dir.path().join("stream-key"), bad).unwrap();
        let output = Command::new(BIN).args(&args).output().unwrap();
        assert!(!output.status.success(), "{bad}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("64 hex digits"), "{stderr}");
    }
}

#[test]
fn it_refuses_a_short_secret() {
    let dir = tempfile::tempdir().unwrap();
    let short = Command::new(BIN)
        .args(write_files(dir.path(), b"short"))
        .output()
        .unwrap();
    assert!(!short.status.success());
}

/// Kills the server this test started, whichever way the test ends.
struct RunningServer(std::process::Child);

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Plain sockets with no token take every descriptor across several seals. When they go, so
/// does the trouble: the WAL writer waits rather than stopping for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_out_of_descriptors_at_a_seal_does_not_stop_writes_for_good() {
    let dir = tempfile::tempdir().unwrap();
    let mut args = write_files(dir.path(), &[7u8; 32]);
    let cert = api::test_cert();
    std::fs::write(dir.path().join("cert.pem"), &cert.cert_pem).unwrap();
    std::fs::write(dir.path().join("key.pem"), &cert.key_pem).unwrap();
    args.extend(["--seal-window-ms".to_owned(), "1000".to_owned()]);
    let stderr_path = dir.path().join("stderr");
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg("ulimit -n 64 && exec \"$0\" \"$@\"")
        .arg(BIN)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let _running = RunningServer(child);
    assert!(line.starts_with("listening on 127.0.0.1:"), "{line}");
    let address = line.trim().rsplit(' ').next().unwrap().to_owned();

    let token = auth::Token {
        expiry_ms: u64::MAX,
        author: std::num::NonZeroU64::new(1).unwrap(),
        grants: vec![auth::Grant {
            pattern: "*".to_owned(),
            perms: api::ALL,
        }],
    }
    .sign(&[7u8; 32])
    .unwrap();
    let client = api::tls_client_config(&cert.der, &[&rustls::version::TLS13]);
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    let publish = || async {
        let request =
            api::Http::new("POST", "/v1/stream/a/publish?ack=2", &token).body(b"x".to_vec());
        let attempt = async {
            let socket = tokio::net::TcpStream::connect(&address).await.ok()?;
            let mut tls = connector
                .connect("localhost".try_into().unwrap(), socket)
                .await
                .ok()?;
            tokio::io::AsyncWriteExt::write_all(&mut tls, &request.bytes())
                .await
                .ok()?;
            Some(api::read_http_reply(&mut tls).await)
        };
        tokio::time::timeout(Duration::from_secs(3), attempt)
            .await
            .ok()
            .flatten()
    };
    let before = publish().await.expect("a publish before the flood");
    assert_eq!(before.status, 200, "{}", before.text());

    let mut held = Vec::new();
    for _ in 0..150 {
        match tokio::net::TcpStream::connect(&address).await {
            Ok(socket) => held.push(socket),
            Err(_) => break,
        }
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    drop(held);

    let started = Instant::now();
    let last = loop {
        let reply = publish().await;
        if reply.as_ref().is_some_and(|reply| reply.status == 200) {
            break reply;
        }
        if started.elapsed() > Duration::from_secs(20) {
            break reply;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let stderr = std::fs::read_to_string(&stderr_path).unwrap();
    assert!(
        last.as_ref().is_some_and(|reply| reply.status == 200),
        "writes never came back: {:?}\n{stderr}",
        last.map(|reply| reply.text())
    );
    assert!(!stderr.contains("the WAL writer stopped"), "{stderr}");
    assert!(
        stderr.contains("can't open a new file"),
        "the flood never reached a rotation:\n{stderr}"
    );
}

/// A clean stop seals, uploads and checkpoints before it exits, so the next start has nothing in
/// the WAL to replay (section 8.1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_stop_leaves_nothing_to_replay() {
    let dir = tempfile::tempdir().unwrap();
    let cert = api::test_cert();
    let start = |stderr: &str| {
        let args = write_files(dir.path(), &[7u8; 32]);
        std::fs::write(dir.path().join("cert.pem"), &cert.cert_pem).unwrap();
        std::fs::write(dir.path().join("key.pem"), &cert.key_pem).unwrap();
        let mut child = Command::new(BIN)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(dir.path().join(stderr)).unwrap())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert!(line.starts_with("listening on 127.0.0.1:"), "{line}");
        let address = line.trim().rsplit(' ').next().unwrap().to_owned();
        (child, address)
    };
    let (mut child, address) = start("first.stderr");

    let token = auth::Token {
        expiry_ms: u64::MAX,
        author: std::num::NonZeroU64::new(1).unwrap(),
        grants: vec![auth::Grant {
            pattern: "*".to_owned(),
            perms: api::ALL,
        }],
    }
    .sign(&[7u8; 32])
    .unwrap();
    let client = api::tls_client_config(&cert.der, &[&rustls::version::TLS13]);
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    let socket = tokio::net::TcpStream::connect(&address).await.unwrap();
    let mut tls = connector
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    let request = api::Http::new("POST", "/v1/stream/a/publish?ack=2", &token).body(b"x".to_vec());
    tokio::io::AsyncWriteExt::write_all(&mut tls, &request.bytes())
        .await
        .unwrap();
    let reply = api::read_http_reply(&mut tls).await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    drop(tls);

    let killed = Command::new("kill")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(killed.success());
    let status = child.wait().unwrap();
    let stderr = std::fs::read_to_string(dir.path().join("first.stderr")).unwrap();
    assert!(status.success(), "{status}\n{stderr}");
    assert!(!stderr.contains("didn't finish"), "{stderr}");

    let (child, _) = start("second.stderr");
    let _running = RunningServer(child);
    let stderr = std::fs::read_to_string(dir.path().join("second.stderr")).unwrap();
    assert!(stderr.contains(", 0 WAL entries replayed,"), "{stderr}");
}

/// Section 7.1's range is refused outside it, not quietly moved into it.
#[test]
fn a_seal_window_out_of_range_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut args = write_files(dir.path(), &[7u8; 32]);
    let cert = api::test_cert();
    std::fs::write(dir.path().join("cert.pem"), &cert.cert_pem).unwrap();
    std::fs::write(dir.path().join("key.pem"), &cert.key_pem).unwrap();
    args.extend(["--seal-window-ms".to_owned(), "999".to_owned()]);
    let output = Command::new(BIN).args(args).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--seal-window-ms"), "{stderr}");
}

#[test]
fn a_config_that_cant_work_is_refused() {
    let mut config = server::NodeConfig::new(1);
    config.validate().unwrap();
    config.catch_up_read_bytes = 0;
    assert_eq!(
        config.validate().unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}
