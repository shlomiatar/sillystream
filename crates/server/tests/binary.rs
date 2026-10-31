//! The server binary as an operator runs it: it starts, says where, serves, and stops on SIGTERM.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_sillystream-server");

fn write_files(dir: &std::path::Path, secret: &[u8]) -> Vec<String> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    std::fs::write(dir.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), cert.key_pair.serialize_pem()).unwrap();
    std::fs::write(dir.join("secret"), secret).unwrap();
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

#[test]
fn it_refuses_a_short_secret() {
    let dir = tempfile::tempdir().unwrap();
    let short = Command::new(BIN)
        .args(write_files(dir.path(), b"short"))
        .output()
        .unwrap();
    assert!(!short.status.success());
}
