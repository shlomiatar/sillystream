//! The binary as someone would run it, against a real server.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use server::net::{Server, ServerConfig};
use server::{NodeConfig, SystemClock};
use storage::DirVolume;

const BIN: &str = env!("CARGO_BIN_EXE_silly");
const SECRET: &[u8] = b"a test secret, thirty-two bytes or more";

struct Fixture {
    server: Server,
    dir: tempfile::TempDir,
    ca_file: PathBuf,
    secret_file: PathBuf,
    token: String,
}

fn start() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let ca_pem = cert.cert.pem().into_bytes();
    let ca_file = dir.path().join("ca.pem");
    std::fs::write(&ca_file, &ca_pem).unwrap();
    let secret_file = dir.path().join("secret");
    std::fs::write(&secret_file, SECRET).unwrap();
    let mut node = NodeConfig::new(1);
    node.seal_window_ms = None;
    node.rebalance_period_ms = None;
    let server = Server::start(
        ServerConfig {
            node,
            listen: "127.0.0.1:0".parse().unwrap(),
            cert_pem: ca_pem,
            key_pem: cert.key_pair.serialize_pem().into_bytes(),
            token_secret: SECRET.to_vec(),
        },
        DirVolume::new(dir.path(), 1024 * 1024),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(SystemClock),
    )
    .unwrap();
    let mut fixture = Fixture {
        server,
        dir,
        ca_file,
        secret_file,
        token: String::new(),
    };
    fixture.token = fixture.issue("*:read,publish,state,admin", "60000");
    fixture
}

impl Fixture {
    fn issue(&self, grant: &str, ttl_ms: &str) -> String {
        let output = Command::new(BIN)
            .args([
                "token", "issue", "--author", "5", "--grant", grant, "--ttl-ms", ttl_ms,
            ])
            .arg("--secret-file")
            .arg(&self.secret_file)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(BIN);
        command
            .args(args)
            .env(
                "SILLYSTREAM_URL",
                format!("localhost:{}", self.server.local_addr().port()),
            )
            .env("SILLYSTREAM_CA", &self.ca_file)
            .env("SILLYSTREAM_TOKEN", &self.token);
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    fn fails(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(!output.status.success(), "{args:?}: {output:?}");
        String::from_utf8(output.stderr).unwrap()
    }
}

#[test]
fn stream_admin() {
    let fixture = start();
    assert!(fixture.fails(&["stream", "info", "a"]).contains("404"));
    let created = fixture.ok(&[
        "stream",
        "create",
        "a",
        "--retention",
        "7d",
        "--min-ack",
        "1",
    ]);
    assert!(created.contains("\"retention\":\"7d\""), "{created}");
    let changed = fixture.ok(&["stream", "config", "a", "--default-ack", "1"]);
    assert!(changed.contains("\"default_ack\":1") || changed.contains("\"default_ack\":\"1\""));
    assert!(
        fixture
            .ok(&["stream", "info", "a"])
            .contains("\"stream\":\"a\"")
    );
    assert!(
        fixture
            .fails(&["stream", "config", "a", "--retention", "2d"])
            .contains("400")
    );
    assert!(
        fixture
            .fails(&["stream", "create", "a", "--typo", "x"])
            .contains("--typo")
    );
    fixture.ok(&["stream", "delete", "a"]);
    assert!(fixture.fails(&["stream", "info", "a"]).contains("404"));
    drop(fixture.server.shutdown());
}

#[test]
fn sink_admin() {
    let fixture = start();
    fixture.ok(&["stream", "create", "k"]);
    fixture.ok(&[
        "sink", "create", "k", "1", "--kind", "forward", "--target", "other",
    ]);
    assert!(
        fixture
            .ok(&["stream", "info", "k"])
            .contains("\"kind\":\"forward\"")
    );
    fixture.ok(&["sink", "pause", "k", "1"]);
    assert!(
        fixture
            .ok(&["stream", "info", "k"])
            .contains("\"paused\":true")
    );
    fixture.ok(&["sink", "resume", "k", "1"]);
    fixture.ok(&["sink", "reset", "k", "1", "--start", "earliest"]);
    fixture.ok(&["sink", "delete", "k", "1"]);
    assert!(fixture.fails(&["sink", "delete", "k", "1"]).contains("404"));
    fixture.ok(&[
        "sink",
        "create",
        "k",
        "3",
        "--kind",
        "webhook.notify",
        "--sink-url",
        "https://example.invalid/x",
        "--secret",
        "s",
        "--interval-ms",
        "500",
        "--budget-bytes",
        "1024",
    ]);
    assert!(
        !fixture.ok(&["stream", "info", "k"]).contains("\"secret\""),
        "a sink's secret is never shown"
    );
    fixture.ok(&["sink", "delete", "k", "3"]);
    assert!(
        fixture
            .fails(&["sink", "create", "k", "2", "--kind", "nope"])
            .contains("400")
    );
    drop(fixture.server.shutdown());
}

#[test]
fn publish_and_tail_round_trip() {
    let fixture = start();
    let acked = fixture.ok(&["publish", "p", "--options", "0x10", "one", "two"]);
    assert!(acked.contains("count=2"), "{acked}");
    let mut child = fixture
        .command(&["publish", "p", "--options", "2", "--ack", "1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"three\nfour\n")
        .unwrap();
    assert!(child.wait_with_output().unwrap().status.success());

    let tailed = fixture.ok(&["tail", "p", "--from", "start", "--limit", "4"]);
    let lines: Vec<&str> = tailed.lines().collect();
    assert_eq!(lines.len(), 4, "{tailed}");
    assert!(lines[0].ends_with(" 0x10 5 one"), "{}", lines[0]);
    assert!(lines[1].ends_with(" 0x10 5 two"));
    assert!(lines[2].ends_with(" 0x2 5 three"));
    assert!(lines[3].ends_with(" 0x2 5 four"));

    let filtered = fixture.ok(&[
        "tail",
        "p",
        "--from",
        "start",
        "--limit",
        "2",
        "--filter",
        "eq:0xFF:2",
    ]);
    assert!(
        filtered.contains("three") && !filtered.contains("one"),
        "{filtered}"
    );
    assert!(
        fixture
            .fails(&["tail", "p", "--filter", "wat"])
            .contains("wat")
    );

    // A named cursor carries on from where the last one stopped. Reading all of the first record
    // moves past it, and the commit covers only what was printed.
    let first = fixture.ok(&[
        "tail", "p", "--from", "start", "--limit", "2", "--name", "me",
    ]);
    assert!(first.contains("two"), "{first}");
    let rest = fixture.ok(&[
        "tail", "p", "--from", "start", "--limit", "1", "--name", "me",
    ]);
    assert!(rest.contains("three"), "{rest}");
    drop(fixture.server.shutdown());
}

#[test]
fn an_issued_token_grants_what_it_says_and_nothing_else() {
    let mut fixture = start();
    fixture.token = fixture.issue("logs.*:publish", "60000");
    fixture.ok(&["publish", "logs.a", "x"]);
    assert!(
        fixture
            .fails(&["publish", "other", "x"])
            .contains("error 1")
    );
    assert!(
        fixture
            .fails(&["tail", "logs.a", "--from", "start"])
            .contains("error 1")
    );

    fixture.token = fixture.issue("*:read", "0");
    assert!(fixture.fails(&["stream", "info", "logs.a"]).contains("401"));

    let bad = |args: &[&str]| {
        let output = Command::new(BIN)
            .args(["token", "issue", "--ttl-ms", "1"])
            .arg("--secret-file")
            .arg(&fixture.secret_file)
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
    };
    bad(&["--author", "0", "--grant", "*:read"]);
    bad(&["--author", "1", "--grant", "*:fly"]);
    bad(&["--author", "1", "--grant", "nocolon"]);
    bad(&["--author", "1"]);
    drop(fixture.server.shutdown());
}

fn build_server(bin_dir: &Path) -> PathBuf {
    let status = Command::new(env!("CARGO"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "build",
            "--offline",
            "-p",
            "server",
            "--bin",
            "sillystream-server",
            // This overwrites the binary the crash matrix runs (`CARGO_BIN_EXE_...`), so it keeps
            // the crash points, which do nothing unless their variable is set.
            "--features",
            "test-hooks",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    bin_dir.join("sillystream-server")
}

/// A small run of every part of `bench`, to show the tables come out with numbers in them. The
/// numbers themselves are in docs/BENCH.md, from a release build.
///
/// The parts run at once, each with its own servers, so the test takes as long as the slowest
/// part, not all of them in turn. The ack matrix runs level 3 only: a flat-out level 0 row in a
/// debug build spends most of a minute delivering what it published.
#[test]
fn bench_prints_tables_for_each_level_and_throughput_and_memory() {
    let fixture = start();
    let server_bin = build_server(Path::new(BIN).parent().unwrap());
    let small = [
        "--server-bin",
        server_bin.to_str().unwrap(),
        "--count",
        "25",
        "--seconds",
        "1",
        "--idle-count",
        "50",
        "--idle-streams",
        "2000",
        "--connect-count",
        "200",
        "--cores",
        "1",
        "--seal-windows",
        "1000",
        "--subscribers",
        "1",
        "--wal-mib",
        "0,1",
        "--runs",
        "1",
    ];
    let parts: [&[&str]; 11] = [
        &["latency"],
        &["throughput"],
        &["idle"],
        &["catch-up"],
        &["many-streams"],
        &["puts"],
        &["idle-streams"],
        &["ack-matrix", "--levels", "3"],
        &["fan-out"],
        &["connect"],
        &["recovery"],
    ];
    let runs: Vec<_> = parts
        .iter()
        .map(|part| {
            let mut args = vec!["bench"];
            args.extend_from_slice(part);
            args.extend_from_slice(&small);
            let mut command = fixture.command(&args);
            let part = part.join(" ");
            std::thread::spawn(move || (part, command.output().unwrap()))
        })
        .collect();
    let mut output = String::new();
    for run in runs {
        let (part, run) = run.join().unwrap();
        assert!(run.status.success(), "bench {part}: {run:?}");
        output.push_str(&String::from_utf8(run.stdout).unwrap());
    }
    for row in [
        "| 0 | publish to delivery",
        "| 1 | ack",
        "| 2 | ack",
        "| 3 | ack",
    ] {
        assert!(output.contains(row), "{row}: {output}");
    }
    assert!(output.contains("### Throughput"), "{output}");
    assert!(output.contains("| 2 | 4 | 65536 |"), "{output}");
    assert!(output.contains("| 50 |"), "{output}");
    assert!(output.contains("### Ack levels by seal window"), "{output}");
    assert!(output.contains("| 1000 | 3 | 1 stream, 50/s |"), "{output}");
    assert!(output.contains("### Fan-out"), "{output}");
    assert!(output.contains("| 1 | 1 | flat out |"), "{output}");
    // 256 records of 4,096 bytes, all acked, then all read back after the crash.
    assert!(output.contains("### Recovery"), "{output}");
    assert!(output.contains("| 1 | 1 | 256 |"), "{output}");
    drop(fixture.dir);
    drop(fixture.server.shutdown());
}
