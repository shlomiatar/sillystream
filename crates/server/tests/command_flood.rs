//! What one connection can make a core do with commands that have no answer on the socket. In
//! its own binary because it measures the process's memory, which another test would disturb.

mod api;

use std::time::{Duration, Instant};

use api::*;
use proto::ClientFrame;
use tokio::io::AsyncWriteExt;

const READ: u8 = auth::PERM_READ;

fn rss_kib() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap()
}

/// `frames` masked and repeated, as a client would send them with no care for the answers.
fn masked_batch(frame: &ClientFrame<'_>, count: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    frame.encode(&mut payload).unwrap();
    assert!(payload.len() < 126);
    let mask = [1u8, 2, 3, 4];
    let mut one = vec![0x82u8, 0x80 | payload.len() as u8];
    one.extend_from_slice(&mask);
    for (at, byte) in payload.iter().enumerate() {
        one.push(byte ^ mask[at % 4]);
    }
    one.repeat(count)
}

/// Writes for `flood_ms` on a socket it never reads, then gives it back still open.
async fn flood(ws: &mut Ws, batch: &[u8], flood_ms: u64) {
    let socket = ws.socket.get_mut();
    let started = Instant::now();
    let mut at = 0;
    while started.elapsed() < Duration::from_millis(flood_ms) {
        // A single write can be given up on without losing the place in a frame.
        let write = tokio::time::timeout(Duration::from_millis(200), socket.write(&batch[at..]));
        if let Ok(written_bytes) = write.await {
            at = (at + written_bytes.unwrap()) % batch.len();
        }
    }
}

/// A `COMMIT` has no reply and passes its check on an id the connection never had, so nothing
/// on the socket slows it. Nothing else did either: the core's inbox has no bound.
#[tokio::test(flavor = "multi_thread")]
async fn commit_floods_hold_little_and_dont_delay_the_core() {
    let server = start_node(node_config(1));
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "s").await;
    writer
        .publish(1, 1, proto::AckLevel::WalFsync, &record(0, b"x"))
        .await;
    assert!(matches!(writer.recv_owned().await, Owned::Ack { .. }));
    let info = Http::new("GET", "/v1/stream/s", &token(ALL)).bytes();

    // Twice: with no grants on an id it never had, then with read on a named cursor, which
    // takes a second trip through the core's inbox.
    let nothing = token_at(START_MS + 3_600_000, 5, &[]);
    let reader = token_at(START_MS + 3_600_000, 5, &[("s", READ)]);
    for (flooder_token, named) in [(&nothing, false), (&reader, true)] {
        let mut flooder = server.ws(flooder_token).await;
        if named {
            flooder
                .send(&ClientFrame::Subscribe(proto::Subscribe {
                    sub: 1,
                    from: 0,
                    linger_ms: 0,
                    author: None,
                    clauses: proto::WireClauses::default(),
                    stream: "s",
                    name: Some("c"),
                    with_state: false,
                    state_only: false,
                    include_system: false,
                }))
                .await;
            assert!(matches!(
                flooder.recv_owned().await,
                Owned::Subscribed { .. }
            ));
        }
        let batch = masked_batch(&ClientFrame::Commit { sub: 1, id: 1 }, 2_000);
        let before_kib = rss_kib();
        flood(&mut flooder, &batch, 6_000).await;
        let after_kib = rss_kib();
        assert!(
            after_kib < before_kib + 100 * 1024,
            "named {named}: RSS went from {before_kib} KiB to {after_kib} KiB"
        );
        // Another client is served at once, with the flood's bytes still being read.
        let started = Instant::now();
        let mut tls = server.tls().await;
        tls.write_all(&info).await.unwrap();
        let reply = read_http_reply(&mut tls).await;
        assert_eq!(reply.status, 200);
        assert!(
            started.elapsed() < Duration::from_millis(1_500),
            "named {named}: a GET took {:?}",
            started.elapsed()
        );
    }
    drop(writer);
    server.server.shutdown();
}
