//! What a client that goes slowly, or doesn't read, can hold on to: through real sockets, so
//! these wait out the real 30 s limits.

mod api;

use std::time::{Duration, Instant};

use api::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: usize = 1024 * 1024;
/// `REQUEST_TIMEOUT_MS`, and how late the server may be about it on a busy machine.
const REQUEST_LIMIT: Duration = Duration::from_secs(30);
const LATE: Duration = Duration::from_secs(5);

async fn seed(server: &TestServer, stream: &str, count: usize, each_bytes: usize) {
    let text = token(ALL);
    let path = format!("/v1/stream/{stream}/publish");
    for _ in 0..count {
        let reply = server
            .http(&Http::new("POST", &path, &text).body(vec![b'x'; each_bytes]))
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text());
    }
}

/// Each byte used to start the wait again, so a head could take weeks and still be read.
#[tokio::test(flavor = "multi_thread")]
async fn a_head_gets_30_s_in_all_however_its_bytes_trickle() {
    let server = start();
    let mut tls = server.tls().await;
    let started = Instant::now();
    tls.write_all(b"GET /v1/stream/x HTTP/1.1\r\nx-a: ")
        .await
        .unwrap();
    tls.flush().await.unwrap();
    let mut got = [0u8; 16];
    let closed_at = loop {
        assert!(
            started.elapsed() < REQUEST_LIMIT + LATE,
            "still open after {:?}",
            started.elapsed()
        );
        let wrote = tls.write_all(b"a").await.and(tls.flush().await);
        if wrote.is_err() {
            break started.elapsed();
        }
        match tokio::time::timeout(Duration::from_secs(1), tls.read(&mut got)).await {
            Err(_) => {}
            Ok(Ok(0) | Err(_)) => break started.elapsed(),
            Ok(Ok(_)) => panic!("a response to a head that never ended"),
        }
    };
    assert!(
        closed_at >= REQUEST_LIMIT - LATE,
        "closed after only {closed_at:?}"
    );
    server.server.shutdown();
}

/// A complete head still goes through at a slow pace within the limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_head_within_the_limit_is_answered() {
    let server = start();
    seed(&server, "slow", 1, 8).await;
    let text = token(ALL);
    let request = Http::new("GET", "/v1/stream/slow", &text).bytes();
    let mut tls = server.tls().await;
    let (first, rest) = request.split_at(request.len() / 2);
    tls.write_all(first).await.unwrap();
    tls.flush().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    tls.write_all(rest).await.unwrap();
    tls.flush().await.unwrap();
    assert_eq!(read_http_reply(&mut tls).await.status, 200);
    server.server.shutdown();
}

/// A client that asks and never reads doesn't keep the response in memory for as long as it likes.
#[tokio::test(flavor = "multi_thread")]
async fn a_response_nobody_reads_is_given_up_after_30_s() {
    let mut node = node_config(2);
    node.max_frame_bytes = 16 * MIB as u64;
    let server = start_node(node);
    seed(&server, "big", 14, MIB).await;
    let text = token(ALL);
    let mut tls = server.tls().await;
    tls.write_all(&Http::new("GET", "/v1/stream/big/read?from=0", &text).bytes())
        .await
        .unwrap();
    tls.flush().await.unwrap();
    tokio::time::sleep(REQUEST_LIMIT + LATE).await;
    let mut got_bytes = 0;
    let mut chunk = vec![0u8; 256 * 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(10), tls.read(&mut chunk)).await {
            Ok(Ok(0) | Err(_)) => break,
            Ok(Ok(read_bytes)) => got_bytes += read_bytes,
            Err(_) => panic!("the server is still holding this connection open"),
        }
    }
    assert!(
        got_bytes < 14 * MIB,
        "all {got_bytes} bytes came, so the server hadn't given up"
    );
    server.server.shutdown();
}

/// What read responses hold while they wait for their clients counts against the memory cap.
/// Without this, each connection kept a whole frame of its own, and nothing bounded their sum.
#[tokio::test(flavor = "multi_thread")]
async fn read_responses_waiting_for_their_clients_are_bounded() {
    let mut node = node_config(2);
    // A quarter of this is what waiting responses may hold: just under two of these records.
    node.memory_cap_bytes = 64 * MIB as u64;
    node.max_frame_bytes = 17 * MIB as u64;
    let server = start_node(node);
    seed(&server, "big", 3, 8 * MIB).await;
    let text = token(ALL);
    let read = Http::new("GET", "/v1/stream/big/read?from=0", &text);

    // A response this big won't fit in the socket's buffers, so it stays in memory.
    let mut not_reading = server.tls().await;
    not_reading.write_all(&read.bytes()).await.unwrap();
    not_reading.flush().await.unwrap();
    let started = Instant::now();
    let refused = loop {
        let reply = server.http(&read).await;
        if reply.status == 429 {
            break reply;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "still {}, count {:?}",
            reply.status,
            reply.header("x-count")
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(refused.code(), Some(4), "{}", refused.text());

    // Its end lets the bytes go, so this was never more than waiting for room.
    drop(not_reading);
    let started = Instant::now();
    loop {
        let reply = server.http(&read).await;
        if reply.status == 200 {
            // The rest is left for a read from `x-through`.
            assert_eq!(reply.header("x-count"), Some("1"));
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{}",
            reply.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.server.shutdown();
}

/// One record bigger than all of that still gets read, one reader at a time.
#[tokio::test(flavor = "multi_thread")]
async fn a_record_over_the_bound_can_still_be_read() {
    let mut node = node_config(2);
    node.memory_cap_bytes = 16 * MIB as u64;
    node.max_frame_bytes = 16 * MIB as u64;
    let server = start_node(node);
    seed(&server, "big", 1, 8 * MIB).await;
    let text = token(ALL);
    let reply = server
        .http(&Http::new("GET", "/v1/stream/big/read?from=0", &text))
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    assert_eq!(reply.header("x-count"), Some("1"));
    server.server.shutdown();
}
