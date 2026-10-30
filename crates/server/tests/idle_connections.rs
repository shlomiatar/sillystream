//! Section 6.5: an idle connection holds no read buffer. 10,000 of them, over real TLS, asserted
//! on the pool's counters. Memory per connection is measured by `sillystream bench idle`, on
//! the server's own process, and recorded in docs/BENCH.md.

mod api;

use std::sync::Arc;
use std::time::Duration;

use api::*;
use proto::ClientFrame;
use tokio::sync::Semaphore;

const IDLE_COUNT: usize = 10_000;

/// A file descriptor limit too low makes this fail on connect rather than on what it tests, so
/// it says so plainly. Client and server are one process here, so each connection takes two.
fn descriptors_needed_are_available() {
    let output = std::process::Command::new("sh")
        .args(["-c", "ulimit -n"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    let limit = text.trim();
    assert!(
        limit == "unlimited" || limit.parse::<usize>().unwrap_or(0) > IDLE_COUNT * 2 + 64,
        "this test needs ulimit -n above {}, and it is {limit}",
        IDLE_COUNT * 2 + 64,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ten_thousand_idle_connections_hold_no_read_buffer() {
    descriptors_needed_are_available();
    let server = Arc::new(start_node(node_config(4)));
    let text = Arc::new(token(ALL));
    // macOS has a listen backlog of 128, so connecting all at once would drop some.
    let gate = Arc::new(Semaphore::new(64));
    let mut tasks = Vec::with_capacity(IDLE_COUNT);
    for _ in 0..IDLE_COUNT {
        let server = Arc::clone(&server);
        let text = Arc::clone(&text);
        let gate = Arc::clone(&gate);
        tasks.push(tokio::spawn(async move {
            let _permit = gate.acquire_owned().await.unwrap();
            server.ws(&text).await
        }));
    }
    let mut sockets = Vec::with_capacity(IDLE_COUNT);
    for task in tasks {
        sockets.push(task.await.unwrap());
    }
    let stats = server.server.io_stats();
    assert_eq!(stats.open_connection_count, IDLE_COUNT as u64);
    assert_eq!(stats.handshake_failed_count, 0);
    // The reads did happen, and every one gave its buffer back.
    assert!(
        stats.read_borrow_count >= IDLE_COUNT as u64,
        "{stats:?}: the counter has to be live for its zero to mean anything"
    );
    assert_eq!(stats.held_buffer_count, 0, "{stats:?}");
    // One slab per core being filled, however many connections there are.
    let slab_bytes = node_config(1).slab_bytes as u64;
    assert!(
        server.server.pinned_bytes() <= 4 * slab_bytes,
        "{} bytes pinned",
        server.server.pinned_bytes()
    );

    // They stay that way sitting still, with the timers and checks of a node running.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(server.server.io_stats().held_buffer_count, 0);

    // And they are real: a sample still answers, and the count of reads shows it.
    let before = server.server.io_stats().read_borrow_count;
    for ws in sockets.iter_mut().step_by(1000) {
        ws.round_trip(1).await;
    }
    let after = server.server.io_stats();
    assert!(after.read_borrow_count > before);
    assert_eq!(after.held_buffer_count, 0, "{after:?}");
    drop(sockets);
    let mut closed = false;
    for _ in 0..2000 {
        if server.server.io_stats().open_connection_count == 0 {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(closed, "{:?}", server.server.io_stats());
}

/// The zero above could come from a counter that never moves. A frame stopped half way holds its
/// buffer, and giving it the rest lets go of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_part_way_through_is_counted_and_let_go() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = start_node(node_config(1));
    let mut raw = server.tls().await;
    raw.write_all(
        b"GET /v1/ws HTTP/1.1\r\nhost: localhost\r\nupgrade: websocket\r\nconnection: Upgrade\r\n\
          sec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buffer = [0u8; 4096];
    let read = raw.read(&mut buffer).await.unwrap();
    assert!(buffer[..read].starts_with(b"HTTP/1.1 101"));

    let mask = [1u8, 2, 3, 4];
    let mut hello = Vec::new();
    let text = token(ALL);
    ClientFrame::Hello {
        ver: 1,
        token: &text,
    }
    .encode(&mut hello)
    .unwrap();
    let masked: Vec<u8> = hello
        .iter()
        .enumerate()
        .map(|(i, byte)| byte ^ mask[i % 4])
        .collect();
    // RFC 6455 wants the shortest length form.
    let mut frame = vec![0x82];
    if hello.len() < 126 {
        frame.push(0x80 | hello.len() as u8);
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(hello.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(&mask);
    frame.extend_from_slice(&masked[..10]);
    raw.write_all(&frame).await.unwrap();
    raw.flush().await.unwrap();

    let mut seen_held = false;
    for _ in 0..500 {
        if server.server.io_stats().held_buffer_count == 1 {
            seen_held = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(seen_held, "{:?}", server.server.io_stats());

    raw.write_all(&masked[10..]).await.unwrap();
    raw.flush().await.unwrap();
    let read = raw.read(&mut buffer).await.unwrap();
    assert_eq!(buffer[2], proto::TYPE_WELCOME, "{read}");
    assert_eq!(server.server.io_stats().held_buffer_count, 0);
    drop(raw);
    server.server.shutdown();
}
