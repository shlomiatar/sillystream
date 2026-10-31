//! What one client may make the server hold or carry on doing: tokens that change or end while
//! a connection lives, frames that never finish, a subscriber that stops reading, and the frame
//! size the server advertises.

mod api;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use api::*;
use proto::{AckLevel, ClientFrame, ErrorCode};
use server::{Clock, ManualClock, SystemClock};
use tokio::io::AsyncWriteExt;

const READ: u8 = auth::PERM_READ;

async fn publish_acked(writer: &mut Ws, seq: u64, data: &[u8]) {
    writer
        .publish(1, seq, AckLevel::WalFsync, &record(0, data))
        .await;
    assert!(matches!(writer.recv_owned().await, Owned::Ack { .. }));
}

/// Frames until the wait passes with nothing more, and whether any held these bytes.
async fn saw_record(ws: &mut Ws, data: &[u8], wait_ms: u64) -> bool {
    let mut seen = false;
    while let Some(frame) = ws.recv_within(wait_ms).await {
        match frame {
            Owned::Data { records, .. } => {
                seen |= parse_records(&records)
                    .iter()
                    .any(|r| r.3.starts_with(data));
            }
            Owned::Closed => break,
            _ => {}
        }
    }
    seen
}

/// Section 12 revokes by expiry, and a renewal is how a client outlives one token. So a stream
/// dropped from the renewed token has to end there, or nothing ever could cut the reader off.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_without_the_stream_ends_its_subscription() {
    let server = start();
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "secret").await;
    publish_acked(&mut writer, 1, b"before").await;

    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(1, "secret", u64::MAX).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    let narrow = token_at(START_MS + 1_000, 5, &[("other", READ)]);
    reader
        .send(&ClientFrame::Hello {
            ver: 1,
            token: &narrow,
        })
        .await;
    assert!(matches!(reader.recv_owned().await, Owned::Welcome { .. }));
    assert_eq!(
        reader.recv_owned().await,
        Owned::Error {
            reference: 1,
            code: ErrorCode::Unauthorized
        }
    );
    publish_acked(&mut writer, 2, b"after-narrow").await;
    // Still refused for what it is, rather than as a subscription that never was.
    reader.send(&ClientFrame::Commit { sub: 1, id: 1 }).await;
    assert_eq!(
        reader.recv_within(WAIT_MS).await,
        Some(Owned::Error {
            reference: 1,
            code: ErrorCode::Unauthorized
        })
    );
    assert!(!saw_record(&mut reader, b"after-narrow", 500).await);
    drop((writer, reader));
    server.server.shutdown();
}

/// The same reached through expiry: a long token for something else can't keep a short one's
/// subscription going past its end.
#[tokio::test(flavor = "multi_thread")]
async fn an_unrelated_long_token_cant_keep_a_subscription_past_expiry() {
    let manual = Arc::new(ManualClock::new(START_MS));
    let server = start_with(node_config(1), Arc::new(SystemClock), manual);
    let now_ms = SystemClock.now_ms();
    let far = token_at(now_ms + 3_600_000, AUTHOR, &[("*", ALL)]);
    let mut writer = server.ws(&far).await;
    writer.open(1, "secret").await;
    publish_acked(&mut writer, 1, b"a").await;

    let short = token_at(now_ms + 500, AUTHOR, &[("secret", READ)]);
    let other = token_at(now_ms + 3_600_000, AUTHOR, &[("public", READ)]);
    let mut reader = server.ws(&short).await;
    reader.subscribe(1, "secret", u64::MAX).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    reader
        .send(&ClientFrame::Hello {
            ver: 1,
            token: &other,
        })
        .await;
    assert!(matches!(reader.recv_owned().await, Owned::Welcome { .. }));
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    publish_acked(&mut writer, 2, b"after-expiry").await;
    assert!(!saw_record(&mut reader, b"after-expiry", 500).await);
    drop((writer, reader));
    server.server.shutdown();
}

/// A token's end has to come even when frames never stop arriving for the connection.
#[tokio::test(flavor = "multi_thread")]
async fn expiry_ends_a_subscription_under_a_steady_flow_of_records() {
    let manual = Arc::new(ManualClock::new(START_MS));
    let server = Arc::new(start_with(node_config(1), Arc::new(SystemClock), manual));
    let now_ms = SystemClock.now_ms();
    let far = token_at(now_ms + 3_600_000, AUTHOR, &[("*", ALL)]);
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let (server, far, stop) = (Arc::clone(&server), far.clone(), Arc::clone(&stop));
        tasks.push(tokio::spawn(async move {
            let mut writer = server.ws(&far).await;
            writer.open(1, "hot").await;
            let mut seq = 0;
            while !stop.load(Ordering::Relaxed) {
                seq += 1;
                writer
                    .publish(1, seq, AckLevel::Memory, &record(0, b"x"))
                    .await;
                let _ = writer.recv_within(WAIT_MS).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let short = token_at(now_ms + 600, AUTHOR, &[("hot", READ)]);
    let mut reader = server.ws(&short).await;
    reader.subscribe(1, "hot", u64::MAX).await;
    let started = Instant::now();
    let mut ended = false;
    while started.elapsed() < Duration::from_secs(5) {
        match reader.recv_within(1_000).await {
            Some(Owned::Error {
                reference: 1,
                code: ErrorCode::Unauthorized,
            }) => {
                ended = true;
                break;
            }
            Some(Owned::Closed) | None => break,
            _ => {}
        }
    }
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        task.await.unwrap();
    }
    assert!(ended, "the subscription was still running after its token");
}

/// A frame's claimed length is only a claim. A token with no grants, at one payload byte per
/// connection, must not use up the core's memory cap and stop everyone else's publishes.
#[tokio::test(flavor = "multi_thread")]
async fn claiming_big_frames_doesnt_stall_other_publishers() {
    let server = start_node(node_config(1));
    let nothing = token_at(START_MS + 3_600_000, 5, &[]);
    let mut holders = Vec::new();
    for _ in 0..260 {
        let mut ws = server.ws(&nothing).await;
        let socket = ws.socket.get_mut();
        let mut frame = vec![0x82u8, 0x80 | 127];
        frame.extend_from_slice(&(16u64 * 1024 * 1024 - 64).to_be_bytes());
        frame.extend_from_slice(&[1, 2, 3, 4, 0]);
        socket.write_all(&frame).await.unwrap();
        socket.flush().await.unwrap();
        holders.push(ws);
    }
    for _ in 0..500 {
        if server.server.io_stats().held_buffer_count == 260 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(server.server.io_stats().held_buffer_count, 260);
    assert!(
        server.server.pinned_bytes() < 1024 * 1024 * 1024,
        "pinned {}",
        server.server.pinned_bytes()
    );
    let mut victim = server.ws(&token(ALL)).await;
    victim.open(1, "victim").await;
    victim
        .publish(1, 1, AckLevel::WalFsync, &record(0, b"v"))
        .await;
    let got = victim.recv_within(5_000).await;
    assert!(matches!(got, Some(Owned::Ack { .. })), "stalled: {got:?}");
    drop((victim, holders));
    server.server.shutdown();
}

/// A big frame arriving in pieces still arrives whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_frame_in_many_pieces_arrives_intact() {
    let server = start();
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "pieces").await;
    let data: Vec<u8> = (0..1_000_000u32).map(|at| (at % 251) as u8).collect();
    let mut frame = Vec::new();
    ClientFrame::Publish(proto::Publish {
        handle: 1,
        producer_id: 9,
        producer_seq: 1,
        ack: AckLevel::WalFsync,
        state: None,
        records: &record(0, &data),
    })
    .encode(&mut frame)
    .unwrap();
    let mask = [9u8, 8, 7, 6];
    let mut head = vec![0x82u8, 0x80 | 127];
    head.extend_from_slice(&(frame.len() as u64).to_be_bytes());
    head.extend_from_slice(&mask);
    for (at, byte) in frame.iter_mut().enumerate() {
        *byte ^= mask[at % 4];
    }
    let socket = writer.socket.get_mut();
    socket.write_all(&head).await.unwrap();
    for piece in frame.chunks(90_000) {
        socket.write_all(piece).await.unwrap();
        socket.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(matches!(writer.recv_owned().await, Owned::Ack { .. }));
    assert_eq!(server.server.io_stats().held_buffer_count, 0);
    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(1, "pieces", 0).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    let Owned::Data { records, .. } = reader.recv_owned().await else {
        panic!("expected DATA");
    };
    assert_eq!(&parse_records(&records)[0].3[..data.len()], &data[..]);
    drop((writer, reader));
    server.server.shutdown();
}

/// What did arrive of an unfinished frame or message is real memory, so it can't be held for ever.
#[tokio::test(flavor = "multi_thread")]
async fn an_unfinished_frame_or_message_ends_the_connection_in_time() {
    let mut node = node_config(1);
    node.partial_frame_timeout_ms = 300;
    let server = start_node(node);
    let nothing = token_at(START_MS + 3_600_000, 5, &[]);

    let mut frame_holder = server.ws(&nothing).await;
    let socket = frame_holder.socket.get_mut();
    let mut frame = vec![0x82u8, 0x80 | 126];
    frame.extend_from_slice(&1000u16.to_be_bytes());
    frame.extend_from_slice(&[1, 2, 3, 4, 0]);
    socket.write_all(&frame).await.unwrap();
    socket.flush().await.unwrap();

    // The first frame of a fragmented message, with its last never sent.
    let mut message_holder = server.ws(&nothing).await;
    let socket = message_holder.socket.get_mut();
    socket
        .write_all(&[0x02, 0x80 | 1, 1, 2, 3, 4, 0])
        .await
        .unwrap();
    socket.flush().await.unwrap();

    for ws in [&mut frame_holder, &mut message_holder] {
        let mut closed = false;
        while let Some(got) = ws.recv_within(3_000).await {
            if got == Owned::Closed {
                closed = true;
                break;
            }
        }
        assert!(closed, "still open past the timeout");
    }

    // A connection that is merely idle stays.
    let mut idle = server.ws(&nothing).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    idle.round_trip(1).await;
    drop((frame_holder, message_holder, idle));
    server.server.shutdown();
}

/// STATE frames count as replies once they are on their way, and a subscriber that stops reading
/// must close the connection's window for them too. D88 then keeps only the latest.
#[tokio::test(flavor = "multi_thread")]
async fn a_state_subscriber_that_stops_reading_holds_a_bounded_amount() {
    const OPS_COUNT: u32 = 1_500;
    let server = start_node(node_config(2));
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "st").await;
    let doc = {
        use ciborium::value::Value;
        cbor(&Value::Map(vec![(
            Value::Text("k".into()),
            Value::Bytes(vec![7u8; 60_000]),
        )]))
    };
    publish_acked(&mut writer, 1, b"makes the stream").await;
    let mut reader = server.ws(&token(ALL)).await;
    reader
        .send(&ClientFrame::Subscribe(proto::Subscribe {
            sub: 1,
            from: u64::MAX,
            linger_ms: 0,
            author: None,
            clauses: proto::WireClauses::default(),
            stream: "st",
            name: None,
            with_state: false,
            state_only: true,
            include_system: false,
        }))
        .await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    let mut last_ver = 0;
    for start in (0..OPS_COUNT).step_by(50) {
        for req in start..start + 50 {
            writer
                .send(&ClientFrame::StateOp {
                    handle: 1,
                    req: req + 1,
                    expect_ver: 0,
                    op: proto::StateOp::Set,
                    doc: &doc,
                })
                .await;
        }
        for _ in 0..50 {
            let Owned::StateResult { ok: true, ver, .. } = writer.recv_owned().await else {
                panic!("expected a good STATE_RESULT");
            };
            last_ver = ver;
        }
    }
    let mut state_count = 0u32;
    let mut newest = 0;
    while let Some(frame) = reader.recv_within(1_000).await {
        if let Owned::State { ver, .. } = frame {
            state_count += 1;
            newest = ver;
        }
    }
    assert_eq!(newest, last_ver, "the latest state must still arrive");
    assert!(
        state_count < 400,
        "{state_count} of {OPS_COUNT} STATE frames were held for a reader that wasn't reading"
    );
    drop((writer, reader));
    server.server.shutdown();
}

/// D43: `max_frame` covers the whole frame, so what HTTP takes has to fit `DATA` around it.
#[tokio::test(flavor = "multi_thread")]
async fn nothing_http_accepts_makes_a_data_frame_over_max_frame() {
    const MAX_BYTES: usize = 64 * 1024;
    let mut node = node_config(2);
    node.max_frame_bytes = MAX_BYTES as u64;
    let server = start_node(node);
    let text = token(ALL);
    let records_type = ("content-type", "application/x-sillystream-records");
    let room_bytes = MAX_BYTES - proto::DATA_PREFIX_BYTES;

    // Binary: a record just over and just inside.
    let over = record(1, &vec![9u8; MAX_BYTES - 32]);
    assert_eq!(over.len(), MAX_BYTES);
    let reply = server
        .http(
            &Http::new("POST", "/v1/stream/b/publish?ack=2", &text)
                .header(records_type.0, records_type.1)
                .body(over),
        )
        .await;
    assert_eq!((reply.status, reply.code()), (413, Some(6)));
    let fits = record(1, &vec![9u8; room_bytes / 8 * 8 - 32]);
    let fits_bytes = fits.len();
    let reply = server
        .http(
            &Http::new("POST", "/v1/stream/b/publish?ack=2", &text)
                .header(records_type.0, records_type.1)
                .body(fits),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());

    // The simple form: the record is the body and a header.
    let reply = server
        .http(&Http::new("POST", "/v1/stream/s/publish?ack=2", &text).body(vec![1u8; 65_504]))
        .await;
    assert_eq!((reply.status, reply.code()), (413, Some(6)));
    let body_bytes = room_bytes / 8 * 8 - 32;
    let reply = server
        .http(&Http::new("POST", "/v1/stream/s/publish?ack=2", &text).body(vec![1u8; body_bytes]))
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());

    let mut ws = server.ws(&text).await;
    for stream in ["b", "s"] {
        ws.subscribe(1, stream, 0).await;
        assert!(matches!(ws.recv_owned().await, Owned::Subscribed { .. }));
        let Owned::Data { records, .. } = ws.recv_owned().await else {
            panic!("expected DATA");
        };
        assert_eq!(records.len(), fits_bytes);
        assert!(records.len() + proto::DATA_PREFIX_BYTES <= MAX_BYTES);
        ws.send(&ClientFrame::Unsubscribe { sub: 1 }).await;
    }
    drop(ws);
    server.server.shutdown();
}

/// A stock client trusts its frame limit, which is 16 MiB by default like ours. The largest record
/// HTTP takes has to reach it, or that client can never get past the record. A larger one is
/// refused, which `nothing_http_accepts_makes_a_data_frame_over_max_frame` checks.
#[tokio::test(flavor = "multi_thread")]
async fn a_stock_client_reads_the_largest_record_http_accepts() {
    let server = start_node(node_config(2));
    let text = token(ALL);
    let max_bytes = 16 * 1024 * 1024usize;
    let largest = (max_bytes - proto::DATA_PREFIX_BYTES) / 8 * 8;
    let reply = server
        .http(
            &Http::new("POST", "/v1/stream/big/publish?ack=2", &text)
                .header("content-type", "application/x-sillystream-records")
                .body(record(1, &vec![9u8; largest - 32])),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let mut ws = server.ws(&text).await;
    ws.subscribe(1, "big", 0).await;
    assert!(matches!(ws.recv_owned().await, Owned::Subscribed { .. }));
    let got = ws.recv_owned().await;
    assert!(matches!(got, Owned::Data { .. }), "got {got:?}");
    drop(ws);
    server.server.shutdown();
}

/// Section 12: a token that has ended fails the next operation with error 1, whether or not the
/// subscription it names is still there.
#[tokio::test(flavor = "multi_thread")]
async fn commit_on_an_ended_token_is_error_1() {
    let server = start();
    let ending = token_at(START_MS + 1_000, 5, &[("*", ALL)]);
    let mut ws = server.ws(&ending).await;
    ws.open(1, "c").await;
    publish_acked(&mut ws, 1, b"x").await;
    ws.subscribe(1, "c", u64::MAX).await;
    assert!(matches!(ws.recv_owned().await, Owned::Subscribed { .. }));
    server.clock.set(START_MS + 1_000);
    for sub in [42, 1, 1] {
        ws.send(&ClientFrame::Commit { sub, id: 1 }).await;
        assert_eq!(
            ws.recv_owned().await,
            Owned::Error {
                reference: sub,
                code: ErrorCode::Unauthorized
            }
        );
    }
    drop(ws);
    server.server.shutdown();
}

/// The core refuses an id already in use and keeps the subscription it has, so the connection's
/// own idea of what that id reads must stay too, or a renewal checks the wrong stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_resubscribe_doesnt_hide_the_live_one_from_a_renewal() {
    let server = start();
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "secret").await;
    publish_acked(&mut writer, 1, b"before").await;
    writer.open(2, "other").await;
    publish_acked(&mut writer, 2, b"before").await;

    let both = token_at(
        START_MS + 3_600_000,
        5,
        &[("secret", READ), ("other", READ)],
    );
    let mut reader = server.ws(&both).await;
    reader.subscribe(1, "secret", u64::MAX).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    reader.subscribe(1, "other", u64::MAX).await;
    assert_eq!(
        reader.recv_owned().await,
        Owned::Error {
            reference: 1,
            code: ErrorCode::Malformed
        }
    );
    let narrow = token_at(START_MS + 3_600_000, 5, &[("other", READ)]);
    reader
        .send(&ClientFrame::Hello {
            ver: 1,
            token: &narrow,
        })
        .await;
    assert!(matches!(reader.recv_owned().await, Owned::Welcome { .. }));
    assert_eq!(
        reader.recv_owned().await,
        Owned::Error {
            reference: 1,
            code: ErrorCode::Unauthorized
        },
        "the live subscription on `secret` wasn't ended"
    );
    publish_acked(&mut writer, 3, b"after-revoke").await;
    assert!(!saw_record(&mut reader, b"after-revoke", 500).await);
    drop((writer, reader));
    server.server.shutdown();
}

/// Only subscriptions that exist count against the limit.
#[tokio::test(flavor = "multi_thread")]
async fn refused_subscribes_dont_use_up_the_limit() {
    let mut node = node_config(1);
    node.max_subscriptions_per_connection = 2;
    let server = start_node(node);
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "s").await;
    publish_acked(&mut writer, 1, b"x").await;

    let mut reader = server.ws(&token(ALL)).await;
    for sub in [100, 101, 102] {
        reader
            .send(&ClientFrame::Subscribe(proto::Subscribe {
                sub,
                from: u64::MAX,
                linger_ms: 0,
                author: None,
                clauses: proto::WireClauses::default(),
                stream: "s",
                name: Some("sink:1"),
                with_state: false,
                state_only: false,
                include_system: false,
            }))
            .await;
        assert_eq!(
            reader.recv_owned().await,
            Owned::Error {
                reference: sub,
                code: ErrorCode::Malformed
            }
        );
    }
    reader.subscribe(1, "s", u64::MAX).await;
    assert!(
        matches!(reader.recv_owned().await, Owned::Subscribed { sub: 1, .. }),
        "no subscription is live, so the limit can't be reached"
    );
    drop((writer, reader));
    server.server.shutdown();
}

/// HTTP's version of `claiming_big_frames_doesnt_stall_other_publishers`: a `Content-Length` is
/// only a claim too, and a head costs a client almost nothing.
#[tokio::test(flavor = "multi_thread")]
async fn claiming_big_http_bodies_doesnt_stall_other_publishers() {
    let server = start_node(node_config(1));
    let publisher = token(auth::PERM_PUBLISH);
    let head = format!(
        "POST /v1/stream/x/publish?ack=2 HTTP/1.1\r\nhost: l\r\nauthorization: Bearer {publisher}\r\n\
         content-type: application/x-sillystream-records\r\ncontent-length: 16777152\r\n\r\n"
    );
    let mut holders = Vec::new();
    for _ in 0..260 {
        let mut tls = server.tls().await;
        tls.write_all(head.as_bytes()).await.unwrap();
        tls.flush().await.unwrap();
        holders.push(tls);
    }
    for _ in 0..500 {
        if server.server.io_stats().held_buffer_count == 260 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(server.server.io_stats().held_buffer_count, 260);
    assert!(
        server.server.pinned_bytes() < 1024 * 1024 * 1024,
        "pinned {}",
        server.server.pinned_bytes()
    );
    let mut victim = server.ws(&token(ALL)).await;
    victim.open(1, "victim").await;
    victim
        .publish(1, 1, AckLevel::WalFsync, &record(0, b"v"))
        .await;
    let got = victim.recv_within(5_000).await;
    assert!(matches!(got, Some(Owned::Ack { .. })), "stalled: {got:?}");
    drop((victim, holders));
    server.server.shutdown();
}

/// A body read through its own buffer still arrives whole, in pieces, in both publish forms.
/// The owner refuses padding that isn't zero, so a 200 says the slab's old bytes were cleared.
#[tokio::test(flavor = "multi_thread")]
async fn http_bodies_in_pieces_arrive_intact() {
    let server = start();
    let all = token(ALL);
    let data: Vec<u8> = (0..1_000_001u32).map(|at| (at % 251) as u8).collect();
    let simple = format!(
        "POST /v1/stream/pieces/publish?ack=2 HTTP/1.1\r\nhost: l\r\nauthorization: Bearer {all}\r\n\
         content-length: {}\r\n\r\n",
        data.len()
    );
    let records = record(0, &data);
    let binary = format!(
        "POST /v1/stream/pieces/publish?ack=2 HTTP/1.1\r\nhost: l\r\nauthorization: Bearer {all}\r\n\
         content-type: application/x-sillystream-records\r\ncontent-length: {}\r\n\r\n",
        records.len()
    );
    for (head, body) in [(simple, &data), (binary, &records)] {
        let mut tls = server.tls().await;
        tls.write_all(head.as_bytes()).await.unwrap();
        for piece in body.chunks(90_000) {
            tls.write_all(piece).await.unwrap();
            tls.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let reply = read_http_reply(&mut tls).await;
        assert_eq!(reply.status, 200, "{}", reply.text());
    }
    assert_eq!(server.server.io_stats().held_buffer_count, 0);
    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(1, "pieces", 0).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    let mut got = Vec::new();
    while got.len() < 2 {
        if let Owned::Data { records, .. } = reader.recv_owned().await {
            got.extend(parse_records(&records));
        }
    }
    for (_, _, _, bytes) in got {
        assert_eq!(bytes, data);
    }
    drop(reader);
    server.server.shutdown();
}
