//! The WebSocket API against a third-party client, `tokio-tungstenite` (M6).

mod api;

use std::sync::Arc;

use api::*;
use futures_util::SinkExt;
use proto::{AckLevel, ClientFrame, ErrorCode, PublishState};
use server::SystemClock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::{Data, OpCode};

/// `type | flags | handle | producer_id | producer_seq | ack | length`, ahead of the records.
const PUBLISH_HEAD_BYTES: usize = 27;

fn publish_frame(handle: u32, seq: u64, records: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ClientFrame::Publish(proto::Publish {
        handle,
        producer_id: 9,
        producer_seq: seq,
        ack: AckLevel::WalFsync,
        state: None,
        records,
    })
    .encode(&mut out)
    .unwrap();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_and_subscribe_through_a_third_party_client() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "a.stream").await;
    let first = record(0xAB, b"first record");
    ws.publish(1, 1, AckLevel::WalFsync, &first).await;
    let Owned::Ack {
        handle,
        producer_seq,
        first_id,
        count,
    } = ws.recv_owned().await
    else {
        panic!("expected ACK");
    };
    assert_eq!((handle, producer_seq, count), (1, 1, 1));

    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(5, "a.stream", 0).await;
    let Owned::Subscribed { sub, head, .. } = reader.recv_owned().await else {
        panic!("expected SUBSCRIBED");
    };
    assert_eq!((sub, head), (5, first_id));
    let Owned::Data { sub, records } = reader.recv_owned().await else {
        panic!("expected DATA");
    };
    assert_eq!(sub, 5);
    let parsed = parse_records(&records);
    // Stamped by the owner, with the data untouched.
    assert_eq!(parsed, [(first_id, 0xAB, AUTHOR, b"first record".to_vec())]);

    // And live, once caught up.
    let second = record(0xCD, b"second");
    ws.publish(1, 2, AckLevel::WalFsync, &second).await;
    loop {
        match reader.recv_owned().await {
            Owned::Data { records, .. } => {
                assert_eq!(parse_records(&records)[0].3, b"second");
                break;
            }
            Owned::Progress { .. } => {}
            other => panic!("expected DATA, got {other:?}"),
        }
    }
    drop((ws, reader));
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn ping_is_answered_at_both_levels() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.send(&ClientFrame::Ping { nonce: 42 }).await;
    assert_eq!(ws.recv_owned().await, Owned::Pong { nonce: 42 });

    ws.socket.send(Message::Ping(b"hi".to_vec())).await.unwrap();
    // tungstenite answers on its own, so its queue shows only a frame after the WebSocket
    // pong. The server's pong comes back through `next`.
    use futures_util::StreamExt;
    let message = tokio::time::timeout(std::time::Duration::from_millis(WAIT_MS), ws.socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message, Message::Pong(b"hi".to_vec()));
    drop(ws);
    server.server.shutdown();
}

/// Split at every kind of place, including inside the frame's own fields, and with a control
/// frame between fragments, as RFC 6455 allows.
#[tokio::test(flavor = "multi_thread")]
async fn a_fragmented_frame_is_one_frame() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "frag").await;
    let records = record(1, &[7u8; 1000]);
    let frame = publish_frame(1, 1, &records);
    let (a, rest) = frame.split_at(3);
    let (b, c) = rest.split_at(500);
    let send_raw = async |ws: &mut Ws, frame: Frame| {
        ws.socket.send(Message::Frame(frame)).await.unwrap();
    };
    send_raw(
        &mut ws,
        Frame::message(a.to_vec(), OpCode::Data(Data::Binary), false),
    )
    .await;
    send_raw(
        &mut ws,
        Frame::message(b.to_vec(), OpCode::Data(Data::Continue), false),
    )
    .await;
    ws.socket
        .send(Message::Ping(b"between".to_vec()))
        .await
        .unwrap();
    send_raw(
        &mut ws,
        Frame::message(c.to_vec(), OpCode::Data(Data::Continue), true),
    )
    .await;
    let Owned::Ack { first_id, .. } = ws.recv_owned().await else {
        panic!("expected ACK");
    };
    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(1, "frag", 0).await;
    let _ = reader.recv_owned().await;
    let Owned::Data { records: got, .. } = reader.recv_owned().await else {
        panic!("expected DATA");
    };
    assert_eq!(
        parse_records(&got),
        [(first_id, 1, AUTHOR, vec![7u8; 1000])]
    );
    drop((ws, reader));
    server.server.shutdown();
}

/// RFC 6455 5.4: fragments of one message can't have another message between them.
#[tokio::test(flavor = "multi_thread")]
async fn a_text_frame_inside_a_fragmented_message_ends_the_connection() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "frag").await;
    let records = record(1, b"x");
    let frame = publish_frame(1, 1, &records);
    let (a, b) = frame.split_at(3);
    for raw in [
        Frame::message(a.to_vec(), OpCode::Data(Data::Binary), false),
        Frame::message(b"hi".to_vec(), OpCode::Data(Data::Text), true),
        Frame::message(b.to_vec(), OpCode::Data(Data::Continue), true),
    ] {
        // The server may have closed already by the last one.
        let _ = ws.socket.send(Message::Frame(raw)).await;
    }
    let mut closed = false;
    while let Ok(Some(message)) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        futures_util::StreamExt::next(&mut ws.socket),
    )
    .await
    {
        match message {
            Ok(Message::Close(Some(close))) => {
                assert_eq!(u16::from(close.code), 1002);
                closed = true;
                break;
            }
            Ok(Message::Binary(body)) => {
                assert!(body.first() != Some(&0x84), "the message completed");
            }
            _ => {}
        }
    }
    assert!(closed);
    drop(ws);
    server.server.shutdown();
}

/// A frame split over the socket's own reads: a byte at a time for the header, and in pieces
/// for the payload. A raw socket is the only way to choose where those fall.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_arriving_in_pieces_reads_the_same() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "pieces").await;
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
    let masked = |payload: &[u8]| {
        let mask = [0x11, 0x22, 0x33, 0x44];
        let mut out = vec![0x82];
        if payload.len() < 126 {
            out.push(0x80 | payload.len() as u8);
        } else {
            out.push(0x80 | 126);
            out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        out
    };
    let mut hello = Vec::new();
    let text = token(ALL);
    ClientFrame::Hello {
        ver: 1,
        token: &text,
    }
    .encode(&mut hello)
    .unwrap();
    let mut open = Vec::new();
    ClientFrame::Open {
        handle: 1,
        stream: "pieces",
    }
    .encode(&mut open)
    .unwrap();
    let mut bytes = masked(&hello);
    bytes.extend(masked(&open));
    let records = record(3, &[9u8; 700]);
    bytes.extend(masked(&publish_frame(1, 1, &records)));
    for chunk in bytes.chunks(61) {
        raw.write_all(chunk).await.unwrap();
        raw.flush().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let mut seen = Vec::new();
    let found = tokio::time::timeout(std::time::Duration::from_millis(WAIT_MS), async {
        loop {
            let read = raw.read(&mut buffer).await.unwrap();
            seen.extend_from_slice(&buffer[..read]);
            // Welcome is unmasked and small, and an ACK of 26 bytes follows it.
            if seen.contains(&proto::TYPE_ACK) && seen.len() > 40 {
                return;
            }
        }
    })
    .await;
    assert!(found.is_ok(), "no ACK");
    drop((ws, raw));
    assert_eq!(server.server.io_stats().held_buffer_count, 0);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_of_exactly_max_frame_round_trips_and_one_more_byte_is_error_6() {
    let records_bytes = 1024 * 1024;
    let max_frame_bytes = PUBLISH_HEAD_BYTES + records_bytes;
    let mut node = node_config(2);
    node.max_frame_bytes = max_frame_bytes as u64;
    let server = start_node(node);
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "big").await;

    let data_bytes = records_bytes - 32;
    let data: Vec<u8> = (0..data_bytes).map(|i| (i % 251) as u8).collect();
    let records = record(1, &data);
    let frame = publish_frame(1, 1, &records);
    assert_eq!(frame.len(), max_frame_bytes);
    ws.send_bytes(frame).await;
    let Owned::Ack { first_id, .. } = ws.recv_owned().await else {
        panic!("expected ACK");
    };

    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(1, "big", 0).await;
    let _ = reader.recv_owned().await;
    let Owned::Data { records: got, .. } = reader.recv_owned().await else {
        panic!("expected DATA");
    };
    assert_eq!(parse_records(&got), [(first_id, 1, AUTHOR, data)]);

    // One byte over the limit. It doesn't parse as a frame either, but size is what is said.
    ws.send_bytes(vec![0u8; max_frame_bytes + 1]).await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 0,
            code: ErrorCode::FrameTooLarge
        }
    );
    // The connection carries on, with nothing of it left behind.
    ws.round_trip(1).await;

    // A fragmented message over the limit is the same error, wherever it goes over.
    let half = vec![0u8; max_frame_bytes / 2 + 1];
    ws.socket
        .send(Message::Frame(Frame::message(
            half.clone(),
            OpCode::Data(Data::Binary),
            false,
        )))
        .await
        .unwrap();
    ws.socket
        .send(Message::Frame(Frame::message(
            half.clone(),
            OpCode::Data(Data::Continue),
            false,
        )))
        .await
        .unwrap();
    ws.socket
        .send(Message::Frame(Frame::message(
            half,
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 0,
            code: ErrorCode::FrameTooLarge
        }
    );
    ws.round_trip(2).await;
    drop((ws, reader));
    assert_eq!(server.server.io_stats().held_buffer_count, 0);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_1_2_is_refused_and_1_3_is_not() {
    let server = start();
    let attempt = async |versions: &[&'static rustls::SupportedProtocolVersion]| {
        let config = tls_client_config(&server.cert_der, versions);
        let connector = TlsConnector::from(Arc::new(config));
        let socket = tokio::net::TcpStream::connect(server.server.local_addr())
            .await
            .unwrap();
        connector
            .connect("localhost".try_into().unwrap(), socket)
            .await
    };
    let refused = attempt(&[&rustls::version::TLS12]).await;
    assert!(refused.is_err(), "TLS 1.2 connected");
    let accepted = attempt(&[&rustls::version::TLS13]).await.unwrap();
    assert_eq!(
        accepted.get_ref().1.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    // No client certificate was needed for that.
    drop(accepted);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_hello_ends_the_connection_with_error_1() {
    let server = start();
    for bad in [
        "not hex".to_owned(),
        // Signed with another secret.
        auth::Token {
            expiry_ms: START_MS + 1000,
            author: std::num::NonZeroU64::new(1).unwrap(),
            grants: Vec::new(),
        }
        .sign(b"another secret, not this server's")
        .unwrap(),
        // Expired.
        token_at(START_MS, 1, &[("*", ALL)]),
    ] {
        let mut ws = server.ws_raw().await;
        ws.send(&ClientFrame::Hello {
            ver: 1,
            token: &bad,
        })
        .await;
        assert_eq!(
            ws.recv_owned().await,
            Owned::Error {
                reference: 0,
                code: ErrorCode::Unauthorized
            }
        );
        assert_eq!(ws.recv_owned().await, Owned::Closed);
    }
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn anything_before_hello_is_error_1() {
    let server = start();
    let mut ws = server.ws_raw().await;
    ws.open(1, "x").await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 0,
            code: ErrorCode::Unauthorized
        }
    );
    assert_eq!(ws.recv_owned().await, Owned::Closed);
    // A frame too big for an unknown client is refused on its header alone, so only the
    // header is sent. The answer is an unmasked `ERROR` of code 6, then a close.
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
    let mut header = vec![0x82, 0x80 | 127];
    header.extend_from_slice(&(1024u64 * 1024).to_be_bytes());
    header.extend_from_slice(&[1, 2, 3, 4]);
    raw.write_all(&header).await.unwrap();
    raw.flush().await.unwrap();
    let mut seen = Vec::new();
    loop {
        match raw.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => seen.extend_from_slice(&buffer[..read]),
        }
    }
    // `0x82 len | type flags ref u32 | code u16`.
    assert_eq!(seen[2], proto::TYPE_ERROR);
    assert_eq!(u16::from_le_bytes([seen[8], seen[9]]), 6);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_protocol_version_is_refused() {
    let server = start();
    let mut ws = server.ws_raw().await;
    let text = token(ALL);
    ws.send(&ClientFrame::Hello {
        ver: 99,
        token: &text,
    })
    .await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 0,
            code: ErrorCode::Malformed
        }
    );
    server.server.shutdown();
}

/// The cores hold only cursor key hashes, so reserved and over-long names are refused here.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscribe_under_a_reserved_or_long_cursor_name_is_error_8() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    let long = "n".repeat(server::CURSOR_NAME_MAX_BYTES + 1);
    for (sub, name) in [(1, "sink:7"), (2, "compact:7d"), (3, long.as_str())] {
        ws.send(&ClientFrame::Subscribe(proto::Subscribe {
            sub,
            from: 0,
            linger_ms: 0,
            author: None,
            clauses: proto::WireClauses::default(),
            stream: "named",
            name: Some(name),
            with_state: false,
            state_only: false,
            include_system: false,
        }))
        .await;
        assert_eq!(
            ws.recv_owned().await,
            Owned::Error {
                reference: sub,
                code: ErrorCode::Malformed
            }
        );
    }
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_frame_is_error_8_and_the_connection_goes_on() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.send_bytes(vec![0x7F, 0]).await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 0,
            code: ErrorCode::Malformed
        }
    );
    ws.socket
        .send(Message::Text("no".to_owned()))
        .await
        .unwrap();
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 0,
            code: ErrorCode::Malformed
        }
    );
    // A publish on a handle nobody opened.
    ws.publish(9, 1, AckLevel::WalFsync, &record(0, b"x")).await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Nack {
            handle: 9,
            producer_seq: 1,
            code: ErrorCode::Malformed
        }
    );
    ws.round_trip(1).await;
    drop(ws);
    server.server.shutdown();
}

const PERMS: [u8; 4] = [
    auth::PERM_READ,
    auth::PERM_PUBLISH,
    auth::PERM_STATE,
    auth::PERM_ADMIN,
];

/// Whether a token with `perms` on the stream may do the operation, checked by what comes back.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Subscribe,
    Commit,
    Publish,
    PublishWithState,
    State,
}

impl Op {
    fn needs(self) -> u8 {
        match self {
            Op::Subscribe | Op::Commit => auth::PERM_READ,
            Op::Publish => auth::PERM_PUBLISH,
            Op::PublishWithState => auth::PERM_PUBLISH | auth::PERM_STATE,
            Op::State => auth::PERM_STATE,
        }
    }
}

/// `true` if allowed, `false` if refused with code 1. Anything else fails the test.
async fn try_op(server: &TestServer, op: Op, perms: u8, stream: &str, seq: u64) -> bool {
    let doc = cbor_map(&[("a", 1)]);
    match op {
        Op::Subscribe => {
            let mut ws = server.ws(&token(perms)).await;
            ws.subscribe(3, stream, 0).await;
            match ws.recv_owned().await {
                Owned::Subscribed { .. } => true,
                Owned::Error {
                    reference: 3,
                    code: ErrorCode::Unauthorized,
                } => false,
                other => panic!("{other:?}"),
            }
        }
        Op::Commit => {
            // Subscribed under a token that may, then a token that doesn't, on one connection.
            let mut ws = server.ws(&token(ALL)).await;
            ws.subscribe(3, stream, 0).await;
            let Owned::Subscribed { head, .. } = ws.recv_owned().await else {
                panic!("expected SUBSCRIBED");
            };
            let text = token(perms);
            ws.send(&ClientFrame::Hello {
                ver: 1,
                token: &text,
            })
            .await;
            ws.send(&ClientFrame::Commit { sub: 3, id: head }).await;
            ws.send(&ClientFrame::Ping { nonce: 1 }).await;
            let mut allowed = true;
            loop {
                match ws.recv_owned().await {
                    Owned::Pong { .. } => return allowed,
                    Owned::Error {
                        reference: 3,
                        code: ErrorCode::Unauthorized,
                    } => allowed = false,
                    // Past the token check: the core refuses it for having no cursor name, and
                    // that can come before or after the PONG.
                    Owned::Error {
                        reference: 3,
                        code: ErrorCode::Malformed,
                    } => {}
                    Owned::Error { code, .. } => panic!("{code:?}"),
                    _ => {}
                }
            }
        }
        Op::Publish | Op::PublishWithState => {
            let mut ws = server.ws(&token(perms)).await;
            ws.open(1, stream).await;
            let records = record(0, b"x");
            ws.send(&ClientFrame::Publish(proto::Publish {
                handle: 1,
                producer_id: 1000 + seq,
                producer_seq: seq,
                ack: AckLevel::WalFsync,
                state: (op == Op::PublishWithState).then_some(PublishState {
                    expect_ver: 0,
                    op: proto::StateOp::Merge,
                    doc: &doc,
                }),
                records: &records,
            }))
            .await;
            match ws.recv_owned().await {
                Owned::Ack { .. } => true,
                Owned::Nack {
                    code: ErrorCode::Unauthorized,
                    ..
                } => false,
                other => panic!("{other:?}"),
            }
        }
        Op::State => {
            let mut ws = server.ws(&token(perms)).await;
            ws.open(1, stream).await;
            ws.send(&ClientFrame::StateOp {
                handle: 1,
                req: 8,
                expect_ver: 0,
                op: proto::StateOp::Merge,
                doc: &doc,
            })
            .await;
            match ws.recv_owned().await {
                Owned::StateResult { ok: true, .. } => true,
                Owned::Error {
                    reference: 8,
                    code: ErrorCode::Unauthorized,
                } => false,
                other => panic!("{other:?}"),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_operation_passes_with_its_permission_and_fails_with_every_other() {
    let server = start();
    let stream = "perm.matrix";
    // The stream has to exist for a subscribe to pass.
    let mut seed = server.ws(&token(ALL)).await;
    seed.open(1, stream).await;
    seed.publish(1, 1, AckLevel::WalFsync, &record(0, b"seed"))
        .await;
    assert!(matches!(seed.recv_owned().await, Owned::Ack { .. }));
    let mut seq = 1;
    for op in [
        Op::Subscribe,
        Op::Commit,
        Op::Publish,
        Op::PublishWithState,
        Op::State,
    ] {
        // Every single permission, and the one that is enough.
        for perms in PERMS.into_iter().chain([op.needs()]) {
            seq += 1;
            let expected = perms & op.needs() == op.needs();
            let got = try_op(&server, op, perms, stream, seq).await;
            assert_eq!(got, expected, "{op:?} with perms {perms:#x}");
        }
        // All but what it needs.
        seq += 1;
        let others = ALL & !op.needs();
        assert!(
            !try_op(&server, op, others, stream, seq).await,
            "{op:?} with {others:#x}"
        );
    }
    drop(seed);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_publish_commits_nothing() {
    let server = start();
    let mut seed = server.ws(&token(ALL)).await;
    seed.open(1, "s").await;
    seed.publish(1, 1, AckLevel::WalFsync, &record(0, b"seed"))
        .await;
    let Owned::Ack { first_id, .. } = seed.recv_owned().await else {
        panic!();
    };
    assert!(!try_op(&server, Op::Publish, auth::PERM_READ, "s", 2).await);
    seed.subscribe(1, "s", 0).await;
    let Owned::Subscribed { head, .. } = seed.recv_owned().await else {
        panic!();
    };
    assert_eq!(head, first_id);
    drop(seed);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_for_another_pattern_fails_on_the_stream() {
    let server = start();
    let text = token_at(START_MS + 1000, AUTHOR, &[("logs.*", ALL)]);
    let mut ws = server.ws(&text).await;
    ws.open(1, "logs.a").await;
    ws.open(2, "metrics.a").await;
    ws.publish(1, 1, AckLevel::WalFsync, &record(0, b"yes"))
        .await;
    assert!(matches!(ws.recv_owned().await, Owned::Ack { .. }));
    ws.publish(2, 2, AckLevel::WalFsync, &record(0, b"no"))
        .await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Nack {
            handle: 2,
            producer_seq: 2,
            code: ErrorCode::Unauthorized
        }
    );
    ws.subscribe(1, "logsx", 0).await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 1,
            code: ErrorCode::Unauthorized
        }
    );
    drop(ws);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_expiring_mid_connection_fails_the_next_operation() {
    let server = start();
    let expiry_ms = START_MS + 1000;
    let text = token_at(expiry_ms, AUTHOR, &[("*", ALL)]);
    let mut ws = server.ws(&text).await;
    ws.open(1, "e").await;
    ws.publish(1, 1, AckLevel::WalFsync, &record(0, b"ok"))
        .await;
    assert!(matches!(ws.recv_owned().await, Owned::Ack { .. }));

    server.clock.set(expiry_ms);
    ws.publish(1, 2, AckLevel::WalFsync, &record(0, b"late"))
        .await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Nack {
            handle: 1,
            producer_seq: 2,
            code: ErrorCode::Unauthorized
        }
    );
    ws.subscribe(1, "e", 0).await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 1,
            code: ErrorCode::Unauthorized
        }
    );
    let doc = cbor_map(&[("a", 1)]);
    ws.send(&ClientFrame::StateOp {
        handle: 1,
        req: 4,
        expect_ver: 0,
        op: proto::StateOp::Merge,
        doc: &doc,
    })
    .await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 4,
            code: ErrorCode::Unauthorized
        }
    );
    // A new token on the same connection works, and the old limit is gone.
    let fresh = token_at(expiry_ms + 1000, AUTHOR, &[("*", ALL)]);
    ws.send(&ClientFrame::Hello {
        ver: 1,
        token: &fresh,
    })
    .await;
    assert!(matches!(ws.recv_owned().await, Owned::Welcome { .. }));
    ws.publish(1, 3, AckLevel::WalFsync, &record(0, b"again"))
        .await;
    assert!(matches!(ws.recv_owned().await, Owned::Ack { .. }));
    drop(ws);
    server.server.shutdown();
}

/// Revocation is by expiry (section 12), so a live subscription doesn't outlast its token.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_ends_when_its_token_expires() {
    let manual = Arc::new(server::ManualClock::new(START_MS));
    let server = start_with(node_config(1), Arc::new(SystemClock), manual);
    let now_ms = server::Clock::now_ms(&SystemClock);
    let far = token_at(now_ms + 3_600_000, AUTHOR, &[("*", ALL)]);
    let mut writer = server.ws(&far).await;
    writer.open(1, "live").await;
    writer
        .publish(1, 1, AckLevel::WalFsync, &record(0, b"a"))
        .await;
    assert!(matches!(writer.recv_owned().await, Owned::Ack { .. }));

    let short = token_at(now_ms + 400, AUTHOR, &[("*", auth::PERM_READ)]);
    let mut reader = server.ws(&short).await;
    reader.subscribe(1, "live", u64::MAX).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));
    let got = reader.recv_within(WAIT_MS).await;
    assert_eq!(
        got,
        Some(Owned::Error {
            reference: 1,
            code: ErrorCode::Unauthorized
        })
    );
    // And nothing published afterwards reaches it.
    writer
        .publish(1, 2, AckLevel::WalFsync, &record(0, b"b"))
        .await;
    assert!(matches!(writer.recv_owned().await, Owned::Ack { .. }));
    while let Some(frame) = reader.recv_within(200).await {
        assert!(!matches!(frame, Owned::Data { .. }), "{frame:?}");
    }
    drop((writer, reader));
    server.server.shutdown();
}

/// A client's own `author` and `id` are refused, and the token's is what gets stamped.
#[tokio::test(flavor = "multi_thread")]
async fn clients_cannot_set_author_or_id() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "who").await;
    for (id, author) in [(0u64, 5u64), (0, 0x1234_5678), (7, 0)] {
        let mut records = record(0, b"forged");
        records[0..8].copy_from_slice(&id.to_le_bytes());
        records[16..24].copy_from_slice(&author.to_le_bytes());
        ws.publish(1, 1, AckLevel::WalFsync, &records).await;
        assert_eq!(
            ws.recv_owned().await,
            Owned::Nack {
                handle: 1,
                producer_seq: 1,
                code: ErrorCode::Malformed
            },
            "id {id} author {author}"
        );
    }
    ws.publish(1, 1, AckLevel::WalFsync, &record(0, b"honest"))
        .await;
    assert!(matches!(ws.recv_owned().await, Owned::Ack { .. }));
    ws.subscribe(1, "who", 0).await;
    let _ = ws.recv_owned().await;
    let Owned::Data { records, .. } = ws.recv_owned().await else {
        panic!();
    };
    assert_eq!(parse_records(&records)[0].2, AUTHOR);
    drop(ws);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn every_frame_is_wired() {
    let server = start();
    let mut ws = server.ws(&token(ALL)).await;
    ws.open(1, "wired").await;

    // STATE_OP, with and without a conflict.
    let doc = cbor_map(&[("n", 1)]);
    ws.send(&ClientFrame::StateOp {
        handle: 1,
        req: 1,
        expect_ver: 0,
        op: proto::StateOp::Set,
        doc: &doc,
    })
    .await;
    let Owned::StateResult {
        req: 1,
        ok: true,
        ver,
    } = ws.recv_owned().await
    else {
        panic!();
    };
    ws.send(&ClientFrame::StateOp {
        handle: 1,
        req: 2,
        expect_ver: ver + 1,
        op: proto::StateOp::Set,
        doc: &doc,
    })
    .await;
    assert!(matches!(
        ws.recv_owned().await,
        Owned::StateResult {
            req: 2,
            ok: false,
            ..
        }
    ));

    // A publish with a state change: its ids, then the state record.
    let records = record(0, b"with state");
    let patch = cbor_map(&[("m", 2)]);
    ws.send(&ClientFrame::Publish(proto::Publish {
        handle: 1,
        producer_id: 1,
        producer_seq: 1,
        ack: AckLevel::WalFsync,
        state: Some(PublishState {
            expect_ver: ver,
            op: proto::StateOp::Merge,
            doc: &patch,
        }),
        records: &records,
    }))
    .await;
    let Owned::Ack {
        first_id, count, ..
    } = ws.recv_owned().await
    else {
        panic!();
    };
    assert_eq!(count, 2, "D5 counts the system record");

    // WITH_STATE, a named cursor and INCLUDE_SYSTEM together.
    ws.send(&ClientFrame::Subscribe(proto::Subscribe {
        sub: 2,
        from: 0,
        linger_ms: 0,
        author: None,
        clauses: proto::WireClauses::default(),
        stream: "wired",
        name: Some("c"),
        with_state: true,
        state_only: false,
        include_system: true,
    }))
    .await;
    let Owned::Subscribed {
        sub: 2,
        state_ver,
        state,
        ..
    } = ws.recv_owned().await
    else {
        panic!();
    };
    assert_eq!(state_ver, first_id + 1);
    assert!(!state.is_empty());
    let mut authors = Vec::new();
    while authors.len() < 3 {
        if let Owned::Data { records, .. } = ws.recv_owned().await {
            authors.extend(parse_records(&records).into_iter().map(|r| r.2));
        }
    }
    assert!(authors.contains(&0), "system records were asked for");
    assert!(authors.contains(&AUTHOR));

    // COMMIT above the head is an error, and at the head isn't.
    ws.send(&ClientFrame::Commit {
        sub: 2,
        id: first_id + 1,
    })
    .await;
    ws.round_trip(7).await;
    ws.send(&ClientFrame::Commit {
        sub: 2,
        id: first_id + 100,
    })
    .await;
    assert_eq!(
        ws.recv_owned().await,
        Owned::Error {
            reference: 2,
            code: ErrorCode::Malformed
        }
    );

    // STATE_ONLY and STATE.
    ws.send(&ClientFrame::Subscribe(proto::Subscribe {
        sub: 3,
        from: u64::MAX,
        linger_ms: 0,
        author: None,
        clauses: proto::WireClauses::default(),
        stream: "wired",
        name: None,
        with_state: false,
        state_only: true,
        include_system: false,
    }))
    .await;
    loop {
        if matches!(ws.recv_owned().await, Owned::Subscribed { sub: 3, .. }) {
            break;
        }
    }
    let doc = cbor_map(&[("k", 3)]);
    ws.send(&ClientFrame::StateOp {
        handle: 1,
        req: 3,
        expect_ver: 0,
        op: proto::StateOp::Merge,
        doc: &doc,
    })
    .await;
    let mut saw_state = false;
    let mut saw_result = false;
    while !(saw_state && saw_result) {
        match ws.recv_owned().await {
            Owned::State { sub: 3, .. } => saw_state = true,
            Owned::StateResult {
                req: 3, ok: true, ..
            } => saw_result = true,
            _ => {}
        }
    }

    // UNSUBSCRIBE ends it, and doing it twice is error 2.
    ws.send(&ClientFrame::Unsubscribe { sub: 3 }).await;
    ws.send(&ClientFrame::Unsubscribe { sub: 3 }).await;
    loop {
        match ws.recv_owned().await {
            Owned::Error {
                reference: 3,
                code: ErrorCode::NotFound,
            } => break,
            Owned::Closed => panic!("closed"),
            _ => {}
        }
    }
    drop(ws);
    server.server.shutdown();
}

/// A subscriber that doesn't read: the socket fills, the connection's window closes, and it
/// gets `BEHIND`, while a publisher on its own connection goes on unharmed.
#[tokio::test(flavor = "multi_thread")]
async fn a_socket_not_reading_falls_behind_and_catches_up() {
    let mut node = node_config(2);
    node.subscriber_queue_bytes = 256 * 1024;
    node.connection_window_bytes = 256 * 1024;
    let server = start_node(node);
    let mut writer = server.ws(&token(ALL)).await;
    writer.open(1, "slow").await;
    writer
        .publish(1, 1, AckLevel::WalFsync, &record(0, b"first"))
        .await;
    let Owned::Ack { first_id, .. } = writer.recv_owned().await else {
        panic!();
    };
    let mut reader = server.ws(&token(ALL)).await;
    reader.subscribe(1, "slow", first_id - 1).await;
    // Past what the socket's buffers and the bounds hold, with the reader not reading.
    let payload = vec![5u8; 256 * 1024];
    let records = record(0, &payload);
    let total: u64 = 64;
    for seq in 2..2 + total {
        writer.publish(1, seq, AckLevel::WalFsync, &records).await;
        assert!(matches!(writer.recv_owned().await, Owned::Ack { .. }));
    }
    let mut last_id = 0;
    let mut got = 0;
    let mut behind = 0;
    while got < total + 1 {
        match reader.recv_owned().await {
            Owned::Data { records, .. } => {
                for record in parse_records(&records) {
                    assert!(record.0 > last_id, "ids go up, with no repeats");
                    last_id = record.0;
                    got += 1;
                }
            }
            Owned::Behind { .. } => behind += 1,
            Owned::Closed => panic!("closed"),
            _ => {}
        }
    }
    assert!(behind >= 1, "it should have fallen behind");
    drop((writer, reader));
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unmasked_frame_ends_the_connection() {
    let server = start();
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
    raw.write_all(&[0x82, 0x01, 0xFF]).await.unwrap();
    raw.flush().await.unwrap();
    let mut seen = Vec::new();
    loop {
        match raw.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => seen.extend_from_slice(&buffer[..read]),
        }
    }
    assert!(
        !seen.is_empty(),
        "an ERROR and a close frame should come first"
    );
    server.server.shutdown();
}
