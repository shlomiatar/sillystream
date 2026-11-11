//! Loading the `.hex` vectors, and the frames they should decode to.
#![allow(dead_code)]

use proto::{
    AckLevel, ClientFrame, ErrorCode, FLAG_HAS_AUTHOR, FLAG_INCLUDE_SYSTEM, FLAG_NAMED_CURSOR,
    FLAG_STATE_ONLY, FLAG_WITH_STATE, Publish, PublishState, ServerFrame, StateOp, Subscribe,
    WireClause, WireClauses,
};
use std::path::PathBuf;

pub const MAX: usize = proto::DEFAULT_MAX_FRAME_BYTES;

pub fn vector_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/vectors")
}

pub fn load(name: &str) -> Vec<u8> {
    let path = vector_dir().join(format!("{name}.hex"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    let mut bytes = Vec::new();
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        for token in line.split_whitespace() {
            bytes.push(u8::from_str_radix(token, 16).expect("hex byte"));
        }
    }
    bytes
}

/// The record the vectors carry: options 3, count 1, "hello" and 3 padding bytes.
pub fn record() -> Vec<u8> {
    let mut bytes = vec![0u8; 40];
    bytes[8] = 3;
    bytes[24] = 5;
    bytes[28] = 1;
    bytes[32..37].copy_from_slice(b"hello");
    bytes
}

pub const DOC: &[u8] = &[0xA1, 0x61, 0x61, 0x01];

pub fn subscribe_with_flags(flags: u8, clauses: WireClauses) -> Subscribe<'static> {
    Subscribe {
        sub: 0x0102_0304,
        from: 0x1112_1314_1516_1718,
        linger_ms: 0x2122,
        author: (flags & FLAG_HAS_AUTHOR != 0).then_some(0x3132_3334_3536_3738),
        clauses,
        stream: "logs.a",
        name: (flags & FLAG_NAMED_CURSOR != 0).then_some("c1"),
        with_state: flags & FLAG_WITH_STATE != 0,
        state_only: flags & FLAG_STATE_ONLY != 0,
        include_system: flags & FLAG_INCLUDE_SYSTEM != 0,
    }
}

pub fn one_clause() -> WireClauses {
    WireClauses::new(&[WireClause {
        mask: 0x4142_4344_4546_4748,
        value: 0x5152_5354_5556_5758,
        op: 0,
    }])
    .unwrap()
}

fn publish<'a>(
    ack: AckLevel,
    state: Option<PublishState<'a>>,
    records: &'a [u8],
) -> ClientFrame<'a> {
    ClientFrame::Publish(Publish {
        handle: 5,
        producer_id: 0x0102_0304_0506_0708,
        producer_seq: 42,
        ack,
        state,
        records,
    })
}

fn state(op: StateOp) -> Option<PublishState<'static>> {
    Some(PublishState {
        expect_ver: 99,
        op,
        doc: DOC,
    })
}

/// Every client vector with the frame it decodes to.
pub fn client_vectors(record: &[u8]) -> Vec<(String, ClientFrame<'_>)> {
    let mut vectors = vec![
        (
            "hello".to_string(),
            ClientFrame::Hello {
                ver: 1,
                token: "tok",
            },
        ),
        (
            "subscribe_no_clauses".to_string(),
            ClientFrame::Subscribe(subscribe_with_flags(0, WireClauses::default())),
        ),
        (
            "subscribe_four_clauses".to_string(),
            ClientFrame::Subscribe(subscribe_with_flags(
                0,
                WireClauses::new(&[
                    WireClause {
                        mask: 0xFF,
                        value: 1,
                        op: 0,
                    },
                    WireClause {
                        mask: 0xFF00,
                        value: 0,
                        op: 1,
                    },
                    WireClause {
                        mask: 0xFF_0000,
                        value: 7,
                        op: 1,
                    },
                    WireClause {
                        mask: u64::MAX,
                        value: u64::MAX,
                        op: 0,
                    },
                ])
                .unwrap(),
            )),
        ),
        (
            "unsubscribe".to_string(),
            ClientFrame::Unsubscribe { sub: 7 },
        ),
        (
            "open".to_string(),
            ClientFrame::Open {
                handle: 9,
                stream: "inbox.reviewer",
            },
        ),
        (
            "publish_00".to_string(),
            publish(AckLevel::WalFsync, None, record),
        ),
        (
            "publish_01".to_string(),
            publish(AckLevel::WalFsync, state(StateOp::Merge), record),
        ),
        (
            "publish_ack1".to_string(),
            publish(AckLevel::Memory, None, record),
        ),
        (
            "publish_ack3".to_string(),
            publish(AckLevel::SegmentInS3, None, record),
        ),
        (
            "publish_state_no_records".to_string(),
            publish(AckLevel::WalFsync, state(StateOp::Merge), &[]),
        ),
        (
            "publish_state_set".to_string(),
            publish(AckLevel::WalFsync, state(StateOp::Set), record),
        ),
        (
            "publish_state_incr".to_string(),
            publish(AckLevel::WalFsync, state(StateOp::Incr), record),
        ),
        (
            "state_op".to_string(),
            ClientFrame::StateOp {
                handle: 5,
                req: 6,
                expect_ver: 99,
                op: StateOp::Set,
                doc: DOC,
            },
        ),
        (
            "commit".to_string(),
            ClientFrame::Commit {
                sub: 7,
                id: 0x0102_0304_0506_0708,
            },
        ),
        ("ping".to_string(), ClientFrame::Ping { nonce: 0xDEAD_BEEF }),
    ];
    for flags in 0..32u8 {
        vectors.push((
            format!("subscribe_{flags:02x}"),
            ClientFrame::Subscribe(subscribe_with_flags(flags, one_clause())),
        ));
    }
    vectors
}

/// Every server vector with the frame it decodes to.
pub fn server_vectors(record: &[u8]) -> Vec<(String, ServerFrame<'_>)> {
    let mut vectors = vec![
        (
            "welcome",
            ServerFrame::Welcome {
                session: 0x0102_0304_0506_0708,
                max_frame_bytes: 16 << 20,
                server_ms: 1_800_000_000_000,
            },
        ),
        (
            "subscribed",
            ServerFrame::Subscribed {
                sub: 7,
                head: 1000,
                state_ver: 900,
                state: DOC,
            },
        ),
        (
            "subscribed_no_state",
            ServerFrame::Subscribed {
                sub: 7,
                head: 1000,
                state_ver: 0,
                state: &[],
            },
        ),
        (
            "data",
            ServerFrame::Data {
                sub: 7,
                records: record,
            },
        ),
        (
            "ack",
            ServerFrame::Ack {
                handle: 5,
                producer_seq: 42,
                first_id: 1000,
                count: 3,
            },
        ),
        (
            "ack_already_committed",
            ServerFrame::Ack {
                handle: 5,
                producer_seq: 41,
                first_id: 0,
                count: 0,
            },
        ),
        (
            "state",
            ServerFrame::State {
                sub: 7,
                ver: 900,
                doc: DOC,
            },
        ),
        (
            "behind",
            ServerFrame::Behind {
                sub: 7,
                resume_from: 1000,
            },
        ),
        ("progress", ServerFrame::Progress { sub: 7, id: 1000 }),
        (
            "state_result_ok",
            ServerFrame::StateResult {
                req: 6,
                ok: true,
                ver: 900,
            },
        ),
        (
            "state_result_conflict",
            ServerFrame::StateResult {
                req: 6,
                ok: false,
                ver: 901,
            },
        ),
        (
            "redirect",
            ServerFrame::Redirect {
                stream: "logs.a",
                host: "node2:443",
            },
        ),
        (
            "nack",
            ServerFrame::Nack {
                handle: 5,
                producer_seq: 42,
                code: ErrorCode::VersionConflict,
                msg: "conflict",
            },
        ),
        ("pong", ServerFrame::Pong { nonce: 0xDEAD_BEEF }),
    ]
    .into_iter()
    .map(|(name, frame)| (name.to_string(), frame))
    .collect::<Vec<_>>();
    for code in 1..=8u16 {
        vectors.push((
            format!("error_{code}"),
            ServerFrame::Error {
                reference: 11,
                code: ErrorCode::from_wire(code).unwrap(),
                msg: "no",
            },
        ));
    }
    vectors
}
