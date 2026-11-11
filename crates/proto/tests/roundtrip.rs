//! Property tests: every frame round-trips, and no input panics a decoder.

mod common;

use common::{MAX, client_vectors, load, record, server_vectors};
use proptest::prelude::*;
use proto::{
    AckLevel, ClientFrame, ErrorCode, Publish, PublishState, ServerFrame, StateOp, Subscribe,
    WireClause, WireClauses, decode_client, decode_server,
};

fn client_roundtrip(frame: ClientFrame<'_>) -> Result<(), TestCaseError> {
    let mut bytes = Vec::new();
    frame.encode(&mut bytes).unwrap();
    prop_assert_eq!(decode_client(&bytes, MAX), Ok(frame));
    Ok(())
}

fn server_roundtrip(frame: ServerFrame<'_>) -> Result<(), TestCaseError> {
    let mut bytes = Vec::new();
    frame.encode(&mut bytes).unwrap();
    prop_assert_eq!(decode_server(&bytes, MAX), Ok(frame));
    Ok(())
}

fn arb_stream() -> impl Strategy<Value = String> {
    "[^/]{1,40}"
}

fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..300)
}

fn arb_clauses() -> impl Strategy<Value = WireClauses> {
    proptest::collection::vec((any::<u64>(), any::<u64>(), 0..=1u8), 0..=4).prop_map(|clauses| {
        let clauses: Vec<WireClause> = clauses
            .into_iter()
            .map(|(mask, value, op)| WireClause { mask, value, op })
            .collect();
        WireClauses::new(&clauses).unwrap()
    })
}

fn arb_ack() -> impl Strategy<Value = AckLevel> {
    (1..=3u8).prop_map(|level| AckLevel::from_wire(level).unwrap())
}

fn arb_op() -> impl Strategy<Value = StateOp> {
    (1..=3u8).prop_map(|op| StateOp::from_wire(op).unwrap())
}

fn arb_code() -> impl Strategy<Value = ErrorCode> {
    (1..=8u16).prop_map(|code| ErrorCode::from_wire(code).unwrap())
}

/// Either decoder over `bytes`: must not panic, and anything accepted must re-encode
/// to the same bytes, since each frame has one encoding.
fn check_decoders(bytes: &[u8]) -> Result<(), TestCaseError> {
    if let Ok(frame) = decode_client(bytes, MAX) {
        let mut out = Vec::new();
        frame.encode(&mut out).unwrap();
        prop_assert_eq!(&out[..], bytes);
    }
    if let Ok(frame) = decode_server(bytes, MAX) {
        let mut out = Vec::new();
        frame.encode(&mut out).unwrap();
        prop_assert_eq!(&out[..], bytes);
    }
    Ok(())
}

fn vector_names() -> Vec<String> {
    let record = record();
    client_vectors(&record)
        .into_iter()
        .map(|(name, _)| name)
        .chain(server_vectors(&record).into_iter().map(|(name, _)| name))
        .collect()
}

proptest! {
    #[test]
    fn hello(ver: u16, token: String) {
        client_roundtrip(ClientFrame::Hello { ver, token: &token })?;
    }

    #[test]
    fn subscribe(
        sub: u32,
        from: u64,
        linger_ms: u16,
        author: Option<u64>,
        clauses in arb_clauses(),
        stream in arb_stream(),
        name: Option<String>,
        with_state: bool,
        state_only: bool,
        include_system: bool,
    ) {
        client_roundtrip(ClientFrame::Subscribe(Subscribe {
            sub,
            from,
            linger_ms,
            author,
            clauses,
            stream: &stream,
            name: name.as_deref(),
            with_state,
            state_only,
            include_system,
        }))?;
    }

    #[test]
    fn unsubscribe(sub: u32) {
        client_roundtrip(ClientFrame::Unsubscribe { sub })?;
    }

    #[test]
    fn open(handle: u32, stream in arb_stream()) {
        client_roundtrip(ClientFrame::Open { handle, stream: &stream })?;
    }

    #[test]
    fn publish(
        handle: u32,
        producer_id: u64,
        producer_seq: u64,
        ack in arb_ack(),
        state in proptest::option::of((any::<u64>(), arb_op(), arb_bytes())),
        records in arb_bytes(),
    ) {
        client_roundtrip(ClientFrame::Publish(Publish {
            handle,
            producer_id,
            producer_seq,
            ack,
            state: state.as_ref().map(|(expect_ver, op, doc)| PublishState {
                expect_ver: *expect_ver,
                op: *op,
                doc,
            }),
            records: &records,
        }))?;
    }

    #[test]
    fn state_op(handle: u32, req: u32, expect_ver: u64, op in arb_op(), doc in arb_bytes()) {
        client_roundtrip(ClientFrame::StateOp { handle, req, expect_ver, op, doc: &doc })?;
    }

    #[test]
    fn commit(sub: u32, id: u64) {
        client_roundtrip(ClientFrame::Commit { sub, id })?;
    }

    #[test]
    fn ping(nonce: u32) {
        client_roundtrip(ClientFrame::Ping { nonce })?;
    }

    #[test]
    fn welcome(session: u64, max_frame_bytes: u32, server_ms: u64) {
        server_roundtrip(ServerFrame::Welcome { session, max_frame_bytes, server_ms })?;
    }

    #[test]
    fn subscribed(sub: u32, head: u64, state_ver: u64, state in arb_bytes()) {
        server_roundtrip(ServerFrame::Subscribed { sub, head, state_ver, state: &state })?;
    }

    #[test]
    fn data(sub: u32, records in arb_bytes()) {
        server_roundtrip(ServerFrame::Data { sub, records: &records })?;
    }

    #[test]
    fn ack(handle: u32, producer_seq: u64, first_id: u64, count: u32) {
        server_roundtrip(ServerFrame::Ack { handle, producer_seq, first_id, count })?;
    }

    #[test]
    fn state(sub: u32, ver: u64, doc in arb_bytes()) {
        server_roundtrip(ServerFrame::State { sub, ver, doc: &doc })?;
    }

    #[test]
    fn behind(sub: u32, resume_from: u64) {
        server_roundtrip(ServerFrame::Behind { sub, resume_from })?;
    }

    #[test]
    fn progress(sub: u32, id: u64) {
        server_roundtrip(ServerFrame::Progress { sub, id })?;
    }

    #[test]
    fn state_result(req: u32, ok: bool, ver: u64) {
        server_roundtrip(ServerFrame::StateResult { req, ok, ver })?;
    }

    #[test]
    fn redirect(stream in arb_stream(), host: String) {
        server_roundtrip(ServerFrame::Redirect { stream: &stream, host: &host })?;
    }

    #[test]
    fn nack(handle: u32, producer_seq: u64, code in arb_code(), msg: String) {
        server_roundtrip(ServerFrame::Nack { handle, producer_seq, code, msg: &msg })?;
    }

    #[test]
    fn error(reference: u32, code in arb_code(), msg: String) {
        server_roundtrip(ServerFrame::Error { reference, code, msg: &msg })?;
    }

    #[test]
    fn pong(nonce: u32) {
        server_roundtrip(ServerFrame::Pong { nonce })?;
    }

    /// Plain random bytes almost always die on the type byte, so half the inputs
    /// start with a real type and no flags.
    #[test]
    fn random_bytes_never_panic(
        frame_type in prop_oneof![any::<u8>(), 0x01..=0x08u8, 0x81..=0x8Fu8],
        flags in prop_oneof![Just(0u8), any::<u8>()],
        body in proptest::collection::vec(any::<u8>(), 0..300),
    ) {
        let mut bytes = vec![frame_type, flags];
        bytes.extend_from_slice(&body);
        check_decoders(&bytes)?;
    }

    #[test]
    fn mutated_vectors_never_panic(
        index in any::<prop::sample::Index>(),
        changes in proptest::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
        keep in any::<prop::sample::Index>(),
    ) {
        let names = vector_names();
        let mut bytes = load(index.get(&names));
        for (at, value) in changes {
            let at = at.index(bytes.len());
            bytes[at] = value;
        }
        bytes.truncate(keep.index(bytes.len() + 1));
        check_decoders(&bytes)?;
    }
}

/// Guards the tests above against testing nothing: single-bit flips over every
/// vector must reach both accepted frames and each kind of rejection.
#[test]
fn bit_flips_reach_accepted_frames_and_rejections() {
    use std::collections::BTreeSet;
    let mut accepted_count = 0u32;
    let mut rejections = BTreeSet::new();
    for name in vector_names() {
        let original = load(&name);
        for byte in 0..original.len() {
            for bit in 0..8 {
                let mut bytes = original.clone();
                bytes[byte] ^= 1 << bit;
                let (client, server) = (decode_client(&bytes, MAX), decode_server(&bytes, MAX));
                if client.is_ok() || server.is_ok() {
                    accepted_count += 1;
                }
                if let Err(error) = client {
                    rejections.insert(error.to_string());
                }
                if let Err(error) = server {
                    rejections.insert(error.to_string());
                }
                check_decoders(&bytes).unwrap();
            }
        }
    }
    assert!(accepted_count > 1000, "accepted {accepted_count}");
    for reached in [
        "frame is truncated",
        "frame has trailing bytes",
        "unknown frame type",
        "unknown flag bits",
        "string is not UTF-8",
        "stream id is empty or contains '/'",
        "filter has more than 4 clauses",
        "unknown filter clause op",
        "unknown ack level",
        "unknown state op",
        "ok is not 0 or 1",
        "unknown error code",
    ] {
        assert!(rejections.contains(reached), "never saw: {reached}");
    }
}
