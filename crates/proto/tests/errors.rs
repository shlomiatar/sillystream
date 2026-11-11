//! Each way a frame can be wrong, and what the encoders refuse.

mod common;

use common::{MAX, load, one_clause, subscribe_with_flags};
use format::Clause;
use proto::{
    AckLevel, ClientFrame, ErrorCode, ProtoError, Publish, ServerFrame, StateOp, Subscribe,
    WireClause, WireClauses, decode_client, decode_server,
};

const SUBSCRIBE_NFILT_OFFSET: usize = 16;

fn client_err(bytes: &[u8]) -> ProtoError {
    decode_client(bytes, MAX).unwrap_err()
}

fn server_err(bytes: &[u8]) -> ProtoError {
    decode_server(bytes, MAX).unwrap_err()
}

#[test]
fn empty_and_one_byte_frames_are_truncated() {
    for bytes in [&[][..], &[0x01][..]] {
        assert_eq!(client_err(bytes), ProtoError::Truncated);
        assert_eq!(server_err(bytes), ProtoError::Truncated);
    }
}

#[test]
fn a_type_from_the_other_direction_is_unknown() {
    for frame_type in (0x00..=0xFFu8).filter(|t| !(0x01..=0x08).contains(t)) {
        assert_eq!(client_err(&[frame_type, 0, 0, 0]), ProtoError::UnknownType);
    }
    for frame_type in 0x00..=0xFFu8 {
        if !(0x81..=0x8A).contains(&frame_type) && frame_type != 0x8E && frame_type != 0x8F {
            assert_eq!(server_err(&[frame_type, 0, 0, 0]), ProtoError::UnknownType);
            assert_eq!(server_err(&[frame_type, 1, 0, 0]), ProtoError::UnknownType);
        }
    }
}

#[test]
fn undefined_flag_bits_are_rejected_on_every_frame() {
    // Bits above the 5 SUBSCRIBE defines, and above the 1 PUBLISH defines.
    for bit in [0x20u8, 0x40, 0x80] {
        let mut bytes = load("subscribe_00");
        bytes[1] |= bit;
        assert_eq!(client_err(&bytes), ProtoError::UnknownFlags);
    }
    for bit in [0x02u8, 0x10, 0x80] {
        let mut bytes = load("publish_00");
        bytes[1] |= bit;
        assert_eq!(client_err(&bytes), ProtoError::UnknownFlags);
    }
    for name in ["hello", "unsubscribe", "open", "state_op", "commit", "ping"] {
        let mut bytes = load(name);
        bytes[1] = 1;
        assert_eq!(client_err(&bytes), ProtoError::UnknownFlags, "{name}");
    }
    for name in [
        "welcome",
        "subscribed",
        "data",
        "ack",
        "state",
        "behind",
        "progress",
        "state_result_ok",
        "redirect",
        "nack",
        "error_1",
        "pong",
    ] {
        let mut bytes = load(name);
        bytes[1] = 1;
        assert_eq!(server_err(&bytes), ProtoError::UnknownFlags, "{name}");
    }
}

#[test]
fn more_than_four_clauses_is_rejected() {
    let mut bytes = load("subscribe_no_clauses");
    bytes[SUBSCRIBE_NFILT_OFFSET] = 5;
    assert_eq!(client_err(&bytes), ProtoError::TooManyClauses);
    bytes[SUBSCRIBE_NFILT_OFFSET] = 255;
    assert_eq!(client_err(&bytes), ProtoError::TooManyClauses);
}

#[test]
fn unknown_clause_op_is_rejected() {
    let mut bytes = load("subscribe_00");
    bytes[SUBSCRIBE_NFILT_OFFSET + 1 + 16] = 2;
    assert_eq!(client_err(&bytes), ProtoError::UnknownClauseOp);
}

#[test]
fn unknown_ack_level_is_rejected() {
    let mut bytes = load("publish_00");
    bytes[22] = 4;
    assert_eq!(client_err(&bytes), ProtoError::UnknownAckLevel);
    // Level 0 is retired (section 7.2).
    bytes[22] = 0;
    assert_eq!(client_err(&bytes), ProtoError::UnknownAckLevel);
}

#[test]
fn unknown_state_op_is_rejected() {
    for op in [0u8, 4, 255] {
        let mut bytes = load("publish_01");
        bytes[31] = op;
        assert_eq!(client_err(&bytes), ProtoError::UnknownStateOp);

        let mut bytes = load("state_op");
        bytes[18] = op;
        assert_eq!(client_err(&bytes), ProtoError::UnknownStateOp);
    }
}

#[test]
fn ok_must_be_0_or_1() {
    let mut bytes = load("state_result_ok");
    bytes[6] = 2;
    assert_eq!(server_err(&bytes), ProtoError::InvalidOk);
}

#[test]
fn unknown_error_codes_are_rejected() {
    for code in [0u16, 9, u16::MAX] {
        let mut bytes = load("error_1");
        bytes[6..8].copy_from_slice(&code.to_le_bytes());
        assert_eq!(server_err(&bytes), ProtoError::UnknownErrorCode);

        let mut bytes = load("nack");
        bytes[14..16].copy_from_slice(&code.to_le_bytes());
        assert_eq!(server_err(&bytes), ProtoError::UnknownErrorCode);
    }
}

#[test]
fn strings_must_be_utf8() {
    let mut bytes = load("hello");
    bytes[6] = 0xFF;
    assert_eq!(client_err(&bytes), ProtoError::InvalidUtf8);
    let mut bytes = load("nack");
    *bytes.last_mut().unwrap() = 0xFF;
    assert_eq!(server_err(&bytes), ProtoError::InvalidUtf8);
}

#[test]
fn stream_ids_can_not_be_empty_or_contain_a_slash() {
    let frame = |stream: &[u8]| {
        let mut bytes = vec![0x04, 0, 9, 0, 0, 0];
        bytes.extend_from_slice(&(stream.len() as u16).to_le_bytes());
        bytes.extend_from_slice(stream);
        bytes
    };
    assert!(decode_client(&frame(b"logs.a"), MAX).is_ok());
    assert_eq!(client_err(&frame(b"")), ProtoError::InvalidStreamId);
    assert_eq!(client_err(&frame(b"a/b")), ProtoError::InvalidStreamId);

    let mut out = Vec::new();
    let open = ClientFrame::Open {
        handle: 1,
        stream: "a/b",
    };
    assert_eq!(open.encode(&mut out), Err(ProtoError::InvalidStreamId));
    let redirect = ServerFrame::Redirect {
        stream: "",
        host: "h",
    };
    assert_eq!(redirect.encode(&mut out), Err(ProtoError::InvalidStreamId));

    // Each encoder that writes a stream id has to refuse what its decoder refuses.
    for stream in ["", "a/b"] {
        let subscribe = ClientFrame::Subscribe(Subscribe {
            stream,
            ..subscribe_with_flags(0, WireClauses::default())
        });
        assert_eq!(subscribe.encode(&mut out), Err(ProtoError::InvalidStreamId));
    }
}

#[test]
fn length_prefixes_past_the_end_are_truncated_not_panics() {
    let mut bytes = load("data");
    bytes[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(server_err(&bytes), ProtoError::Truncated);

    let mut bytes = load("publish_00");
    bytes[23..27].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(client_err(&bytes), ProtoError::Truncated);

    let mut bytes = load("hello");
    bytes[4..6].copy_from_slice(&u16::MAX.to_le_bytes());
    assert_eq!(client_err(&bytes), ProtoError::Truncated);
}

#[test]
fn a_frame_over_max_frame_is_too_large_and_maps_to_code_6() {
    let bytes = load("publish_00");
    assert!(decode_client(&bytes, bytes.len()).is_ok());
    let error = decode_client(&bytes, bytes.len() - 1).unwrap_err();
    assert_eq!(error, ProtoError::FrameTooLarge);
    assert_eq!(error.code(), ErrorCode::FrameTooLarge);

    let bytes = load("data");
    assert!(decode_server(&bytes, bytes.len()).is_ok());
    assert_eq!(
        decode_server(&bytes, bytes.len() - 1),
        Err(ProtoError::FrameTooLarge)
    );
    // The limit comes before anything else is looked at.
    assert_eq!(decode_client(&[0xFF; 3], 2), Err(ProtoError::FrameTooLarge));
    assert_eq!(ProtoError::Truncated.code(), ErrorCode::Malformed);
}

#[test]
fn a_str_at_the_limit_encodes_and_one_past_it_does_not() {
    let token = "x".repeat(65_535);
    let mut out = vec![0xAA];
    let frame = ClientFrame::Hello {
        ver: 1,
        token: &token,
    };
    frame.encode(&mut out).unwrap();
    assert_eq!(decode_client(&out[1..], MAX), Ok(frame));

    let token = "x".repeat(65_536);
    let mut out = vec![0xAA];
    let frame = ClientFrame::Hello {
        ver: 1,
        token: &token,
    };
    assert_eq!(frame.encode(&mut out), Err(ProtoError::StrTooLong));
    assert_eq!(out, [0xAA], "a failed encode leaves the output as it was");
}

/// `vec!` of zeros is lazily mapped, and the length is refused before any copy.
#[cfg(target_pointer_width = "64")]
#[test]
fn bytes_over_u32_are_refused_without_copying() {
    let records = vec![0u8; u32::MAX as usize + 1];
    let mut out = vec![0xAA];
    let publish = ClientFrame::Publish(Publish {
        handle: 1,
        producer_id: 1,
        producer_seq: 1,
        ack: AckLevel::Memory,
        state: None,
        records: &records,
    });
    assert_eq!(publish.encode(&mut out), Err(ProtoError::BytesTooLong));
    assert_eq!(out, [0xAA]);
    let data = ServerFrame::Data {
        sub: 1,
        records: &records,
    };
    assert_eq!(data.encode(&mut out), Err(ProtoError::BytesTooLong));
    assert_eq!(out, [0xAA]);
    assert_eq!(proto::data_prefix(1, records.len()), None);
}

#[test]
fn clause_lists_refuse_what_the_decoder_refuses() {
    let clause = WireClause::default();
    assert_eq!(
        WireClauses::new(&[clause; 5]),
        Err(ProtoError::TooManyClauses)
    );
    assert_eq!(
        WireClauses::new(&[WireClause { op: 2, ..clause }]),
        Err(ProtoError::UnknownClauseOp)
    );
}

#[test]
fn subscribe_builds_the_filter_it_asks_for() {
    let subscribe = subscribe_with_flags(0x18, one_clause());
    let filter = subscribe.filter().unwrap();
    assert_eq!(
        filter.clauses(),
        &[Clause::Eq {
            mask: 0x4142_4344_4546_4748,
            value: 0x5152_5354_5556_5758,
        }]
    );
    assert_eq!(filter.author, Some(0x3132_3334_3536_3738));
    assert!(filter.include_system);
    assert_eq!((filter.id_first, filter.id_last), (0, u64::MAX));

    let filter = subscribe_with_flags(0, WireClauses::default())
        .filter()
        .unwrap();
    assert!(filter.clauses().is_empty());
    assert_eq!(filter.author, None);
    assert!(!filter.include_system);
}

#[test]
fn state_op_codes_match_the_system_record_entry_types() {
    assert_eq!(StateOp::Set as u64, format::ENTRY_STATE_SET);
    assert_eq!(StateOp::Merge as u64, format::ENTRY_STATE_MERGE);
    assert_eq!(StateOp::Incr as u64, format::ENTRY_STATE_INCR);
}

/// The decoder never looks inside `records`: bytes that aren't valid records pass
/// through untouched. Validating them is the owner's job (section 3).
#[test]
fn records_are_opaque_to_the_codec() {
    let junk = [0xFFu8; 13];
    let mut out = Vec::new();
    let frame = ServerFrame::Data {
        sub: 1,
        records: &junk,
    };
    frame.encode(&mut out).unwrap();
    assert_eq!(decode_server(&out, MAX), Ok(frame));
}
