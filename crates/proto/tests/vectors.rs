//! Byte-exact vectors. The `.hex` files come from `tests/vectors/generate.py`, which
//! shares no code with this crate, so these check the code against SPEC section
//! 11.1 and not against itself.

mod common;

use common::{MAX, client_vectors, load, record, server_vectors, vector_dir};
use proto::{ErrorCode, data_prefix, decode_client, decode_server};
use std::collections::BTreeSet;

#[test]
fn every_client_vector_decodes_and_encodes_byte_exact() {
    let record = record();
    for (name, frame) in client_vectors(&record) {
        let bytes = load(&name);
        assert_eq!(decode_client(&bytes, MAX), Ok(frame), "decode {name}");
        let mut out = Vec::new();
        frame.encode(&mut out).unwrap();
        assert_eq!(out, bytes, "encode {name}");
    }
}

#[test]
fn every_server_vector_decodes_and_encodes_byte_exact() {
    let record = record();
    for (name, frame) in server_vectors(&record) {
        let bytes = load(&name);
        assert_eq!(decode_server(&bytes, MAX), Ok(frame), "decode {name}");
        let mut out = Vec::new();
        frame.encode(&mut out).unwrap();
        assert_eq!(out, bytes, "encode {name}");
    }
}

/// A file no test reads would look like coverage without being any.
#[test]
fn every_vector_file_is_checked_and_every_frame_type_has_one() {
    let record = record();
    let client = client_vectors(&record);
    let server = server_vectors(&record);
    let expected: BTreeSet<String> = client
        .iter()
        .map(|(name, _)| name.clone())
        .chain(server.iter().map(|(name, _)| name.clone()))
        .collect();
    let on_disk: BTreeSet<String> = std::fs::read_dir(vector_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "hex"))
        .map(|path| path.file_stem().unwrap().to_str().unwrap().to_string())
        .collect();
    assert_eq!(on_disk, expected);

    let client_types: BTreeSet<u8> = client.iter().map(|(name, _)| load(name)[0]).collect();
    assert_eq!(client_types, (0x01..=0x08).collect());
    let server_types: BTreeSet<u8> = server.iter().map(|(name, _)| load(name)[0]).collect();
    assert_eq!(
        server_types,
        (0x81..=0x8A).chain([0x8E, 0x8F]).collect::<BTreeSet<u8>>()
    );
}

/// All 32 flag combinations, with the optional fields where section 11.1 puts them.
#[test]
fn subscribe_flags_select_the_optional_fields() {
    let without_optional = load("subscribe_00").len();
    for flags in 0..32u8 {
        let bytes = load(&format!("subscribe_{flags:02x}"));
        assert_eq!(bytes[1], flags);
        let mut expected = without_optional;
        if flags & 0x10 != 0 {
            expected += 8;
            // author sits right after sub, from and linger_ms.
            assert_eq!(
                u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                0x3132_3334_3536_3738
            );
        }
        if flags & 0x04 != 0 {
            expected += 4;
            assert_eq!(&bytes[bytes.len() - 4..], &[2, 0, b'c', b'1']);
        }
        assert_eq!(bytes.len(), expected, "flags {flags:#x}");
    }
}

#[test]
fn publish_state_sits_between_ack_and_records() {
    let plain = load("publish_00");
    let with_state = load("publish_01");
    assert_eq!((plain[1], with_state[1]), (0, 1));
    assert_eq!(with_state.len(), plain.len() + 8 + 1 + 4 + 4);
    assert_eq!(plain[..2 + 4 + 8 + 8 + 1][2..], with_state[..23][2..]);
    assert_eq!(
        u64::from_le_bytes(with_state[23..31].try_into().unwrap()),
        99
    );
    assert_eq!(with_state[31], 2);
    assert_eq!(plain[23..], with_state[40..]);
}

#[test]
fn every_strict_prefix_is_an_error_and_so_is_an_extra_byte() {
    let record = record();
    for (name, _) in client_vectors(&record) {
        let bytes = load(&name);
        for end in 0..bytes.len() {
            assert!(
                decode_client(&bytes[..end], MAX).is_err(),
                "{name} at {end}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(
            decode_client(&longer, MAX).is_err(),
            "{name} with extra byte"
        );
    }
    for (name, _) in server_vectors(&record) {
        let bytes = load(&name);
        for end in 0..bytes.len() {
            assert!(
                decode_server(&bytes[..end], MAX).is_err(),
                "{name} at {end}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(
            decode_server(&longer, MAX).is_err(),
            "{name} with extra byte"
        );
    }
}

#[test]
fn error_codes_have_the_numbers_in_section_11_1() {
    let codes = [
        (1, ErrorCode::Unauthorized),
        (2, ErrorCode::NotFound),
        (3, ErrorCode::VersionConflict),
        (4, ErrorCode::RateLimited),
        (5, ErrorCode::TooManySubscriptions),
        (6, ErrorCode::FrameTooLarge),
        (7, ErrorCode::RetentionExpired),
        (8, ErrorCode::Malformed),
    ];
    for (number, code) in codes {
        assert_eq!(code.to_wire(), number);
        assert_eq!(ErrorCode::from_wire(number), Some(code));
    }
    assert_eq!(ErrorCode::from_wire(0), None);
    assert_eq!(ErrorCode::from_wire(9), None);
}

#[test]
fn data_prefix_is_the_start_of_the_data_vector() {
    let record = record();
    let bytes = load("data");
    let prefix = data_prefix(7, record.len()).unwrap();
    assert_eq!(&bytes[..prefix.len()], &prefix);
    assert_eq!(&bytes[prefix.len()..], &record[..]);
}

/// The owner finds the bytes to stamp this way (see `Publish`).
#[test]
fn publish_records_end_the_frame() {
    for name in ["publish_00", "publish_01"] {
        let bytes = load(name);
        let proto::ClientFrame::Publish(publish) = decode_client(&bytes, MAX).unwrap() else {
            panic!("not a publish");
        };
        assert_eq!(
            &bytes[bytes.len() - publish.records.len()..],
            publish.records
        );
        assert_eq!(publish.records, &record()[..]);
    }
}
