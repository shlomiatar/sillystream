//! Byte-exact vectors. The `.hex` files are written by hand, so they check the
//! code against the layout in SPEC section 2 and not against itself.

use std::num::NonZeroU64;

use format::{
    ENTRY_STATE_SET, RecordHeader, Records, SystemRecord, decode_record, encode_record,
    encode_system_record, validate_publish,
};

fn parse_hex(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        for token in line.split_whitespace().filter(|token| *token != "|") {
            bytes.push(u8::from_str_radix(token, 16).expect("hex byte"));
        }
    }
    bytes
}

/// Reads each field at its section 2 offset without going through the decoder.
fn assert_fields_at_offsets(bytes: &[u8], header: &RecordHeader) {
    assert_eq!(
        u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        header.id
    );
    assert_eq!(
        u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        header.options
    );
    assert_eq!(
        u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        header.author
    );
    assert_eq!(
        u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
        header.len
    );
    assert_eq!(
        u16::from_le_bytes(bytes[28..30].try_into().unwrap()),
        header.count
    );
    assert_eq!(
        u16::from_le_bytes(bytes[30..32].try_into().unwrap()),
        header.codec
    );
}

fn check_vector(text: &str, header: RecordHeader, data: &[u8], expected_total_bytes: usize) {
    let bytes = parse_hex(text);
    assert_eq!(bytes.len(), expected_total_bytes);
    assert_eq!(bytes.len() % 8, 0, "records are padded to 8");
    assert_fields_at_offsets(&bytes, &header);

    let (view, total_bytes) = decode_record(&bytes).unwrap();
    assert_eq!(view.header, header);
    assert_eq!(view.data, data);
    assert_eq!(total_bytes, expected_total_bytes);

    let mut encoded = Vec::new();
    encode_record(&mut encoded, &header, data).unwrap();
    assert_eq!(encoded, bytes);
}

#[test]
fn single_item() {
    check_vector(
        include_str!("vectors/single_item.hex"),
        RecordHeader {
            id: 0x0123_4567_89AB_CDEF,
            options: 0x8000_0000_0000_0001,
            author: 0x1122_3344_5566_7788,
            len: 8,
            count: 1,
            codec: 0,
        },
        b"sillystr",
        40,
    );
}

#[test]
fn multi_item() {
    let data: Vec<u8> = (0..16).collect();
    check_vector(
        include_str!("vectors/multi_item.hex"),
        RecordHeader {
            id: 0x0000_0190_0000_0005,
            options: 0xDEAD_BEEF,
            author: 7,
            len: 16,
            count: 3,
            codec: 1,
        },
        &data,
        48,
    );
}

#[test]
fn max_count() {
    check_vector(
        include_str!("vectors/max_count.hex"),
        RecordHeader {
            id: 0x0000_0190_0001_0000,
            options: 0,
            author: 1,
            len: 0,
            count: 65_535,
            codec: 0,
        },
        b"",
        32,
    );
}

#[test]
fn padded() {
    let text = include_str!("vectors/padded.hex");
    check_vector(
        text,
        RecordHeader {
            id: 2,
            options: 0x0102_0304_0506_0708,
            author: 9,
            len: 5,
            count: 1,
            codec: 0,
        },
        b"hello",
        40,
    );
    let bytes = parse_hex(text);
    assert_eq!(&bytes[37..40], &[0, 0, 0]);
}

#[test]
fn system_record() {
    let text = include_str!("vectors/system.hex");
    let mut data = 42u64.to_le_bytes().to_vec();
    data.extend_from_slice(&[0xA1, 0x61, 0x61, 0x01]);
    let header = RecordHeader {
        id: 0x0000_0190_0000_0100,
        options: ENTRY_STATE_SET,
        author: 0,
        len: 12,
        count: 1,
        codec: 0,
    };
    check_vector(text, header, &data, 48);

    let bytes = parse_hex(text);
    let (view, _) = decode_record(&bytes).unwrap();
    let system = SystemRecord::parse(&view).unwrap();
    assert_eq!(system.entry_type, ENTRY_STATE_SET);
    assert_eq!(system.actor, 42);
    assert_eq!(system.doc, &[0xA1, 0x61, 0x61, 0x01]);

    let mut encoded = Vec::new();
    encode_system_record(&mut encoded, header.id, ENTRY_STATE_SET, 42, system.doc).unwrap();
    assert_eq!(encoded, bytes);
}

#[test]
fn stamping_changes_only_id_and_author() {
    let mut bytes = parse_hex(include_str!("vectors/publish_unstamped.hex"));
    let expected = parse_hex(include_str!("vectors/publish_stamped.hex"));

    let mut publish = validate_publish(&mut bytes).unwrap();
    assert_eq!(publish.record_count(), 2);
    assert_eq!(publish.item_count(), 3);
    publish.stamp(0x0000_0190_0000_0010, NonZeroU64::new(0xAB).unwrap());
    assert_eq!(bytes, expected);

    let ids: Vec<u64> = Records::new(&bytes)
        .map(|record| record.unwrap().header.id)
        .collect();
    assert_eq!(ids, [0x0000_0190_0000_0010, 0x0000_0190_0000_0012]);
}

#[test]
fn a_manifest_doc_is_its_size_then_its_path() {
    let doc = format::encode_manifest_doc(0x0102, "sealed/1/n/x.seg");
    let mut want = vec![2, 1, 0, 0, 0, 0, 0, 0];
    want.extend_from_slice(b"sealed/1/n/x.seg");
    assert_eq!(doc, want);
    assert_eq!(
        format::decode_manifest_doc(&doc).unwrap(),
        (0x0102, "sealed/1/n/x.seg")
    );
    assert!(format::decode_manifest_doc(&doc[..7]).is_err());
    assert!(format::decode_manifest_doc(&[0, 0, 0, 0, 0, 0, 0, 0, 0xff]).is_err());
}
