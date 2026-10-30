use format::{
    FormatError, RecordHeader, Records, SystemRecord, decode_record, encode_record,
    encode_system_record, padded_data_bytes, validate_forward, validate_publish,
};
use proptest::prelude::*;

fn arb_record() -> impl Strategy<Value = (RecordHeader, Vec<u8>)> {
    (
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        1..=u16::MAX,
        any::<u16>(),
        proptest::collection::vec(any::<u8>(), 0..200),
    )
        .prop_map(|(id, options, author, count, codec, data)| {
            let len = data.len() as u32;
            (
                RecordHeader {
                    id,
                    options,
                    author,
                    len,
                    count,
                    codec,
                },
                data,
            )
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn header_round_trip(id: u64, options: u64, author: u64, len: u32, count: u16, codec: u16) {
        let header = RecordHeader { id, options, author, len, count, codec };
        prop_assert_eq!(RecordHeader::read(&header.to_bytes()).unwrap(), header);
    }

    #[test]
    fn records_round_trip(records in proptest::collection::vec(arb_record(), 0..20)) {
        let mut bytes = Vec::new();
        let mut offsets = Vec::new();
        for (header, data) in &records {
            offsets.push(bytes.len());
            encode_record(&mut bytes, header, data).unwrap();
            prop_assert_eq!(bytes.len() % 8, 0);
        }
        let decoded: Vec<_> = Records::new(&bytes).collect::<Result<_, _>>().unwrap();
        prop_assert_eq!(decoded.len(), records.len());
        for (view, (header, data)) in decoded.iter().zip(&records) {
            prop_assert_eq!(&view.header, header);
            prop_assert_eq!(view.data, data.as_slice());
        }
        // Re-encoding what was decoded gives the same bytes.
        let mut again = Vec::new();
        for view in &decoded {
            encode_record(&mut again, &view.header, view.data).unwrap();
        }
        prop_assert_eq!(again, bytes);
    }

    #[test]
    fn system_record_round_trip(
        id: u64,
        entry_type: u64,
        actor: u64,
        doc in proptest::collection::vec(any::<u8>(), 0..100),
    ) {
        let mut bytes = Vec::new();
        encode_system_record(&mut bytes, id, entry_type, actor, &doc).unwrap();
        let (view, total_bytes) = decode_record(&bytes).unwrap();
        prop_assert_eq!(total_bytes, bytes.len());
        let system = SystemRecord::parse(&view).unwrap();
        prop_assert_eq!(system.entry_type, entry_type);
        prop_assert_eq!(system.actor, actor);
        prop_assert_eq!(system.doc, doc.as_slice());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    /// Every entry point returns normally on arbitrary bytes.
    #[test]
    fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..400)) {
        let _ = RecordHeader::read(&bytes);
        if let Ok((view, total_bytes)) = decode_record(&bytes) {
            prop_assert!(total_bytes <= bytes.len());
            prop_assert_eq!(total_bytes, view.header.total_bytes());
            let _ = SystemRecord::parse(&view);
        }
        for record in Records::new(&bytes) {
            let _ = record;
        }
        let _ = validate_publish(&mut bytes.clone()).map(|publish| publish.item_count());
        let _ = validate_forward(&mut bytes.clone()).map(|forward| forward.item_count());
    }

    /// Headers with huge `len` values and small bodies, which random bytes rarely hit.
    #[test]
    fn hostile_len_never_panics(
        len: u32,
        count: u16,
        body in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        let header = RecordHeader { id: 0, options: 0, author: 0, len, count, codec: 0 };
        let mut bytes = header.to_bytes().to_vec();
        bytes.extend_from_slice(&body);
        if let Ok((view, total_bytes)) = decode_record(&bytes) {
            prop_assert_eq!(view.data.len(), len as usize);
            prop_assert!(total_bytes <= bytes.len());
        }
        let _ = validate_publish(&mut bytes).map(|publish| publish.item_count());
    }

    /// Any truncation of valid bytes yields the whole records before the cut, then an error.
    #[test]
    fn truncation_yields_the_valid_prefix(
        records in proptest::collection::vec(arb_record(), 1..6),
        cut_fraction in 0.0f64..1.0,
    ) {
        let mut bytes = Vec::new();
        let mut ends = Vec::new();
        for (header, data) in &records {
            encode_record(&mut bytes, header, data).unwrap();
            ends.push(bytes.len());
        }
        let cut = (bytes.len() as f64 * cut_fraction) as usize;
        let whole_records = ends.iter().filter(|end| **end <= cut).count();
        let decoded: Vec<_> = Records::new(&bytes[..cut]).collect();
        let ok_count = decoded.iter().filter(|record| record.is_ok()).count();
        prop_assert_eq!(ok_count, whole_records);
        let on_boundary = cut == 0 || ends.contains(&cut);
        prop_assert_eq!(decoded.len(), if on_boundary { ok_count } else { ok_count + 1 });
    }
}

#[test]
fn padding_rule() {
    assert_eq!(padded_data_bytes(0), 0);
    assert_eq!(padded_data_bytes(1), 8);
    assert_eq!(padded_data_bytes(8), 8);
    assert_eq!(padded_data_bytes(9), 16);
    assert_eq!(padded_data_bytes(u32::MAX), 4_294_967_296);
}

#[test]
fn encoder_refuses_inconsistent_headers() {
    let header = RecordHeader {
        id: 0,
        options: 0,
        author: 1,
        len: 2,
        count: 1,
        codec: 0,
    };
    let mut out = Vec::new();
    assert_eq!(
        encode_record(&mut out, &header, b"abc"),
        Err(FormatError::LengthMismatch)
    );
    let zero = RecordHeader {
        count: 0,
        len: 3,
        ..header
    };
    assert_eq!(
        encode_record(&mut out, &zero, b"abc"),
        Err(FormatError::ZeroCount)
    );
    assert!(out.is_empty());
}

#[test]
fn system_parse_refuses_other_authors_and_short_data() {
    let mut bytes = Vec::new();
    let header = RecordHeader {
        id: 1,
        options: 1,
        author: 5,
        len: 8,
        count: 1,
        codec: 0,
    };
    encode_record(&mut bytes, &header, &[0; 8]).unwrap();
    let (view, _) = decode_record(&bytes).unwrap();
    assert_eq!(
        SystemRecord::parse(&view),
        Err(FormatError::NotSystemRecord)
    );

    let mut bytes = Vec::new();
    let header = RecordHeader {
        id: 1,
        options: 1,
        author: 0,
        len: 7,
        count: 1,
        codec: 0,
    };
    encode_record(&mut bytes, &header, &[0; 7]).unwrap();
    let (view, _) = decode_record(&bytes).unwrap();
    assert_eq!(
        SystemRecord::parse(&view),
        Err(FormatError::TruncatedSystemRecord)
    );
}
