use format::{
    COUNT_OFFSET, FormatError, RecordHeader, Records, SystemRecord, decode_record, encode_record,
    encode_system_record, padded_data_bytes, validate_forward, validate_publish,
};
use proptest::prelude::*;
use std::num::NonZeroU64;

const AUTHOR: NonZeroU64 = NonZeroU64::new(77).unwrap();

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

/// A valid frame: `id` and `author` zero or not, sometimes a system record.
fn arb_valid_frame() -> impl Strategy<Value = Vec<u8>> {
    let record = (
        any::<u64>(),
        prop_oneof![9 => Just(0u64), 1 => any::<u64>()],
        1u64..9,
        1..=u16::MAX,
        proptest::collection::vec(any::<u8>(), 0..40),
    );
    (proptest::collection::vec(record, 1..5), any::<bool>()).prop_map(|(records, zero_author)| {
        let mut bytes = Vec::new();
        for (options, id, author, count, data) in records {
            let author = if zero_author { 0 } else { author };
            let header = RecordHeader {
                id,
                options,
                author,
                len: data.len() as u32,
                count,
                codec: 0,
            };
            encode_record(&mut bytes, &header, &data).unwrap();
        }
        bytes
    })
}

/// Uniform bytes reject at the first `len`, so most inputs are valid frames with
/// a few byte changes and sometimes a cut, which get further before going wrong.
fn arb_frame() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        1 => proptest::collection::vec(any::<u8>(), 0..400),
        3 => arb_valid_frame(),
        6 => (
            arb_valid_frame(),
            proptest::collection::vec((any::<usize>(), any::<u8>()), 1..5),
            prop_oneof![3 => Just(1.0f64), 1 => 0.0f64..1.0],
            prop::bool::weighted(0.1),
        )
            .prop_map(|(mut bytes, changes, keep_fraction, zero_count)| {
                for (at, value) in changes {
                    let at = at % bytes.len();
                    bytes[at] = value;
                }
                // Two bytes must both be 0, which random changes rarely do.
                if zero_count {
                    bytes[COUNT_OFFSET..COUNT_OFFSET + 2].fill(0);
                }
                bytes.truncate((bytes.len() as f64 * keep_fraction) as usize);
                bytes
            }),
    ]
}

/// A `len` that fits the body or nearly does, or one that can't possibly fit.
fn arb_hostile_len() -> impl Strategy<Value = Vec<u8>> {
    (
        prop_oneof![
            3 => 0u32..=72,
            1 => any::<u32>(),
            1 => (0u32..16).prop_map(|below| u32::MAX - below),
        ],
        1..=u16::MAX,
        prop_oneof![
            1 => proptest::collection::vec(any::<u8>(), 0..64),
            // Bodies of zeros, so that padding checks pass and `Ok` is reached.
            2 => (0usize..64).prop_map(|len| vec![0u8; len]),
        ],
    )
        .prop_map(|(len, count, body)| {
            let header = RecordHeader {
                id: 0,
                options: 0,
                author: 0,
                len,
                count,
                codec: 0,
            };
            let mut bytes = header.to_bytes().to_vec();
            bytes.extend_from_slice(&body);
            bytes
        })
}

/// Which paths one input reached.
#[derive(Debug, Default)]
struct Reached {
    decode_ok: u32,
    system_ok: u32,
    second_record: u32,
    publish_ok: u32,
    forward_ok: u32,
    non_zero_padding: u32,
    zero_count: u32,
    truncated_data: u32,
    truncated_header: u32,
}

/// Runs every entry point on `bytes`, and checks what each one says it did.
fn exercise(bytes: &[u8], reached: &mut Reached) {
    let _ = RecordHeader::read(bytes);
    match decode_record(bytes) {
        Ok((view, total_bytes)) => {
            reached.decode_ok += 1;
            assert!(total_bytes <= bytes.len());
            assert_eq!(total_bytes, view.header.total_bytes());
            assert_eq!(view.data.len(), view.header.len as usize);
            if SystemRecord::parse(&view).is_ok() {
                reached.system_ok += 1;
            }
        }
        Err(FormatError::NonZeroPadding) => reached.non_zero_padding += 1,
        Err(FormatError::ZeroCount) => reached.zero_count += 1,
        Err(FormatError::TruncatedData) => reached.truncated_data += 1,
        Err(FormatError::TruncatedHeader) => reached.truncated_header += 1,
        Err(error) => panic!("decode_record returned {error:?}"),
    }
    let mut walked = 0;
    let mut walked_ok = true;
    for record in Records::new(bytes) {
        walked += 1;
        walked_ok &= record.is_ok();
    }
    if walked >= 2 {
        reached.second_record += 1;
    }

    let mut publish = bytes.to_vec();
    if let Ok(mut validated) = validate_publish(&mut publish) {
        assert!(walked_ok);
        assert_eq!(validated.record_count(), walked);
        let item_count = validated.item_count();
        if walked > 0 {
            reached.publish_ok += 1;
        }
        validated.stamp(1000, AUTHOR);
        check_stamped(&publish, walked, item_count, AUTHOR.get());
    }
    let mut forward = bytes.to_vec();
    if let Ok(mut validated) = validate_forward(&mut forward) {
        assert!(walked_ok);
        let item_count = validated.item_count();
        if walked > 0 {
            reached.forward_ok += 1;
        }
        validated.stamp(1000);
        let mut next_id = 1000;
        for record in Records::new(&forward) {
            let view = record.unwrap();
            assert_eq!(view.header.id, next_id);
            assert_ne!(view.header.author, 0);
            next_id += u64::from(view.header.count);
        }
        assert_eq!(next_id, 1000 + item_count);
    }
}

fn check_stamped(bytes: &[u8], record_count: u32, item_count: u64, author: u64) {
    let mut seen = 0;
    let mut next_id = 1000;
    for record in Records::new(bytes) {
        let view = record.unwrap();
        assert_eq!(view.header.id, next_id);
        assert_eq!(view.header.author, author);
        next_id += u64::from(view.header.count);
        seen += 1;
    }
    assert_eq!(seen, record_count);
    assert_eq!(next_id, 1000 + item_count);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    /// Every entry point returns normally on arbitrary frames.
    #[test]
    fn random_bytes_never_panic(bytes in arb_frame()) {
        exercise(&bytes, &mut Reached::default());
    }

    /// Headers with hostile `len` values and small bodies.
    #[test]
    fn hostile_len_never_panics(bytes in arb_hostile_len()) {
        exercise(&bytes, &mut Reached::default());
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

fn reached_by(strategy: impl Strategy<Value = Vec<u8>>) -> Reached {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::deterministic();
    let mut reached = Reached::default();
    for _ in 0..5_000 {
        let bytes = strategy.new_tree(&mut runner).unwrap().current();
        exercise(&bytes, &mut reached);
    }
    reached
}

/// The no-panic tests prove nothing about paths their inputs never reach, so
/// count them: past the first header check, across records, and through to stamp.
#[test]
fn the_no_panic_inputs_reach_every_path() {
    let reached = reached_by(arb_frame());
    assert!(reached.decode_ok > 500, "{reached:?}");
    assert!(reached.system_ok > 50, "{reached:?}");
    assert!(reached.second_record > 500, "{reached:?}");
    assert!(reached.publish_ok > 100, "{reached:?}");
    assert!(reached.forward_ok > 100, "{reached:?}");
    assert!(reached.non_zero_padding > 50, "{reached:?}");
    assert!(reached.zero_count > 50, "{reached:?}");
    assert!(reached.truncated_data > 50, "{reached:?}");
    assert!(reached.truncated_header > 50, "{reached:?}");
}

#[test]
fn the_hostile_len_inputs_get_past_the_length_check() {
    let reached = reached_by(arb_hostile_len());
    assert!(reached.decode_ok > 500, "{reached:?}");
    assert!(reached.non_zero_padding > 50, "{reached:?}");
    assert!(reached.truncated_data > 500, "{reached:?}");
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
