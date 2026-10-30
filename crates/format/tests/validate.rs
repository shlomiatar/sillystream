//! Publish validation (SPEC section 3) and stamping.

use std::num::NonZeroU64;

use format::{
    COUNT_OFFSET, FormatError, RecordHeader, Records, encode_record, validate_forward,
    validate_publish,
};
use proptest::prelude::*;

const AUTHOR: NonZeroU64 = NonZeroU64::new(77).unwrap();

fn unstamped(options: u64, count: u16, data: &[u8]) -> Vec<u8> {
    let header = RecordHeader {
        id: 0,
        options,
        author: 0,
        len: data.len() as u32,
        count,
        codec: 0,
    };
    let mut bytes = Vec::new();
    encode_record(&mut bytes, &header, data).unwrap();
    bytes
}

fn check(mut bytes: Vec<u8>) -> Result<(u32, u64), FormatError> {
    validate_publish(&mut bytes).map(|publish| (publish.record_count(), publish.item_count()))
}

#[test]
fn accepts_a_valid_publish() {
    let mut bytes = unstamped(1, 2, b"abc");
    bytes.extend(unstamped(2, 3, b"12345678"));
    assert_eq!(check(bytes), Ok((2, 5)));
    assert_eq!(check(Vec::new()), Ok((0, 0)));
}

#[test]
fn rejects_lengths_that_do_not_add_up() {
    let valid = unstamped(1, 1, b"12345678");

    // Frame shorter than the record, cut at every byte.
    for cut in 1..valid.len() {
        assert!(check(valid[..cut].to_vec()).is_err(), "cut at {cut}");
    }
    assert_eq!(
        check(valid[..31].to_vec()),
        Err(FormatError::TruncatedHeader)
    );
    assert_eq!(check(valid[..39].to_vec()), Err(FormatError::TruncatedData));

    // Frame longer than the record: extra bytes that aren't a whole header.
    for extra in 1..32 {
        let mut bytes = valid.clone();
        bytes.resize(valid.len() + extra, 0);
        assert_eq!(
            check(bytes),
            Err(FormatError::TruncatedHeader),
            "extra {extra}"
        );
    }

    // A `len` that claims more than the frame holds.
    let mut bytes = valid.clone();
    bytes[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(check(bytes), Err(FormatError::TruncatedData));

    // Padding counts: a 5-byte record needs 8 bytes after the header.
    let mut padded = unstamped(1, 1, b"hello");
    padded.truncate(37);
    assert_eq!(check(padded), Err(FormatError::TruncatedData));
}

#[test]
fn rejects_count_zero() {
    let mut bytes = unstamped(1, 1, b"12345678");
    bytes[COUNT_OFFSET..COUNT_OFFSET + 2].copy_from_slice(&0u16.to_le_bytes());
    assert_eq!(check(bytes), Err(FormatError::ZeroCount));
}

#[test]
fn rejects_non_zero_id_in_any_byte() {
    for byte in 0..8 {
        let mut bytes = unstamped(1, 1, b"12345678");
        bytes[byte] = 1;
        assert_eq!(check(bytes), Err(FormatError::NonZeroId), "byte {byte}");
    }
}

#[test]
fn rejects_non_zero_author_in_any_byte() {
    for byte in 16..24 {
        let mut bytes = unstamped(1, 1, b"12345678");
        bytes[byte] = 1;
        assert_eq!(check(bytes), Err(FormatError::NonZeroAuthor), "byte {byte}");
    }
}

#[test]
fn rejects_non_zero_padding_in_any_byte() {
    for byte in 37..40 {
        let mut bytes = unstamped(1, 1, b"hello");
        bytes[byte] = 1;
        assert_eq!(
            check(bytes),
            Err(FormatError::NonZeroPadding),
            "byte {byte}"
        );
    }
}

#[test]
fn a_bad_later_record_rejects_the_whole_publish() {
    let mut bytes = unstamped(1, 1, b"12345678");
    let mut bad = unstamped(2, 1, b"12345678");
    bad[0] = 9;
    bytes.extend(bad);
    assert_eq!(check(bytes), Err(FormatError::NonZeroId));
}

#[test]
fn rejects_more_items_than_an_ack_can_count() {
    // 65,537 × 65,535 = u32::MAX exactly: that many items fit, and one more record doesn't.
    let record = unstamped(0, u16::MAX, b"");
    let fits = record.repeat(65_537);
    assert_eq!(check(fits), Ok((65_537, u64::from(u32::MAX))));
    let over = record.repeat(65_538);
    assert_eq!(check(over), Err(FormatError::TooManyItems));
}

#[test]
fn forward_keeps_the_author_and_refuses_system_records() {
    let mut bytes = unstamped(1, 2, b"abc");
    assert_eq!(
        validate_forward(&mut bytes).map(|forward| forward.item_count()),
        Err(FormatError::SystemRecordForwarded)
    );

    bytes[16..24].copy_from_slice(&123u64.to_le_bytes());
    let mut forward = validate_forward(&mut bytes).unwrap();
    assert_eq!(forward.item_count(), 2);
    forward.stamp(500);
    let view = Records::new(&bytes).next().unwrap().unwrap();
    assert_eq!(view.header.id, 500);
    assert_eq!(view.header.author, 123);

    // Other rules still apply.
    let mut stamped = bytes.clone();
    assert_eq!(
        validate_forward(&mut stamped).map(|forward| forward.item_count()),
        Err(FormatError::NonZeroId)
    );
}

/// What can go wrong with a record, applied to its header or padding only.
#[derive(Debug, Clone, Copy)]
enum Defect {
    None,
    Id,
    Author,
    CountZero,
    Padding,
    LenOverrun,
    TrailingByte,
}

fn arb_defect() -> impl Strategy<Value = Defect> {
    prop_oneof![
        Just(Defect::None),
        Just(Defect::Id),
        Just(Defect::Author),
        Just(Defect::CountZero),
        Just(Defect::Padding),
        Just(Defect::LenOverrun),
        Just(Defect::TrailingByte),
    ]
}

/// A record's data and `count`.
type PublishRecord = (Vec<u8>, u16);

/// Builds a publish. The lengths and the defect depend only on the shape of
/// `records`, so two calls with equally long data differ in data bytes only.
fn build(records: &[PublishRecord], defect: Defect) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = Vec::new();
    let mut data_offsets = Vec::new();
    let mut last_start = 0;
    for (data, count) in records {
        last_start = bytes.len();
        for at in 0..data.len() {
            data_offsets.push(bytes.len() + 32 + at);
        }
        bytes.extend(unstamped(9, *count, data));
    }
    let last_len = records[records.len() - 1].0.len();
    match defect {
        Defect::None => {}
        Defect::Id => bytes[last_start] = 1,
        Defect::Author => bytes[last_start + 16] = 1,
        Defect::CountZero => bytes[last_start + 28..last_start + 30].fill(0),
        Defect::Padding => {
            if !last_len.is_multiple_of(8) {
                *bytes.last_mut().unwrap() = 1;
            } else {
                bytes[last_start] = 1;
            }
        }
        Defect::LenOverrun => {
            bytes[last_start + 24..last_start + 28]
                .copy_from_slice(&((last_len as u32 + 9).to_le_bytes()));
        }
        Defect::TrailingByte => bytes.push(0),
    }
    (bytes, data_offsets)
}

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Random bytes, often starting with what a codec sniffer would look for.
fn arb_data(len: usize) -> impl Strategy<Value = Vec<u8>> {
    (proptest::collection::vec(any::<u8>(), len), any::<bool>()).prop_map(|(mut data, magic)| {
        if magic {
            let prefix = data.len().min(ZSTD_MAGIC.len());
            data[..prefix].copy_from_slice(&ZSTD_MAGIC[..prefix]);
        }
        data
    })
}

/// Two publishes of the same shape with independent random data.
fn arb_publish_pair() -> impl Strategy<Value = (Vec<PublishRecord>, Vec<PublishRecord>)> {
    proptest::collection::vec((0usize..64, 1u16..=u16::MAX), 1..8).prop_flat_map(|shape| {
        let side = |shape: &[(usize, u16)]| {
            shape
                .iter()
                .map(|(len, count)| (arb_data(*len), Just(*count)))
                .collect::<Vec<_>>()
        };
        (side(&shape), side(&shape))
    })
}

/// A publish whose data is the zstd magic is an ordinary publish: the rules don't
/// look at data, whatever it holds.
#[test]
fn data_that_looks_compressed_is_accepted() {
    assert_eq!(check(unstamped(1, 1, &ZSTD_MAGIC)), Ok((1, 1)));
    for first in 0..=u8::MAX {
        for second in 0..=u8::MAX {
            assert_eq!(check(unstamped(1, 1, &[first, second])), Ok((1, 1)));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// The result is a function of headers and padding alone: changing every data
    /// byte, to anything, never changes it, and validation leaves the bytes alone.
    /// That no read happens at all is shown by the probe tests in `publish.rs`.
    #[test]
    fn validation_does_not_depend_on_data(
        (records_a, records_b) in arb_publish_pair(),
        defect in arb_defect(),
    ) {
        let (bytes_a, offsets) = build(&records_a, defect);
        let (mut bytes_b, _) = build(&records_b, defect);
        // Only data bytes may differ between the two.
        let data_offsets: std::collections::HashSet<usize> = offsets.into_iter().collect();
        for (at, (x, y)) in bytes_a.iter().zip(&bytes_b).enumerate() {
            if x != y {
                prop_assert!(data_offsets.contains(&at));
            }
        }
        let before = bytes_b.clone();
        let result_a = check(bytes_a);
        let result_b = validate_publish(&mut bytes_b)
            .map(|publish| (publish.record_count(), publish.item_count()));
        prop_assert_eq!(result_a, result_b);
        prop_assert_eq!(bytes_b, before);
    }

    /// Stamping writes only `id` and `author`, never data, and ids are consecutive.
    #[test]
    fn stamping_preserves_everything_else(
        records in proptest::collection::vec(
            (any::<u64>(), 1u16..=u16::MAX, proptest::collection::vec(any::<u8>(), 0..64)),
            0..16,
        ),
        first_id in 1u64..u64::MAX / 2,
    ) {
        let mut bytes = Vec::new();
        for (options, count, data) in &records {
            bytes.extend(unstamped(*options, *count, data));
        }
        let before = bytes.clone();
        validate_publish(&mut bytes).unwrap().stamp(first_id, AUTHOR);

        let mut next_id = first_id;
        let stamped: Vec<_> = Records::new(&bytes).collect::<Result<_, _>>().unwrap();
        let original: Vec<_> = Records::new(&before).collect::<Result<_, _>>().unwrap();
        prop_assert_eq!(stamped.len(), records.len());
        for (view, old) in stamped.iter().zip(&original) {
            prop_assert_eq!(view.header.id, next_id);
            prop_assert_eq!(view.header.author, AUTHOR.get());
            prop_assert_eq!(view.header.options, old.header.options);
            prop_assert_eq!(view.header.len, old.header.len);
            prop_assert_eq!(view.header.count, old.header.count);
            prop_assert_eq!(view.header.codec, old.header.codec);
            prop_assert_eq!(view.data, old.data);
            next_id += u64::from(view.header.count);
        }
    }
}
