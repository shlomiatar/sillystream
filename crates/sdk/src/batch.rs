//! Items to records and back (SPEC sections 2 and 3). The server never sees inside any of this.
//!
//! A record's data, before compression: with `count = 1` the item's bytes, and with `count > 1`
//! `count` little-endian `u32` end offsets into the item bytes, then the item bytes.

use format::{RecordHeader, Records, encode_record};

use crate::SdkError;

pub const CODEC_RAW: u16 = 0;
pub const CODEC_ZSTD: u16 = 1;
const MAX_ITEMS_PER_RECORD: usize = 65_535;
/// Below this, zstd's frame costs more than it can save.
const COMPRESS_MIN_BYTES: usize = 64;

/// How `build_records` packs.
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// `None` leaves data raw.
    pub zstd_level: Option<i32>,
    /// A record's item bytes before compression stop growing past this. An item over it gets a
    /// record of its own.
    pub record_target_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> BatchConfig {
        BatchConfig {
            zstd_level: Some(3),
            record_target_bytes: 1024 * 1024,
        }
    }
}

/// Sorts stably by options, keeping producer order within each value, and groups equal options
/// into encoded records.
pub fn build_records(
    items: &[(u64, &[u8])],
    config: &BatchConfig,
) -> Result<Vec<Vec<u8>>, SdkError> {
    let mut order: Vec<usize> = (0..items.len()).collect();
    order.sort_by_key(|index| items[*index].0);
    let mut records = Vec::new();
    let mut start = 0;
    while start < order.len() {
        let options = items[order[start]].0;
        let mut end = start;
        let mut item_bytes = 0;
        while end < order.len()
            && items[order[end]].0 == options
            && end - start < MAX_ITEMS_PER_RECORD
            && (end == start
                || item_bytes + items[order[end]].1.len() <= config.record_target_bytes)
        {
            item_bytes += items[order[end]].1.len();
            end += 1;
        }
        let group: Vec<&[u8]> = order[start..end].iter().map(|at| items[*at].1).collect();
        records.push(encode_group(options, &group, config)?);
        start = end;
    }
    Ok(records)
}

fn encode_group(options: u64, group: &[&[u8]], config: &BatchConfig) -> Result<Vec<u8>, SdkError> {
    let joined;
    let plain: &[u8] = if let [only] = group {
        only
    } else {
        let item_bytes: usize = group.iter().map(|item| item.len()).sum();
        let mut bytes = Vec::with_capacity(group.len() * 4 + item_bytes);
        let mut end = 0u32;
        for item in group {
            end = u32::try_from(end as usize + item.len()).map_err(|_| SdkError::TooLarge)?;
            bytes.extend_from_slice(&end.to_le_bytes());
        }
        for item in group {
            bytes.extend_from_slice(item);
        }
        joined = bytes;
        &joined
    };
    let compressed = match config.zstd_level {
        Some(level) if plain.len() >= COMPRESS_MIN_BYTES => {
            let bytes = zstd::bulk::compress(plain, level).map_err(|_| SdkError::Codec)?;
            (bytes.len() < plain.len()).then_some(bytes)
        }
        _ => None,
    };
    let (data, codec) = match &compressed {
        Some(bytes) => (bytes.as_slice(), CODEC_ZSTD),
        None => (plain, CODEC_RAW),
    };
    let header = RecordHeader {
        id: 0,
        options,
        author: 0,
        len: u32::try_from(data.len()).map_err(|_| SdkError::TooLarge)?,
        count: group.len() as u16,
        codec,
    };
    let mut record = Vec::with_capacity(format::HEADER_BYTES + data.len() + 8);
    encode_record(&mut record, &header, data).map_err(|_| SdkError::Codec)?;
    Ok(record)
}

/// One item as a subscriber gets it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub id: u64,
    pub options: u64,
    pub author: u64,
    pub bytes: Vec<u8>,
}

/// Opens one record: every item above `skip_through`, or an error and none of them.
fn open_record(
    record: &format::RecordView<'_>,
    skip_through: u64,
    limit_bytes: usize,
) -> Result<(Vec<Item>, usize), SdkError> {
    let header = record.header;
    let plain = match header.codec {
        CODEC_RAW => std::borrow::Cow::Borrowed(record.data),
        CODEC_ZSTD => {
            let mut out = Vec::new();
            let decoder = zstd::stream::Decoder::new(record.data).map_err(|_| SdkError::Codec)?;
            let limit = limit_bytes as u64 + 1;
            std::io::copy(&mut std::io::Read::take(decoder, limit), &mut out)
                .map_err(|_| SdkError::Codec)?;
            if out.len() > limit_bytes {
                return Err(SdkError::TooLarge);
            }
            std::borrow::Cow::Owned(out)
        }
        _ => return Err(SdkError::Codec),
    };
    let mut items = Vec::new();
    let mut push = |index: u64, bytes: &[u8]| {
        let id = header.id + index;
        if id > skip_through {
            items.push(Item {
                id,
                options: header.options,
                author: header.author,
                bytes: bytes.to_vec(),
            });
        }
    };
    if header.count == 1 {
        push(0, &plain);
    } else {
        let table_bytes = header.count as usize * 4;
        let (table, bytes) = plain.split_at_checked(table_bytes).ok_or(SdkError::Codec)?;
        let mut start = 0usize;
        for (index, end) in table.as_chunks::<4>().0.iter().enumerate() {
            let end = u32::from_le_bytes(*end) as usize;
            let item = bytes.get(start..end).ok_or(SdkError::Codec)?;
            push(index as u64, item);
            start = end;
        }
    }
    let cost_bytes = plain.len() + items.len() * size_of::<Item>();
    Ok((items, cost_bytes))
}

/// Reads what a `DATA` frame or the read route carries, all or nothing. Items at or below
/// `skip_through` are left out (section 5.2: the server sends a whole record, and the client skips
/// ahead by id), and a record wholly below it isn't decompressed. The limit covers the whole run.
pub fn decode_records(
    records: &[u8],
    skip_through: u64,
    max_decompressed_bytes: usize,
) -> Result<Vec<Item>, SdkError> {
    let mut items = Vec::new();
    let mut used_bytes = 0usize;
    for record in Records::new(records) {
        let record = record.map_err(|_| SdkError::Codec)?;
        if record.header.last_id() <= skip_through {
            continue;
        }
        let limit_bytes = max_decompressed_bytes.saturating_sub(used_bytes);
        let (opened, cost_bytes) = open_record(&record, skip_through, limit_bytes)?;
        used_bytes = used_bytes.saturating_add(cost_bytes);
        items.extend(opened);
    }
    Ok(items)
}

/// What `decode_some` got from the front of a run of records.
#[derive(Debug)]
pub(crate) struct Decoded {
    pub items: Vec<Item>,
    /// How many bytes of the run this covers. The rest is for the next call.
    pub used_bytes: usize,
    /// The record that ended this, behind `items`. Its bytes are covered by `used_bytes`.
    pub failed: Option<SdkError>,
}

/// As `decode_records` for a subscription, where one bad record must not cost its neighbours and
/// where a frame can't be allowed to decompress to any size. It stops at a record that can't be
/// opened, and before one that would take what it holds past the limit. A record is always taken
/// if nothing else is held, so this always moves on.
pub(crate) fn decode_some(
    records: &[u8],
    skip_through: u64,
    max_decompressed_bytes: usize,
) -> Decoded {
    let mut items = Vec::new();
    let mut held_bytes = 0usize;
    let mut used_bytes = 0usize;
    for record in Records::new(records) {
        let Ok(record) = record else {
            // Its length can't be trusted, so nothing behind it can be found.
            return Decoded {
                items,
                used_bytes: records.len(),
                failed: Some(SdkError::Codec),
            };
        };
        let header = record.header;
        let total_bytes = header.total_bytes();
        if header.last_id() <= skip_through {
            used_bytes += total_bytes;
            continue;
        }
        let (opened, cost_bytes) = match open_record(&record, skip_through, max_decompressed_bytes)
        {
            Ok(opened) => opened,
            Err(error) => {
                return Decoded {
                    items,
                    used_bytes: used_bytes + total_bytes,
                    failed: Some(SdkError::BadRecord {
                        first_id: header.id,
                        last_id: header.last_id(),
                        too_large: error == SdkError::TooLarge,
                    }),
                };
            }
        };
        if !items.is_empty() && held_bytes + cost_bytes > max_decompressed_bytes {
            break;
        }
        held_bytes += cost_bytes;
        used_bytes += total_bytes;
        items.extend(opened);
    }
    Decoded {
        items,
        used_bytes,
        failed: None,
    }
}

/// The id of the last item in a run of records, without opening any. `None` if there is none.
pub(crate) fn last_id(records: &[u8]) -> Result<Option<u64>, SdkError> {
    let mut last = None;
    for record in Records::new(records) {
        last = Some(record.map_err(|_| SdkError::Codec)?.header.last_id());
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// What a server does to a run of records: gives them ids and an author.
    fn stamp(records: &[Vec<u8>], first_id: u64) -> Vec<u8> {
        let mut out = Vec::new();
        let mut id = first_id;
        for record in records {
            let start = out.len();
            out.extend_from_slice(record);
            out[start..start + 8].copy_from_slice(&id.to_le_bytes());
            out[start + 16..start + 24].copy_from_slice(&5u64.to_le_bytes());
            let count = u16::from_le_bytes([record[28], record[29]]);
            id += count as u64;
        }
        out
    }

    #[test]
    fn a_single_item_is_its_bytes_and_a_group_has_a_table() {
        let raw = BatchConfig {
            zstd_level: None,
            ..BatchConfig::default()
        };
        let one = build_records(&[(1, b"hello")], &raw).unwrap();
        assert_eq!(&one[0][32..37], b"hello");
        let two = build_records(&[(1, b"ab"), (1, b"c")], &raw).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(&two[0][28..30], &2u16.to_le_bytes());
        assert_eq!(&two[0][32..40], &[2, 0, 0, 0, 3, 0, 0, 0]);
        assert_eq!(&two[0][40..43], b"abc");
    }

    #[test]
    fn what_compresses_is_marked_and_what_doesnt_stays_raw() {
        let config = BatchConfig::default();
        let big = vec![b'a'; 4096];
        let records = build_records(&[(1, &big)], &config).unwrap();
        assert_eq!(
            u16::from_le_bytes([records[0][30], records[0][31]]),
            CODEC_ZSTD
        );
        assert!(records[0].len() < 1024);
        let small = build_records(&[(1, b"abc")], &config).unwrap();
        assert_eq!(u16::from_le_bytes([small[0][30], small[0][31]]), CODEC_RAW);
    }

    #[test]
    fn a_full_record_starts_another() {
        let config = BatchConfig {
            zstd_level: None,
            record_target_bytes: 10,
        };
        let records = build_records(&[(1, &[1; 6]), (1, &[2; 6]), (1, &[3; 40])], &config).unwrap();
        assert_eq!(records.len(), 3);
        let empties: Vec<(u64, &[u8])> = vec![(1, b""); MAX_ITEMS_PER_RECORD + 1];
        let records = build_records(&empties, &BatchConfig::default()).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn a_decompression_bomb_stops_at_the_limit() {
        let big = vec![0u8; 1024 * 1024];
        let records = build_records(&[(1, &big)], &BatchConfig::default()).unwrap();
        assert!(matches!(
            decode_records(&stamp(&records, 1), 0, 1024),
            Err(SdkError::TooLarge)
        ));
    }

    fn raw_record(options: u64, data: &[u8]) -> Vec<u8> {
        let config = BatchConfig {
            zstd_level: None,
            ..BatchConfig::default()
        };
        build_records(&[(options, data)], &config).unwrap().concat()
    }

    #[test]
    fn a_frame_of_small_bombs_stops_at_the_limit_all_or_nothing() {
        let big = vec![0u8; 1024];
        let items: Vec<(u64, &[u8])> = (0..8).map(|at| (at, big.as_slice())).collect();
        let records = build_records(&items, &BatchConfig::default()).unwrap();
        let stamped = stamp(&records, 1);
        assert!(matches!(
            decode_records(&stamped, 0, 4 * 1024),
            Err(SdkError::TooLarge)
        ));
        assert_eq!(decode_records(&stamped, 0, 16 * 1024).unwrap().len(), 8);
    }

    #[test]
    fn decode_some_steps_over_a_bad_record_and_holds_to_the_limit() {
        let mut bad = raw_record(2, b"not zstd");
        bad[30..32].copy_from_slice(&CODEC_ZSTD.to_le_bytes());
        let good = raw_record(1, &[7u8; 100]);
        let run = [good.clone(), bad, good.clone(), good.clone()];
        let stamped = stamp(&run, 10);

        let first = decode_some(&stamped, 0, 1 << 20);
        assert_eq!(first.items.iter().map(|i| i.id).collect::<Vec<_>>(), [10]);
        assert_eq!(
            first.failed,
            Some(SdkError::BadRecord {
                first_id: 11,
                last_id: 11,
                too_large: false
            })
        );
        assert_eq!(first.used_bytes, run[0].len() + run[1].len());
        let last = decode_some(&stamped[first.used_bytes..], 11, 1 << 20);
        assert_eq!(
            last.items.iter().map(|i| i.id).collect::<Vec<_>>(),
            [12, 13]
        );
        assert_eq!(last.failed, None);

        // A limit of one record gives one at a time, and none is ever stuck.
        let mut at = first.used_bytes;
        let mut ids = Vec::new();
        while at < stamped.len() {
            let some = decode_some(&stamped[at..], 11, 1);
            assert!(some.used_bytes > 0);
            assert_eq!(some.items.len(), 1);
            ids.push(some.items[0].id);
            at += some.used_bytes;
        }
        assert_eq!(ids, [12, 13]);
    }

    #[test]
    fn a_bad_table_gives_no_items_from_its_record() {
        let mut record = raw_record(1, &[]);
        // count 2, with a second offset past the bytes.
        record[28..30].copy_from_slice(&2u16.to_le_bytes());
        record[24..28].copy_from_slice(&8u32.to_le_bytes());
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(&9u32.to_le_bytes());
        let stamped = stamp(&[record], 5);
        let some = decode_some(&stamped, 0, 1 << 20);
        assert!(some.items.is_empty());
        assert_eq!(
            some.failed,
            Some(SdkError::BadRecord {
                first_id: 5,
                last_id: 6,
                too_large: false
            })
        );
    }

    proptest! {
        #[test]
        fn items_round_trip_in_stable_option_order(
            items in proptest::collection::vec(
                (0u64..4, proptest::collection::vec(any::<u8>(), 0..300)), 1..60),
            compress in any::<bool>(),
            target in 1usize..2000,
        ) {
            let config = BatchConfig {
                zstd_level: compress.then_some(1),
                record_target_bytes: target,
            };
            let borrowed: Vec<(u64, &[u8])> =
                items.iter().map(|(options, bytes)| (*options, bytes.as_slice())).collect();
            let records = build_records(&borrowed, &config).unwrap();
            let stamped = stamp(&records, 1000);
            let got = decode_records(&stamped, 0, 1 << 20).unwrap();

            let mut expected: Vec<(u64, Vec<u8>)> = items.clone();
            expected.sort_by_key(|item| item.0);
            prop_assert_eq!(got.len(), expected.len());
            for (index, (item, want)) in got.iter().zip(&expected).enumerate() {
                prop_assert_eq!(item.id, 1000 + index as u64);
                prop_assert_eq!(item.options, want.0);
                prop_assert_eq!(&item.bytes, &want.1);
            }
            prop_assert_eq!(last_id(&stamped).unwrap(), Some(1000 + items.len() as u64 - 1));

            // Skipping by id lands on any item, inside a record or between them.
            let skip = 1000 + items.len() as u64 / 2;
            let skipped = decode_records(&stamped, skip, 1 << 20).unwrap();
            prop_assert!(skipped.iter().all(|item| item.id > skip));
            prop_assert_eq!(skipped.len() as u64, 1000 + items.len() as u64 - 1 - skip);
        }

        #[test]
        fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = decode_records(&bytes, 0, 1 << 16);
            let _ = decode_some(&bytes, 0, 1 << 16);
            let _ = last_id(&bytes);
        }
    }
}
