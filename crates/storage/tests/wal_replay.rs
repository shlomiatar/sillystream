//! Recovery: replay returns exactly the valid prefix, whatever happened to the tail.

use bytes::Bytes;
use proptest::prelude::*;
use storage::{
    ReplayedEntry, WAL_FILE_HEADER_BYTES, WalFileInfo, encode_file_header, encode_wal_entry,
    replay_bytes, replay_dir, replay_file, replay_files, wal_file_name,
};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const SEQ: u64 = 42;

struct Case {
    stream: &'static str,
    producer_id: u64,
    producer_seq: u64,
    records: Vec<u8>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            stream: "a",
            producer_id: 1,
            producer_seq: 1,
            records: vec![0xA1; 32],
        },
        Case {
            stream: "inbox.reviewer",
            producer_id: 0,
            producer_seq: 0,
            records: vec![0xB2; 40],
        },
        Case {
            stream: "logs.payments.3",
            producer_id: u64::MAX,
            producer_seq: 9,
            records: vec![0xC3; 48],
        },
    ]
}

/// The file's bytes, and the offset where each entry ends.
fn file(seq: u64, cases: &[Case]) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = encode_file_header(seq, 0).to_vec();
    let mut ends = Vec::new();
    for case in cases {
        bytes.extend(
            encode_wal_entry(
                seq,
                key(case.stream),
                case.producer_id,
                case.producer_seq,
                &case.records,
            )
            .unwrap(),
        );
        ends.push(bytes.len());
    }
    (bytes, ends)
}

fn assert_is_case(entry: &ReplayedEntry, case: &Case) {
    assert_eq!(entry.stream, key(case.stream));
    assert_eq!(entry.producer_id, case.producer_id);
    assert_eq!(entry.producer_seq, case.producer_seq);
    assert_eq!(entry.records[..], case.records[..]);
}

#[test]
fn truncation_at_every_byte_returns_the_entries_that_fit() {
    let cases = cases();
    let (bytes, ends) = file(SEQ, &cases);
    for len in 0..=bytes.len() {
        let replayed = replay_bytes(&Bytes::copy_from_slice(&bytes[..len]), SEQ);
        let expected = ends.iter().filter(|end| **end <= len).count();
        assert_eq!(replayed.entries.len(), expected, "truncated to {len}");
        for (entry, case) in replayed.entries.iter().zip(&cases) {
            assert_is_case(entry, case);
        }
        let expected_end = match expected {
            0 if len < WAL_FILE_HEADER_BYTES => 0,
            0 => WAL_FILE_HEADER_BYTES,
            count => ends[count - 1],
        };
        assert_eq!(replayed.valid_end_bytes, expected_end, "truncated to {len}");
    }
}

#[test]
fn a_flipped_bit_anywhere_returns_the_entries_before_it() {
    let cases = cases();
    let (bytes, ends) = file(SEQ, &cases);
    for bit in 0..bytes.len() * 8 {
        let mut flipped = bytes.clone();
        flipped[bit / 8] ^= 1 << (bit % 8);
        let replayed = replay_bytes(&Bytes::from(flipped), SEQ);
        let expected = if bit / 8 < WAL_FILE_HEADER_BYTES {
            0
        } else {
            ends.iter().filter(|end| **end <= bit / 8).count()
        };
        assert_eq!(replayed.entries.len(), expected, "flipped bit {bit}");
        for (entry, case) in replayed.entries.iter().zip(&cases) {
            assert_is_case(entry, case);
        }
    }
}

#[test]
fn a_preallocated_zero_tail_ends_the_prefix() {
    let cases = cases();
    let (mut bytes, ends) = file(SEQ, &cases);
    bytes.resize(bytes.len() + 4096, 0);
    let replayed = replay_bytes(&Bytes::from(bytes), SEQ);
    assert_eq!(replayed.entries.len(), 3);
    assert_eq!(replayed.valid_end_bytes, ends[2]);
}

#[test]
fn stale_entries_of_a_reused_file_are_not_returned() {
    // An old file full of valid entries for seq 41, reused as 42: one new entry
    // shorter than what it overwrites, leaving old entries at valid-looking offsets.
    let old_cases = cases();
    let (old, old_ends) = file(41, &old_cases);
    let new_entry = encode_wal_entry(42, key("a"), 5, 5, &[0xEE; 32]).unwrap();
    assert_eq!(
        WAL_FILE_HEADER_BYTES + new_entry.len(),
        old_ends[0],
        "the new entry ends exactly where an old one starts"
    );
    let mut reused = old.clone();
    reused[..WAL_FILE_HEADER_BYTES].copy_from_slice(&encode_file_header(42, 0));
    reused[WAL_FILE_HEADER_BYTES..WAL_FILE_HEADER_BYTES + new_entry.len()]
        .copy_from_slice(&new_entry);
    let replayed = replay_bytes(&Bytes::from(reused), 42);
    assert_eq!(replayed.entries.len(), 1);
    assert_eq!(replayed.entries[0].producer_seq, 5);
    assert_eq!(replayed.valid_end_bytes, old_ends[0]);
}

#[test]
fn a_header_that_does_not_match_the_name_has_no_entries() {
    // A crash between renaming a reused file and rewriting its header leaves the
    // old header under the new name.
    let (old, _) = file(41, &cases());
    let replayed = replay_bytes(&Bytes::from(old), 42);
    assert!(replayed.entries.is_empty());
    assert_eq!(replayed.valid_end_bytes, 0);
}

#[test]
fn a_directory_replays_oldest_first_and_ignores_other_files() {
    let dir = tempfile::tempdir().unwrap();
    let (first, _) = file(1, &cases()[..1]);
    let (second, _) = file(2, &cases()[1..2]);
    // Written newest first, so sorting is what puts them in order.
    std::fs::write(dir.path().join(wal_file_name(2)), second).unwrap();
    std::fs::write(dir.path().join(wal_file_name(1)), first).unwrap();
    std::fs::write(dir.path().join("wal-1.log"), b"not ours").unwrap();
    std::fs::write(dir.path().join("notes.txt"), b"not ours").unwrap();
    let entries = replay_dir(dir.path()).unwrap();
    let streams: Vec<_> = entries.iter().map(|entry| entry.stream).collect();
    assert_eq!(streams, [key("a"), key("inbox.reviewer")]);
}

#[test]
fn an_entry_matches_its_byte_exact_vector() {
    // Built by an independent implementation of section 6.3 and D9, D50, with fixed key hashes.
    let vector = include_str!("vectors/wal_file.hex").trim();
    let expected: Vec<u8> = (0..vector.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&vector[at..at + 2], 16).unwrap())
        .collect();
    let mut bytes = encode_file_header(3, 5).to_vec();
    bytes.extend(encode_wal_entry(3, format::KeyHash([0x11; 16]), 7, 42, &[0xAB; 32]).unwrap());
    bytes.extend(encode_wal_entry(3, format::KeyHash([0x22; 16]), 0, 0, &[0xCD; 8]).unwrap());
    assert_eq!(bytes, expected);
    let replayed = replay_bytes(&Bytes::from(expected), 3);
    assert_eq!(replayed.generation, 5);
    assert_eq!(replayed.entries.len(), 2);
    assert_eq!(replayed.entries[0].stream, format::KeyHash([0x11; 16]));
    assert_eq!(replayed.entries[0].producer_id, 7);
    assert_eq!(replayed.entries[0].producer_seq, 42);
}

proptest! {
    #[test]
    fn entries_round_trip(
        entries in prop::collection::vec(
            ("[a-z.]{1,40}", any::<u64>(), any::<u64>(), prop::collection::vec(any::<u8>(), 1..300)),
            0..20,
        ),
        seq in any::<u64>(),
        tail in prop::collection::vec(any::<u8>(), 0..8),
    ) {
        let mut bytes = encode_file_header(seq, 0).to_vec();
        for (stream, id, producer_seq, records) in &entries {
            let entry = encode_wal_entry(seq, key(stream), *id, *producer_seq, records).unwrap();
            prop_assert_eq!(entry.len() % 8, 0);
            bytes.extend(entry);
        }
        let end = bytes.len();
        // A short tail is never a whole entry, whatever it holds.
        bytes.extend(tail);
        let replayed = replay_bytes(&Bytes::from(bytes), seq);
        prop_assert_eq!(replayed.valid_end_bytes, end);
        prop_assert_eq!(replayed.entries.len(), entries.len());
        for (entry, (stream, id, producer_seq, records)) in replayed.entries.iter().zip(&entries) {
            prop_assert_eq!(entry.stream, key(stream));
            prop_assert_eq!(entry.producer_id, *id);
            prop_assert_eq!(entry.producer_seq, *producer_seq);
            prop_assert_eq!(&entry.records[..], &records[..]);
        }
    }

    #[test]
    fn random_bytes_never_panic_and_give_nothing(
        body in prop::collection::vec(any::<u8>(), 0..2048),
        seq in any::<u64>(),
    ) {
        let mut bytes = encode_file_header(seq, 0).to_vec();
        bytes.extend(body);
        let replayed = replay_bytes(&Bytes::from(bytes), seq);
        // A CRC could in theory pass by chance, but not within what proptest tries.
        prop_assert!(replayed.entries.is_empty());
    }

    #[test]
    fn entirely_random_files_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..64), seq in any::<u64>()) {
        let _ = replay_bytes(&Bytes::from(bytes), seq);
    }
}

/// Writes `bytes` at the start of a file of `len_bytes`, the rest a hole, as `DirVolume` does.
fn sparse_wal_file(dir: &std::path::Path, seq: u64, bytes: &[u8], len_bytes: u64) -> WalFileInfo {
    let path = dir.join(wal_file_name(seq));
    std::fs::write(&path, bytes).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len_bytes)
        .unwrap();
    WalFileInfo { seq, path }
}

#[test]
fn a_mapped_file_with_a_hole_for_a_tail_replays_and_has_no_damage() {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = encode_file_header(SEQ, 0).to_vec();
    bytes.extend_from_slice(&encode_wal_entry(SEQ, key("a"), 1, 1, &[0xEE; 32]).unwrap());
    let file = sparse_wal_file(dir.path(), SEQ, &bytes, 1 << 26);
    let replayed = replay_file(&file).unwrap();
    assert_eq!(replayed.entries.len(), 1);
    assert_eq!(replayed.entries[0].records, vec![0xEE; 32]);
    assert_eq!(replayed.valid_end_bytes, bytes.len());
    assert!(!replayed.damaged_tail);
}

#[test]
fn a_mapped_file_finds_damage_past_a_hole() {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = encode_file_header(SEQ, 0).to_vec();
    bytes.extend_from_slice(&encode_wal_entry(SEQ, key("a"), 1, 1, &[0xEE; 32]).unwrap());
    let file = sparse_wal_file(dir.path(), SEQ, &bytes, 1 << 22);
    let mut handle = std::fs::OpenOptions::new()
        .write(true)
        .open(&file.path)
        .unwrap();
    std::io::Seek::seek(&mut handle, std::io::SeekFrom::Start((1 << 22) - 1)).unwrap();
    std::io::Write::write_all(&mut handle, &[1]).unwrap();
    drop(handle);
    let replayed = replay_file(&file).unwrap();
    assert_eq!(replayed.entries.len(), 1);
    assert!(replayed.damaged_tail);
}

#[test]
fn replayed_entries_outlive_their_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = encode_file_header(SEQ, 0).to_vec();
    bytes.extend_from_slice(&encode_wal_entry(SEQ, key("a"), 1, 1, &[0xEE; 32]).unwrap());
    let file = sparse_wal_file(dir.path(), SEQ, &bytes, 1 << 16);
    let replayed = replay_files(std::slice::from_ref(&file))
        .pop()
        .unwrap()
        .unwrap();
    std::fs::remove_file(&file.path).unwrap();
    assert_eq!(replayed.entries[0].records, vec![0xEE; 32]);
}
