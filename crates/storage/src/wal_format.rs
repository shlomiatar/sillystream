//! Bytes of the WAL (SPEC section 6.3, D8, D9, D50).
//!
//! A file is `magic u64 | seq u64 | generation u64 | crc32c u32 | 0 u32`, then entries, each
//! starting at a multiple of 8. `generation` is the owner that opened it, 0 if there is none,
//! which is what tells a replay how far to accept its entries (section 8.1, D152):
//!
//! ```text
//! stream key hash [u8; 16] | producer_id u64 | producer_seq u64 | len u32 | crc32c u32 |
//! records | zero padding up to a multiple of 8
//! ```
//!
//! The CRC is seeded with the file's `seq`, and covers every entry byte except itself.

use std::fmt;

use format::{KeyHash, crc32c, crc32c_append, crc32c_combine, crc32c_seeded};

/// `SSWAL002` as little-endian bytes on disk.
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SSWAL002");
pub const WAL_FILE_HEADER_BYTES: usize = 32;
/// The entry's fixed fields.
pub(crate) const ENTRY_FIXED_BYTES: usize = KeyHash::BYTES + 8 + 8 + 4 + 4;
pub(crate) const ZERO_PADDING: [u8; 8] = [0; 8];

pub fn encode_file_header(seq: u64, generation: u64) -> [u8; WAL_FILE_HEADER_BYTES] {
    let mut out = [0u8; WAL_FILE_HEADER_BYTES];
    out[..8].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    out[8..16].copy_from_slice(&seq.to_le_bytes());
    out[16..24].copy_from_slice(&generation.to_le_bytes());
    let crc = crc32c(&out[..24]);
    out[24..28].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Why an entry can't be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEntryError {
    /// A publish always has at least one record (D6a).
    EmptyRecords,
    /// `len` is a u32.
    RecordsTooLong,
}

impl fmt::Display for WalEntryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WalEntryError::EmptyRecords => "entry has no record bytes",
            WalEntryError::RecordsTooLong => "entry records are over u32::MAX bytes",
        })
    }
}

impl std::error::Error for WalEntryError {}

/// One publish on its way to the WAL. `records` is a reference to bytes the core
/// keeps owning, so nothing is copied before the write (section 6.2), and it is
/// generic because the slab reference arrives with the cores.
#[derive(Debug, Clone)]
pub struct WalEntry<R> {
    stream: KeyHash,
    producer_id: u64,
    producer_seq: u64,
    records: R,
    /// `crc32c(records)`, computed in `new` on the sending core while the bytes are still in its
    /// cache, so the writer thread only combines it into the seeded CRC.
    records_crc: u32,
    /// The core to tell when this entry is durable.
    core: usize,
    /// Increases per core. The writer reports the highest durable one.
    ticket: u64,
    /// The publish's `(first_id, count)`, for a dedupe entry the next file carries (section 3).
    carry: Option<(u64, u32)>,
}

impl<R: AsRef<[u8]>> WalEntry<R> {
    pub fn new(
        stream: KeyHash,
        producer_id: u64,
        producer_seq: u64,
        records: R,
        core: usize,
        ticket: u64,
    ) -> Result<WalEntry<R>, WalEntryError> {
        check_entry(records.as_ref())?;
        let records_crc = crc32c(records.as_ref());
        Ok(WalEntry {
            stream,
            producer_id,
            producer_seq,
            records,
            records_crc,
            core,
            ticket,
            carry: None,
        })
    }

    /// Has the next file carry this publish's dedupe entry (section 3).
    pub fn carrying(mut self, first_id: u64, count: u32) -> WalEntry<R> {
        self.carry = Some((first_id, count));
        self
    }

    /// The dedupe entry the next file carries for this publish, if it has one.
    pub(crate) fn dedupe_carry(&self) -> Option<DedupeCarry> {
        self.carry.map(|(first_id, count)| DedupeCarry {
            stream: self.stream,
            producer_id: self.producer_id,
            producer_seq: self.producer_seq,
            first_id,
            count,
        })
    }

    pub fn stream(&self) -> KeyHash {
        self.stream
    }

    /// What the writer will report as durable once this entry is synced.
    pub fn ticket_for_core(&self) -> u64 {
        self.ticket
    }

    pub(crate) fn core(&self) -> usize {
        self.core
    }

    pub(crate) fn ticket(&self) -> u64 {
        self.ticket
    }

    pub(crate) fn records(&self) -> &[u8] {
        self.records.as_ref()
    }

    pub(crate) fn into_records(self) -> R {
        self.records
    }

    /// Appends the entry's head (everything before `records`) to `out`, with the
    /// CRC over the records, and returns how much padding follows them.
    pub(crate) fn write_head(&self, seq: u64, out: &mut Vec<u8>) -> usize {
        write_head(
            seq,
            self.stream,
            self.producer_id,
            self.producer_seq,
            self.records.as_ref().len(),
            self.records_crc,
            out,
        )
    }
}

/// A dedupe entry a new WAL file begins with (section 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupeCarry {
    pub stream: KeyHash,
    pub producer_id: u64,
    pub producer_seq: u64,
    pub first_id: u64,
    pub count: u32,
}

impl DedupeCarry {
    /// Appends it to `out` as one whole entry of the file `seq`.
    pub(crate) fn encode(&self, seq: u64, out: &mut Vec<u8>) {
        let mut record = Vec::new();
        format::encode_dedupe_carry(&mut record, self.first_id, self.count)
            .expect("a carried entry's doc is 12 bytes");
        let entry = encode_wal_entry(
            seq,
            self.stream,
            self.producer_id,
            self.producer_seq,
            &record,
        )
        .expect("a carried entry has one record");
        out.extend_from_slice(&entry);
    }
}

fn check_entry(records: &[u8]) -> Result<(), WalEntryError> {
    if records.is_empty() {
        return Err(WalEntryError::EmptyRecords);
    }
    if u32::try_from(records.len()).is_err() {
        return Err(WalEntryError::RecordsTooLong);
    }
    Ok(())
}

fn write_head(
    seq: u64,
    stream: KeyHash,
    producer_id: u64,
    producer_seq: u64,
    records_len: usize,
    records_crc: u32,
    out: &mut Vec<u8>,
) -> usize {
    let start = out.len();
    out.extend_from_slice(&stream.0);
    out.extend_from_slice(&producer_id.to_le_bytes());
    out.extend_from_slice(&producer_seq.to_le_bytes());
    out.extend_from_slice(&(records_len as u32).to_le_bytes());
    let total = ENTRY_FIXED_BYTES + records_len;
    let padding = total.next_multiple_of(8) - total;
    let mut crc = crc32c_seeded(seq, &out[start..]);
    crc = crc32c_combine(crc, records_crc, records_len);
    crc = crc32c_append(crc, &ZERO_PADDING[..padding]);
    out.extend_from_slice(&crc.to_le_bytes());
    padding
}

/// One whole entry as bytes, for vectors and tests. The writer doesn't use this,
/// because it passes the records to `writev` without copying them.
pub fn encode_wal_entry(
    seq: u64,
    stream: KeyHash,
    producer_id: u64,
    producer_seq: u64,
    records: &[u8],
) -> Result<Vec<u8>, WalEntryError> {
    check_entry(records)?;
    let mut out = Vec::with_capacity(ENTRY_FIXED_BYTES + records.len() + 8);
    let padding = write_head(
        seq,
        stream,
        producer_id,
        producer_seq,
        records.len(),
        crc32c(records),
        &mut out,
    );
    out.extend_from_slice(records);
    out.extend_from_slice(&ZERO_PADDING[..padding]);
    Ok(out)
}
