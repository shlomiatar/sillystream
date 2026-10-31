//! Bytes of the WAL (SPEC section 6.3, D8, D9, D50).
//!
//! A file is `magic u64 | seq u64 | generation u64 | crc32c u32 | 0 u32`, then entries, each
//! starting at a multiple of 8. `generation` is the owner that opened it, 0 if there is none,
//! which is what tells a replay how far to accept its entries (section 8.1, D152):
//!
//! ```text
//! stream_len u16 | stream | producer_id u64 | producer_seq u64 | len u32 | crc32c u32 |
//! records | zero padding up to a multiple of 8
//! ```
//!
//! The CRC is seeded with the file's `seq`, and covers every entry byte except itself.

use std::fmt;
use std::sync::Arc;

use format::{crc32c, crc32c_append, crc32c_seeded};

/// `SSWAL001` as little-endian bytes on disk.
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SSWAL001");
pub const WAL_FILE_HEADER_BYTES: usize = 32;
/// The entry's fixed fields, without the stream name.
pub(crate) const ENTRY_FIXED_BYTES: usize = 2 + 8 + 8 + 4 + 4;
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
    /// A stream id is never empty (D42).
    EmptyStream,
    /// `stream_len` is a u16.
    StreamTooLong,
    /// A publish always has at least one record (D6a).
    EmptyRecords,
    /// `len` is a u32.
    RecordsTooLong,
}

impl fmt::Display for WalEntryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WalEntryError::EmptyStream => "stream id is empty",
            WalEntryError::StreamTooLong => "stream id is over 65,535 bytes",
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
    stream: Arc<str>,
    producer_id: u64,
    producer_seq: u64,
    records: R,
    /// The core to tell when this entry is durable.
    core: usize,
    /// Increases per core. The writer reports the highest durable one.
    ticket: u64,
}

impl<R: AsRef<[u8]>> WalEntry<R> {
    pub fn new(
        stream: Arc<str>,
        producer_id: u64,
        producer_seq: u64,
        records: R,
        core: usize,
        ticket: u64,
    ) -> Result<WalEntry<R>, WalEntryError> {
        check_entry(&stream, records.as_ref())?;
        Ok(WalEntry {
            stream,
            producer_id,
            producer_seq,
            records,
            core,
            ticket,
        })
    }

    pub fn stream(&self) -> &str {
        &self.stream
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

    /// Appends the entry's head (everything before `records`) to `out`, computing
    /// the CRC over the records, and returns how much padding follows them.
    pub(crate) fn write_head(&self, seq: u64, out: &mut Vec<u8>) -> usize {
        write_head(
            seq,
            &self.stream,
            self.producer_id,
            self.producer_seq,
            self.records.as_ref(),
            out,
        )
    }
}

fn check_entry(stream: &str, records: &[u8]) -> Result<(), WalEntryError> {
    if stream.is_empty() {
        return Err(WalEntryError::EmptyStream);
    }
    if stream.len() > usize::from(u16::MAX) {
        return Err(WalEntryError::StreamTooLong);
    }
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
    stream: &str,
    producer_id: u64,
    producer_seq: u64,
    records: &[u8],
    out: &mut Vec<u8>,
) -> usize {
    let start = out.len();
    out.extend_from_slice(&(stream.len() as u16).to_le_bytes());
    out.extend_from_slice(stream.as_bytes());
    out.extend_from_slice(&producer_id.to_le_bytes());
    out.extend_from_slice(&producer_seq.to_le_bytes());
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    let total = ENTRY_FIXED_BYTES + stream.len() + records.len();
    let padding = total.next_multiple_of(8) - total;
    let mut crc = crc32c_seeded(seq, &out[start..]);
    crc = crc32c_append(crc, records);
    crc = crc32c_append(crc, &ZERO_PADDING[..padding]);
    out.extend_from_slice(&crc.to_le_bytes());
    padding
}

/// One whole entry as bytes, for vectors and tests. The writer doesn't use this,
/// because it passes the records to `writev` without copying them.
pub fn encode_wal_entry(
    seq: u64,
    stream: &str,
    producer_id: u64,
    producer_seq: u64,
    records: &[u8],
) -> Result<Vec<u8>, WalEntryError> {
    check_entry(stream, records)?;
    let mut out = Vec::with_capacity(ENTRY_FIXED_BYTES + stream.len() + records.len() + 8);
    let padding = write_head(seq, stream, producer_id, producer_seq, records, &mut out);
    out.extend_from_slice(records);
    out.extend_from_slice(&ZERO_PADDING[..padding]);
    Ok(out)
}
