//! Reading the WAL during recovery (SPEC sections 6.3, 8.1).

use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use format::{crc32c, crc32c_append, crc32c_seeded};

use crate::wal_format::{ENTRY_FIXED_BYTES, WAL_FILE_HEADER_BYTES, WAL_MAGIC};

/// One valid entry found by replay. `records` shares the file's buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayedEntry {
    pub stream: String,
    pub producer_id: u64,
    pub producer_seq: u64,
    pub records: Bytes,
}

/// The valid prefix of one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayedFile {
    pub entries: Vec<ReplayedEntry>,
    /// Where the prefix ends. 0 if the file header itself isn't valid.
    pub valid_end_bytes: usize,
    /// The generation that opened the file. 0 if it had none, or the header isn't valid.
    pub generation: u64,
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut field = [0u8; 8];
    field.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(field)
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut field = [0u8; 4];
    field.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(field)
}

/// Parses the entry at `offset`, returning it and the next entry's offset. `None`
/// for anything other than one whole valid entry, which is where the prefix ends:
/// the zeros of a preallocated tail have `stream_len = 0`. A CRC can't reject bytes
/// a publisher chose, so stale bytes must not be in the file at all (D60); the `seq`
/// seed is what still rejects an old file's entries if they are found.
fn parse_entry(file: &Bytes, offset: usize, seq: u64) -> Option<(ReplayedEntry, usize)> {
    let bytes = &file[offset..];
    if bytes.len() < ENTRY_FIXED_BYTES {
        return None;
    }
    let stream_len = usize::from(u16::from_le_bytes([bytes[0], bytes[1]]));
    if stream_len == 0 || bytes.len() < ENTRY_FIXED_BYTES + stream_len {
        return None;
    }
    let fields_end = 2 + stream_len + 8 + 8 + 4;
    let len = read_u32(bytes, fields_end - 4) as usize;
    if len == 0 {
        return None;
    }
    let records_start = fields_end + 4;
    let records_end = records_start.checked_add(len)?;
    let total = records_end.checked_next_multiple_of(8)?;
    if total > bytes.len() {
        return None;
    }
    let mut crc = crc32c_seeded(seq, &bytes[..fields_end]);
    // The padding as found, not as it should be, so that a flipped padding bit fails.
    crc = crc32c_append(crc, &bytes[records_start..total]);
    if crc != read_u32(bytes, fields_end) {
        return None;
    }
    let stream = std::str::from_utf8(&bytes[2..2 + stream_len]).ok()?;
    let entry = ReplayedEntry {
        stream: stream.to_owned(),
        producer_id: read_u64(bytes, 2 + stream_len),
        producer_seq: read_u64(bytes, 2 + stream_len + 8),
        records: file.slice(offset + records_start..offset + records_end),
    };
    Some((entry, offset + total))
}

/// The longest valid prefix of a file that should have the given `seq`. A file whose
/// header isn't exactly `magic, seq, generation, crc, 0` has no valid entries: this covers a
/// crash between the rename and the header write of a reused file (D8).
pub fn replay_bytes(file: &Bytes, seq: u64) -> ReplayedFile {
    let mut replayed = ReplayedFile {
        entries: Vec::new(),
        valid_end_bytes: 0,
        generation: 0,
    };
    if file.len() < WAL_FILE_HEADER_BYTES
        || read_u64(file, 0) != WAL_MAGIC
        || read_u64(file, 8) != seq
        || read_u32(file, 24) != crc32c(&file[..24])
        || read_u32(file, 28) != 0
    {
        return replayed;
    }
    replayed.generation = read_u64(file, 16);
    let mut offset = WAL_FILE_HEADER_BYTES;
    while let Some((entry, next)) = parse_entry(file, offset, seq) {
        replayed.entries.push(entry);
        offset = next;
    }
    replayed.valid_end_bytes = offset;
    replayed
}

pub fn wal_file_name(seq: u64) -> String {
    format!("wal-{seq:020}.log")
}

fn parse_file_name(name: &str) -> Option<u64> {
    let seq = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if seq.len() != 20 || !seq.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    seq.parse().ok()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFileInfo {
    pub seq: u64,
    pub path: PathBuf,
}

/// The WAL files in `dir`, oldest first. Other files are ignored.
pub fn list_wal_files(dir: &Path) -> io::Result<Vec<WalFileInfo>> {
    let mut files = Vec::new();
    for item in std::fs::read_dir(dir)? {
        let item = item?;
        if let Some(seq) = item.file_name().to_str().and_then(parse_file_name) {
            files.push(WalFileInfo {
                seq,
                path: item.path(),
            });
        }
    }
    files.sort_by_key(|file| file.seq);
    Ok(files)
}

pub fn replay_file(file: &WalFileInfo) -> io::Result<ReplayedFile> {
    let bytes = Bytes::from(std::fs::read(&file.path)?);
    Ok(replay_bytes(&bytes, file.seq))
}

/// The valid prefix of every file, oldest file first. The end of one file's prefix
/// doesn't stop the next file, because a closed file always ends in stale bytes or
/// zeros (Q15).
pub fn replay_dir(dir: &Path) -> io::Result<Vec<ReplayedEntry>> {
    let mut entries = Vec::new();
    for file in list_wal_files(dir)? {
        entries.extend(replay_file(&file)?.entries);
    }
    Ok(entries)
}
