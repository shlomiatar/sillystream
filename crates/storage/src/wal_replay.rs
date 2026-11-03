//! Reading the WAL during recovery (SPEC sections 6.3, 8.1).

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use format::{KeyHash, crc32c, crc32c_append, crc32c_seeded};

use crate::wal_format::{ENTRY_FIXED_BYTES, WAL_FILE_HEADER_BYTES, WAL_MAGIC};

/// One valid entry found by replay. `records` shares the file's buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayedEntry {
    pub stream: KeyHash,
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
    /// Bytes other than zeros lie past the prefix. A reused file is emptied first (D60), so these
    /// are a torn write or damage, and anything after them in this file can't be read (D252).
    pub damaged_tail: bool,
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
/// the zeros of a preallocated tail have `len = 0`. A CRC can't reject bytes
/// a publisher chose, so stale bytes must not be in the file at all (D60); the `seq`
/// seed is what still rejects an old file's entries if they are found.
fn parse_entry(file: &Bytes, offset: usize, seq: u64) -> Option<(ReplayedEntry, usize)> {
    let bytes = &file[offset..];
    if bytes.len() < ENTRY_FIXED_BYTES {
        return None;
    }
    let fields_end = ENTRY_FIXED_BYTES - 4;
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
    let entry = ReplayedEntry {
        stream: KeyHash::from_slice(&bytes[..KeyHash::BYTES])?,
        producer_id: read_u64(bytes, KeyHash::BYTES),
        producer_seq: read_u64(bytes, KeyHash::BYTES + 8),
        records: file.slice(offset + records_start..offset + records_end),
    };
    Some((entry, offset + total))
}

/// Whether any byte is set. An OR over a whole chunk has no early exit, so it compiles to SIMD.
fn any_nonzero(bytes: &[u8]) -> bool {
    bytes
        .chunks(64 * 1024)
        .any(|chunk| chunk.iter().fold(0u8, |set, byte| set | byte) != 0)
}

/// `any_nonzero` of `bytes[start..]`, the whole of `file`, without touching its holes: a
/// preallocated tail is one (D51), and reading it would fault in every page.
fn any_nonzero_in_data(file: &File, bytes: &[u8], start: usize) -> bool {
    let fd = file.as_raw_fd();
    let mut offset = start;
    while offset < bytes.len() {
        // SAFETY: `lseek` takes no pointers, and `file` keeps `fd` open.
        let data = unsafe { libc::lseek(fd, offset as libc::off_t, libc::SEEK_DATA) };
        if data < 0 {
            return match io::Error::last_os_error().raw_os_error() {
                // Only a hole is left.
                Some(libc::ENXIO) => false,
                _ => any_nonzero(&bytes[offset..]),
            };
        }
        // SAFETY: as above.
        let hole = unsafe { libc::lseek(fd, data, libc::SEEK_HOLE) };
        let end = if hole < 0 { bytes.len() } else { hole as usize };
        let (data, end) = (data as usize, end.min(bytes.len()));
        if data >= end {
            return false;
        }
        if any_nonzero(&bytes[data..end]) {
            return true;
        }
        offset = end;
    }
    false
}

/// The longest valid prefix of a file that should have the given `seq`. A file whose
/// header isn't exactly `magic, seq, generation, crc, 0` has no valid entries: this covers a
/// crash between the rename and the header write of a reused file (D8).
pub fn replay_bytes(file: &Bytes, seq: u64) -> ReplayedFile {
    let mut replayed = parse_prefix(file, seq);
    replayed.damaged_tail = any_nonzero(&file[replayed.valid_end_bytes..]);
    replayed
}

/// The prefix alone, with `damaged_tail` left for the caller.
fn parse_prefix(file: &Bytes, seq: u64) -> ReplayedFile {
    let mut replayed = ReplayedFile {
        entries: Vec::new(),
        valid_end_bytes: 0,
        generation: 0,
        damaged_tail: false,
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

/// Makes every file in `files`, and `dir` itself, durable. A crashed process's writes may reach
/// no further than the page cache, and a replayed entry can be acked at level 2 (section 7.2).
/// A read-only handle is enough, since a sync covers the file and not the handle's own writes.
pub fn sync_wal_files(dir: &Path, files: &[WalFileInfo]) -> io::Result<()> {
    for file in files {
        std::fs::File::open(&file.path)?.sync_data()?;
    }
    std::fs::File::open(dir)?.sync_all()
}

/// Replays a file through a map of it, so that its entries cost page cache the kernel can take
/// back and not heap (D263).
pub fn replay_file(file: &WalFileInfo) -> io::Result<ReplayedFile> {
    let handle = File::open(&file.path)?;
    if handle.metadata()?.len() == 0 {
        return Ok(replay_bytes(&Bytes::new(), file.seq));
    }
    // SAFETY: nothing truncates or writes a file while its map lives. Only the writer touches
    // this directory, and it removes files from before a restart instead of reusing them. A
    // read the disk fails ends the process, as a failed WAL sync does.
    let map = unsafe { memmap2::Mmap::map(&handle) }?;
    map.advise(memmap2::Advice::Sequential)?;
    let bytes = Bytes::from_owner(map);
    let mut replayed = parse_prefix(&bytes, file.seq);
    replayed.damaged_tail = any_nonzero_in_data(&handle, &bytes, replayed.valid_end_bytes);
    Ok(replayed)
}

/// `replay_file` of each of `files`, on every core, in their order.
pub fn replay_files(files: &[WalFileInfo]) -> Vec<io::Result<ReplayedFile>> {
    let next = AtomicUsize::new(0);
    let slots: Vec<Mutex<Option<io::Result<ReplayedFile>>>> =
        files.iter().map(|_| Mutex::new(None)).collect();
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(files.len().max(1));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(file) = files.get(index) else {
                        return;
                    };
                    *slots[index].lock().unwrap() = Some(replay_file(file));
                }
            });
        }
    });
    slots
        .into_iter()
        .map(|slot| slot.into_inner().unwrap().expect("every file was taken"))
        .collect()
}

/// The valid prefix of every file, oldest file first. The end of one file's prefix
/// doesn't stop the next file, because a closed file ends in zeros, or in damage that
/// recovery warns about (Q15, D252).
pub fn replay_dir(dir: &Path) -> io::Result<Vec<ReplayedEntry>> {
    let mut entries = Vec::new();
    for file in list_wal_files(dir)? {
        entries.extend(replay_file(&file)?.entries);
    }
    Ok(entries)
}
