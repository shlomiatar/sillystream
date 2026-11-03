//! Bytes of a segment (SPEC section 7.3, D10, D52).
//!
//! ```text
//! segment  := extent* page* top_index trailer
//! extent   := table block*                   one stream's records in one section
//! table    := crc32c u32 | block_count u32 | block_count × entry
//! entry    := options_or u64 | options_and u64 | id_first u64 | id_last u64 |
//!             count u64 | bytes u64 | author u64 | author_mixed u32 | crc32c u32
//! block    := whole records, exactly as on the wire
//! page     := crc32c u32 | entry_count u32 | entry_count × index_entry
//! index_entry := hash u64 | offset u64 | len u64 | id_first u64 | id_last u64 |
//!             block_count u32 | 0 u32 | stream key hash [u8; 16]
//! top_index:= page_count u32 | 0 u32 | page_count × (first_hash u64 | offset u64 | len u32 | 0 u32)
//! trailer  := magic u64 | top_offset u64 | top_len u32 | crc32c u32
//! ```
//!
//! Each CRC covers the bytes after it in its structure, the trailer's covers
//! the top index and the trailer before it, and a block's covers the block.

use std::fmt;

use format::{BlockAuthor, BlockSummary, FormatError, KeyHash, crc32c};

/// `SSSEG002` as little-endian bytes on disk.
pub const SEGMENT_MAGIC: u64 = u64::from_le_bytes(*b"SSSEG002");
pub const TRAILER_BYTES: usize = 24;
pub const BLOCK_ENTRY_BYTES: usize = 64;
pub(crate) const TABLE_HEAD_BYTES: usize = 8;
pub(crate) const PAGE_HEAD_BYTES: usize = 8;
pub(crate) const INDEX_ENTRY_BYTES: usize = 48 + KeyHash::BYTES;
const TOP_HEAD_BYTES: usize = 8;
const TOP_ENTRY_BYTES: usize = 24;
/// D6d.
pub const DEFAULT_BLOCK_BYTES: usize = 64 * 1024;
/// D6d.
pub const DEFAULT_PAGE_BYTES: usize = 64 * 1024;

pub fn stream_hash(stream: KeyHash) -> u64 {
    stream.top64()
}

#[derive(Debug)]
pub enum SegmentError {
    Store(object_store::Error),
    /// A CRC didn't match. The name says which structure.
    Crc(&'static str),
    /// Bytes that passed no CRC, or don't fit together.
    Corrupt(&'static str),
    /// Bytes given to the builder aren't whole stamped records.
    Records(FormatError),
    /// A section isn't sorted by stream, or a stream's ids don't increase.
    OutOfOrder(&'static str),
    /// Name or sizes don't fit the format.
    TooLong(&'static str),
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SegmentError::Store(error) => write!(f, "object store: {error}"),
            SegmentError::Crc(what) => write!(f, "CRC mismatch in {what}"),
            SegmentError::Corrupt(what) => write!(f, "corrupt segment: {what}"),
            SegmentError::Records(error) => write!(f, "bad records: {error}"),
            SegmentError::OutOfOrder(what) => write!(f, "out of order: {what}"),
            SegmentError::TooLong(what) => write!(f, "too long: {what}"),
        }
    }
}

impl std::error::Error for SegmentError {}

impl From<object_store::Error> for SegmentError {
    fn from(error: object_store::Error) -> Self {
        SegmentError::Store(error)
    }
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    let mut field = [0u8; 8];
    field.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(field)
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    let mut field = [0u8; 4];
    field.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(field)
}

/// One block's summary and CRC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockEntry {
    pub summary: BlockSummary,
    pub crc32c: u32,
}

/// One extent's summary table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTable {
    pub blocks: Vec<BlockEntry>,
}

pub(crate) fn encode_block_table(blocks: &[BlockEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(TABLE_HEAD_BYTES + blocks.len() * BLOCK_ENTRY_BYTES);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for block in blocks {
        let summary = &block.summary;
        for field in [
            summary.options_or,
            summary.options_and,
            summary.id_first,
            summary.id_last,
            summary.count,
            summary.bytes,
        ] {
            out.extend_from_slice(&field.to_le_bytes());
        }
        let (author, mixed) = match summary.author {
            BlockAuthor::Single(author) => (author, 0u32),
            BlockAuthor::Mixed => (0, 1u32),
        };
        out.extend_from_slice(&author.to_le_bytes());
        out.extend_from_slice(&mixed.to_le_bytes());
        out.extend_from_slice(&block.crc32c.to_le_bytes());
    }
    let crc = crc32c(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_block_table(bytes: &[u8]) -> Result<BlockTable, SegmentError> {
    if bytes.len() < TABLE_HEAD_BYTES {
        return Err(SegmentError::Corrupt("block table is truncated"));
    }
    let count = u32_at(bytes, 4) as usize;
    if count
        .checked_mul(BLOCK_ENTRY_BYTES)
        .and_then(|body| body.checked_add(TABLE_HEAD_BYTES))
        != Some(bytes.len())
    {
        return Err(SegmentError::Corrupt("block table has the wrong length"));
    }
    if crc32c(&bytes[4..]) != u32_at(bytes, 0) {
        return Err(SegmentError::Crc("block table"));
    }
    let mut blocks = Vec::with_capacity(count);
    for entry in bytes[TABLE_HEAD_BYTES..].as_chunks::<BLOCK_ENTRY_BYTES>().0 {
        let author = match u32_at(entry, 56) {
            0 => BlockAuthor::Single(u64_at(entry, 48)),
            1 => BlockAuthor::Mixed,
            _ => return Err(SegmentError::Corrupt("author_mixed is not 0 or 1")),
        };
        blocks.push(BlockEntry {
            summary: BlockSummary {
                options_or: u64_at(entry, 0),
                options_and: u64_at(entry, 8),
                id_first: u64_at(entry, 16),
                id_last: u64_at(entry, 24),
                count: u64_at(entry, 32),
                bytes: u64_at(entry, 40),
                author,
            },
            crc32c: u32_at(entry, 60),
        });
    }
    Ok(BlockTable { blocks })
}

/// Where one stream's extent is: the stream index entry of section 7.3. Keeping one
/// of these is the "stored extent" that lets a reader skip the footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamExtent {
    pub stream: KeyHash,
    pub offset: u64,
    pub len_bytes: u64,
    pub id_first: u64,
    pub id_last: u64,
    pub block_count: u32,
}

impl StreamExtent {
    pub(crate) fn table_bytes(&self) -> u64 {
        TABLE_HEAD_BYTES as u64 + u64::from(self.block_count) * BLOCK_ENTRY_BYTES as u64
    }
}

/// Encodes one page from entries already sorted by key hash.
pub(crate) fn encode_page(entries: &[(u64, &StreamExtent)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PAGE_HEAD_BYTES + entries.len() * INDEX_ENTRY_BYTES);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (hash, extent) in entries {
        for field in [
            *hash,
            extent.offset,
            extent.len_bytes,
            extent.id_first,
            extent.id_last,
        ] {
            out.extend_from_slice(&field.to_le_bytes());
        }
        out.extend_from_slice(&extent.block_count.to_le_bytes());
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&extent.stream.0);
    }
    let crc = crc32c(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Decodes a page, keeping the extent of `only` when it is given, and every extent otherwise.
pub fn decode_page(
    bytes: &[u8],
    only: Option<KeyHash>,
) -> Result<Vec<(u64, StreamExtent)>, SegmentError> {
    if bytes.len() < PAGE_HEAD_BYTES {
        return Err(SegmentError::Corrupt("page is truncated"));
    }
    if crc32c(&bytes[4..]) != u32_at(bytes, 0) {
        return Err(SegmentError::Crc("footer page"));
    }
    let count = u32_at(bytes, 4) as usize;
    if count.checked_mul(INDEX_ENTRY_BYTES) != Some(bytes.len() - PAGE_HEAD_BYTES) {
        return Err(SegmentError::Corrupt("page has the wrong length"));
    }
    let mut entries = Vec::new();
    for fixed in bytes[PAGE_HEAD_BYTES..].as_chunks::<INDEX_ENTRY_BYTES>().0 {
        let stream = KeyHash::from_slice(&fixed[48..]).unwrap();
        if only.is_none_or(|wanted| wanted == stream) {
            entries.push((
                u64_at(fixed, 0),
                StreamExtent {
                    stream,
                    offset: u64_at(fixed, 8),
                    len_bytes: u64_at(fixed, 16),
                    id_first: u64_at(fixed, 24),
                    id_last: u64_at(fixed, 32),
                    block_count: u32_at(fixed, 40),
                },
            ));
        }
    }
    Ok(entries)
}

/// The small index over the footer pages. A reader fetches it once and caches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopIndex {
    pages: Vec<TopEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopEntry {
    pub(crate) first_hash: u64,
    pub(crate) offset: u64,
    pub(crate) len_bytes: u32,
}

impl TopIndex {
    pub(crate) fn from_pages(pages: Vec<TopEntry>) -> TopIndex {
        TopIndex { pages }
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// The page that holds `hash`, if any. All entries with one hash are in one page,
    /// which is what keeps a lookup at one page read (D52).
    pub(crate) fn page_for(&self, hash: u64) -> Option<TopEntry> {
        let after = self.pages.partition_point(|page| page.first_hash <= hash);
        after.checked_sub(1).map(|index| self.pages[index])
    }

    pub(crate) fn pages(&self) -> &[TopEntry] {
        &self.pages
    }
}

pub(crate) fn encode_footer(pages: &[TopEntry], top_offset: u64) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(TOP_HEAD_BYTES + pages.len() * TOP_ENTRY_BYTES + TRAILER_BYTES);
    out.extend_from_slice(&(pages.len() as u32).to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    for page in pages {
        out.extend_from_slice(&page.first_hash.to_le_bytes());
        out.extend_from_slice(&page.offset.to_le_bytes());
        out.extend_from_slice(&page.len_bytes.to_le_bytes());
        out.extend_from_slice(&[0; 4]);
    }
    let top_len = out.len() as u32;
    out.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    out.extend_from_slice(&top_offset.to_le_bytes());
    out.extend_from_slice(&top_len.to_le_bytes());
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// What the last 24 bytes of a segment say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub(crate) top_offset: u64,
    pub(crate) top_len: u32,
    pub(crate) crc32c: u32,
}

pub(crate) fn decode_trailer(bytes: &[u8]) -> Result<Trailer, SegmentError> {
    if bytes.len() != TRAILER_BYTES {
        return Err(SegmentError::Corrupt("trailer is truncated"));
    }
    if u64_at(bytes, 0) != SEGMENT_MAGIC {
        return Err(SegmentError::Corrupt("trailer has the wrong magic"));
    }
    Ok(Trailer {
        top_offset: u64_at(bytes, 8),
        top_len: u32_at(bytes, 16),
        crc32c: u32_at(bytes, 20),
    })
}

/// Decodes the bytes `top_index | trailer`, checking the trailer's CRC.
pub fn decode_top_index(bytes: &[u8]) -> Result<TopIndex, SegmentError> {
    if bytes.len() < TRAILER_BYTES {
        return Err(SegmentError::Corrupt("footer is truncated"));
    }
    let (top, trailer_bytes) = bytes.split_at(bytes.len() - TRAILER_BYTES);
    let trailer = decode_trailer(trailer_bytes)?;
    if crc32c(&bytes[..bytes.len() - 4]) != trailer.crc32c {
        return Err(SegmentError::Crc("top index"));
    }
    if trailer.top_len as usize != top.len() || top.len() < TOP_HEAD_BYTES {
        return Err(SegmentError::Corrupt("top index has the wrong length"));
    }
    let count = u32_at(top, 0) as usize;
    if count.checked_mul(TOP_ENTRY_BYTES) != Some(top.len() - TOP_HEAD_BYTES) {
        return Err(SegmentError::Corrupt("top index has the wrong length"));
    }
    let mut pages = Vec::with_capacity(count);
    for entry in top[TOP_HEAD_BYTES..].as_chunks::<TOP_ENTRY_BYTES>().0 {
        let page = TopEntry {
            first_hash: u64_at(entry, 0),
            offset: u64_at(entry, 8),
            len_bytes: u32_at(entry, 16),
        };
        if page.offset.saturating_add(u64::from(page.len_bytes)) > trailer.top_offset {
            return Err(SegmentError::Corrupt("page is past the top index"));
        }
        pages.push(page);
    }
    Ok(TopIndex { pages })
}
