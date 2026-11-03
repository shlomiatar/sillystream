//! Bytes of a segment (SPEC section 7.3, D10, D52, D275).
//!
//! ```text
//! segment  := region* page* last_ids top_index trailer
//! region   := key hash [u8; 16] | body        one stream's, in key hash order from offset 0
//! body     := table block*                    for a segment; a checkpoint keeps metadata here
//! table    := crc32c u32 | block_count u32 | block_count × entry
//! entry    := options_or u64 | options_and u64 | id_first u64 | id_last u64 |
//!             count u64 | bytes u64 | author u64 | author_mixed u32 | crc32c u32
//! block    := whole records, exactly as on the wire
//! page     := crc32c u32 | entry_count u32 | index_entry*
//! index_entry := hash_delta varint | len varint | offset varint, only at every 64th entry
//! last_ids := crc32c u32 | count u32 | count × (key hash [u8; 16] | last_id u64)
//! top_index:= page_count u32 | 0 u32 | data_end u64 | last_ids_offset u64 | last_ids_len u64 |
//!             page_count × (first_hash u64 | first_offset u64 | first_entry u64 |
//!                           offset u64 | len u32 | 0 u32)
//! trailer  := magic u64 | top_offset u64 | top_len u32 | crc32c u32
//! ```
//!
//! An index entry is a region's: its hash is the top 64 bits of the key hash, as a delta from
//! the entry before it in the page (0 for a page's first, which the top index holds whole). A
//! region's offset is the sum of the lengths before it. Every 64th entry of the object also
//! holds it, and the top index holds each page's first. Entries with one hash stay in one page.
//! Varints are LEB128. Each CRC covers the bytes after it in its structure, the trailer's covers
//! the top index and the trailer before it, and a block's covers the block. A table's CRC covers
//! its region's key as well.

use std::fmt;

use format::{BlockAuthor, BlockSummary, FormatError, KeyHash, crc32c, crc32c_append};

/// `SSSEG003` as little-endian bytes on disk.
pub const SEGMENT_MAGIC: u64 = u64::from_le_bytes(*b"SSSEG003");
pub const TRAILER_BYTES: usize = 24;
pub const BLOCK_ENTRY_BYTES: usize = 64;
pub(crate) const TABLE_HEAD_BYTES: usize = 8;
pub(crate) const PAGE_HEAD_BYTES: usize = 8;
const TOP_HEAD_BYTES: usize = 32;
const TOP_ENTRY_BYTES: usize = 40;
const LAST_ID_BYTES: usize = KeyHash::BYTES + 8;
/// Every this many index entries, one holds its region's offset whole.
pub(crate) const OFFSET_EVERY: u64 = 64;
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

/// A region's table. Its CRC covers the region's key too.
pub(crate) fn encode_block_table(stream: KeyHash, blocks: &[BlockEntry]) -> Vec<u8> {
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
    let crc = crc32c_append(crc32c(&stream.0), &out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_block_table(stream: KeyHash, bytes: &[u8]) -> Result<BlockTable, SegmentError> {
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
    if crc32c_append(crc32c(&stream.0), &bytes[4..]) != u32_at(bytes, 0) {
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

/// Where one stream's region is, and what it holds. Keeping one of these is the "stored
/// extent" that lets a reader skip the footer. `offset` is the region's, at its key hash.
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

    /// Where its blocks start.
    pub(crate) fn blocks_offset(&self) -> u64 {
        self.offset + KeyHash::BYTES as u64 + self.table_bytes()
    }
}

/// One region as the index has it: the top 64 bits of its key hash, where it is and how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub hash: u64,
    pub offset: u64,
    pub len_bytes: u64,
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn varint_at(bytes: &[u8], at: &mut usize) -> Result<u64, SegmentError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(*at)
            .ok_or(SegmentError::Corrupt("index entry is truncated"))?;
        *at += 1;
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(SegmentError::Corrupt("varint is too long"))
}

/// Encodes one page of entries in order, the first being entry `first_entry` of the object.
pub(crate) fn encode_page(entries: &[IndexEntry], first_entry: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(PAGE_HEAD_BYTES + entries.len() * 10);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    let mut previous = entries.first().map_or(0, |entry| entry.hash);
    for (at, entry) in entries.iter().enumerate() {
        put_varint(&mut out, entry.hash - previous);
        put_varint(&mut out, entry.len_bytes);
        if (first_entry + at as u64).is_multiple_of(OFFSET_EVERY) {
            put_varint(&mut out, entry.offset);
        }
        previous = entry.hash;
    }
    let crc = crc32c(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

/// Decodes the page `page` describes.
pub(crate) fn decode_page(bytes: &[u8], page: &TopEntry) -> Result<Vec<IndexEntry>, SegmentError> {
    if bytes.len() < PAGE_HEAD_BYTES {
        return Err(SegmentError::Corrupt("page is truncated"));
    }
    if crc32c(&bytes[4..]) != u32_at(bytes, 0) {
        return Err(SegmentError::Crc("footer page"));
    }
    let count = u32_at(bytes, 4) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut at = PAGE_HEAD_BYTES;
    let (mut hash, mut offset) = (page.first_hash, page.first_offset);
    for index in 0..count as u64 {
        hash = hash
            .checked_add(varint_at(bytes, &mut at)?)
            .ok_or(SegmentError::Corrupt("index hash"))?;
        let len_bytes = varint_at(bytes, &mut at)?;
        if (page.first_entry + index).is_multiple_of(OFFSET_EVERY)
            && varint_at(bytes, &mut at)? != offset
        {
            return Err(SegmentError::Corrupt("index offsets don't add up"));
        }
        entries.push(IndexEntry {
            hash,
            offset,
            len_bytes,
        });
        offset = offset
            .checked_add(len_bytes)
            .ok_or(SegmentError::Corrupt("index offset"))?;
    }
    if at != bytes.len() {
        return Err(SegmentError::Corrupt("page has the wrong length"));
    }
    if offset > page.data_end {
        return Err(SegmentError::Corrupt("region is past the data"));
    }
    Ok(entries)
}

/// Each region's full key hash and last id, in region order (section 7.3).
pub(crate) fn encode_last_ids(last_ids: &[(KeyHash, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + last_ids.len() * LAST_ID_BYTES);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(last_ids.len() as u32).to_le_bytes());
    for (key, last_id) in last_ids {
        out.extend_from_slice(&key.0);
        out.extend_from_slice(&last_id.to_le_bytes());
    }
    let crc = crc32c(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_last_ids(bytes: &[u8]) -> Result<Vec<(KeyHash, u64)>, SegmentError> {
    if bytes.len() < 8 {
        return Err(SegmentError::Corrupt("last ids are truncated"));
    }
    if crc32c(&bytes[4..]) != u32_at(bytes, 0) {
        return Err(SegmentError::Crc("last ids"));
    }
    let count = u32_at(bytes, 4) as usize;
    if count
        .checked_mul(LAST_ID_BYTES)
        .and_then(|body| body.checked_add(8))
        != Some(bytes.len())
    {
        return Err(SegmentError::Corrupt("last ids have the wrong length"));
    }
    Ok(bytes[8..]
        .as_chunks::<LAST_ID_BYTES>()
        .0
        .iter()
        .map(|entry| {
            let key = KeyHash::from_slice(&entry[..KeyHash::BYTES]).unwrap();
            (key, u64_at(entry, KeyHash::BYTES))
        })
        .collect())
}

/// The small index over the footer pages. A reader fetches it once and caches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopIndex {
    pages: Vec<TopEntry>,
    /// Where the regions end and the pages start.
    pub(crate) data_end: u64,
    pub(crate) last_ids_offset: u64,
    pub(crate) last_ids_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopEntry {
    pub(crate) first_hash: u64,
    pub(crate) first_offset: u64,
    pub(crate) first_entry: u64,
    pub(crate) offset: u64,
    pub(crate) len_bytes: u32,
    /// The top index's, so a page knows where the regions end.
    pub(crate) data_end: u64,
}

impl TopIndex {
    pub(crate) fn new(
        pages: Vec<TopEntry>,
        data_end: u64,
        last_ids_offset: u64,
        last_ids_len: u64,
    ) -> TopIndex {
        TopIndex {
            pages,
            data_end,
            last_ids_offset,
            last_ids_len,
        }
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

pub(crate) fn encode_footer(top: &TopIndex, top_offset: u64) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(TOP_HEAD_BYTES + top.pages.len() * TOP_ENTRY_BYTES + TRAILER_BYTES);
    out.extend_from_slice(&(top.pages.len() as u32).to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    for field in [top.data_end, top.last_ids_offset, top.last_ids_len] {
        out.extend_from_slice(&field.to_le_bytes());
    }
    for page in &top.pages {
        for field in [
            page.first_hash,
            page.first_offset,
            page.first_entry,
            page.offset,
        ] {
            out.extend_from_slice(&field.to_le_bytes());
        }
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
    let (data_end, last_ids_offset, last_ids_len) =
        (u64_at(top, 8), u64_at(top, 16), u64_at(top, 24));
    if last_ids_offset.saturating_add(last_ids_len) > trailer.top_offset
        || data_end > last_ids_offset
    {
        return Err(SegmentError::Corrupt("last ids are past the top index"));
    }
    let mut pages = Vec::with_capacity(count);
    for entry in top[TOP_HEAD_BYTES..].as_chunks::<TOP_ENTRY_BYTES>().0 {
        let page = TopEntry {
            first_hash: u64_at(entry, 0),
            first_offset: u64_at(entry, 8),
            first_entry: u64_at(entry, 16),
            offset: u64_at(entry, 24),
            len_bytes: u32_at(entry, 32),
            data_end,
        };
        if page.offset < data_end
            || page.offset.saturating_add(u64::from(page.len_bytes)) > last_ids_offset
        {
            return Err(SegmentError::Corrupt("page is outside the index"));
        }
        pages.push(page);
    }
    Ok(TopIndex::new(
        pages,
        data_end,
        last_ids_offset,
        last_ids_len,
    ))
}
