//! Reads a segment with range reads only (SPEC section 7.3). Readers never scan.
//!
//! Reading a stream is one tail read for the top index, which the caller caches, one read
//! for the page and one for the region. A caller holding a `StreamExtent` reads the region
//! alone, or its table and then the blocks a filter can't rule out.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use format::{Decision, Filter, KeyHash, crc32c};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};

use crate::segment_format::{
    BLOCK_ENTRY_BYTES, BlockTable, IndexEntry, SegmentError, StreamExtent, TABLE_HEAD_BYTES,
    TRAILER_BYTES, TopIndex, decode_block_table, decode_last_ids, decode_page, decode_top_index,
    decode_trailer, stream_hash,
};

/// The tail read is big enough for the top index of a segment under the 128 MB cap of
/// section 7.3 with normal stream ids, so the footer normally takes one read. Nothing
/// enforces that cap yet (Q18). A larger top index takes a second read for the rest
/// (D52).
pub const SEGMENT_TAIL_READ_BYTES: usize = 256 * 1024;

/// A lookup's first read of a region: its key and a table of up to 63 blocks. A longer
/// table takes a second read for the rest.
const REGION_HEAD_READ_BYTES: u64 = 4096;

/// The region in `bytes`, which start at its key and hold at least its key and table.
fn extent_of(bytes: &[u8], entry: &IndexEntry) -> Result<(StreamExtent, BlockTable), SegmentError> {
    let stream = KeyHash::from_slice(&bytes[..KeyHash::BYTES])
        .ok_or(SegmentError::Corrupt("region is truncated"))?;
    let table_start = KeyHash::BYTES;
    let count = bytes
        .get(table_start + 4..table_start + TABLE_HEAD_BYTES)
        .ok_or(SegmentError::Corrupt("region is truncated"))?;
    let count = u32::from_le_bytes([count[0], count[1], count[2], count[3]]);
    let table_end = table_start + TABLE_HEAD_BYTES + count as usize * BLOCK_ENTRY_BYTES;
    let table = decode_block_table(
        stream,
        bytes
            .get(table_start..table_end)
            .ok_or(SegmentError::Corrupt("region is truncated"))?,
    )?;
    let (Some(first), Some(last)) = (table.blocks.first(), table.blocks.last()) else {
        return Err(SegmentError::Corrupt("region has no blocks"));
    };
    let extent = StreamExtent {
        stream,
        offset: entry.offset,
        len_bytes: entry.len_bytes,
        id_first: first.summary.id_first,
        id_last: last.summary.id_last,
        block_count: count,
    };
    check_table(&extent, &table)?;
    Ok((extent, table))
}

/// A whole region's records, checking its table and every block's CRC.
fn records_of(region: &Bytes, entry: &IndexEntry) -> Result<(StreamExtent, Bytes), SegmentError> {
    if region.len() as u64 != entry.len_bytes {
        return Err(SegmentError::Corrupt("range read came back short"));
    }
    let (extent, table) = extent_of(region, entry)?;
    let mut offset = (extent.blocks_offset() - extent.offset) as usize;
    let records = region.slice(offset..);
    for block in &table.blocks {
        let end = offset + block.summary.bytes as usize;
        if crc32c(&region[offset..end]) != block.crc32c {
            return Err(SegmentError::Crc("block"));
        }
        offset = end;
    }
    Ok((extent, records))
}

fn check_table(extent: &StreamExtent, table: &BlockTable) -> Result<(), SegmentError> {
    let blocks_bytes = table
        .blocks
        .iter()
        .try_fold(0u64, |sum, block| sum.checked_add(block.summary.bytes));
    if blocks_bytes.and_then(|sum| sum.checked_add(KeyHash::BYTES as u64 + extent.table_bytes()))
        != Some(extent.len_bytes)
    {
        return Err(SegmentError::Corrupt("table doesn't add up to the region"));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SegmentReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl SegmentReader {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> SegmentReader {
        SegmentReader { store, path }
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SegmentError> {
        let start = usize::try_from(range.start).map_err(|_| SegmentError::Corrupt("offset"))?;
        let end = usize::try_from(range.end).map_err(|_| SegmentError::Corrupt("offset"))?;
        // Stores disagree on an empty range (one returns nothing, another an error).
        if start == end {
            return Ok(Bytes::new());
        }
        let bytes = self.store.get_range(&self.path, start..end).await?;
        if bytes.len() != end - start {
            return Err(SegmentError::Corrupt("range read came back short"));
        }
        Ok(bytes)
    }

    /// Fetches the top index. Callers keep the result (section 7.3).
    pub async fn read_top_index(&self) -> Result<TopIndex, SegmentError> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(SEGMENT_TAIL_READ_BYTES)),
            ..GetOptions::default()
        };
        let tail = self
            .store
            .get_opts(&self.path, options)
            .await?
            .bytes()
            .await?;
        if tail.len() < TRAILER_BYTES {
            return Err(SegmentError::Corrupt("object is shorter than a trailer"));
        }
        let trailer = decode_trailer(&tail[tail.len() - TRAILER_BYTES..])?;
        let footer_len = trailer.top_len as usize + TRAILER_BYTES;
        if footer_len <= tail.len() {
            return decode_top_index(&tail[tail.len() - footer_len..]);
        }
        // Only a very large footer gets here.
        let start = trailer.top_offset;
        let end = start
            .checked_add(footer_len as u64)
            .ok_or(SegmentError::Corrupt("top index offset"))?;
        let footer = self.read_range(start..end).await?;
        decode_top_index(&footer)
    }

    /// The index entries that may be `stream`'s: those with the top 64 bits of its key hash,
    /// from one page read. Only the region's key says which, if any, it is.
    pub(crate) async fn candidates(
        &self,
        top: &TopIndex,
        stream: KeyHash,
    ) -> Result<Vec<IndexEntry>, SegmentError> {
        let hash = stream_hash(stream);
        let Some(page) = top.page_for(hash) else {
            return Ok(Vec::new());
        };
        let bytes = self
            .read_range(page.offset..page.offset + u64::from(page.len_bytes))
            .await?;
        let mut entries = decode_page(&bytes, &page)?;
        entries.retain(|entry| entry.hash == hash);
        Ok(entries)
    }

    /// `stream`'s region, if it has one: a page read, then one for the region's key and table.
    /// A stream has at most one.
    pub async fn lookup(
        &self,
        top: &TopIndex,
        stream: KeyHash,
    ) -> Result<Vec<StreamExtent>, SegmentError> {
        for entry in self.candidates(top, stream).await? {
            // Read whole even when the key is another's: a key the CRC fails is corruption, not
            // a miss.
            let (extent, _) = self.read_head(&entry).await?;
            if extent.stream == stream {
                return Ok(vec![extent]);
            }
        }
        Ok(Vec::new())
    }

    /// A region's key and table: one read, or two for a table of more than 63 blocks.
    async fn read_head(
        &self,
        entry: &IndexEntry,
    ) -> Result<(StreamExtent, BlockTable), SegmentError> {
        let head_end = entry.offset + entry.len_bytes.min(REGION_HEAD_READ_BYTES);
        let head = self.read_range(entry.offset..head_end).await?;
        let table_end = KeyHash::BYTES + TABLE_HEAD_BYTES;
        let Some(count) = head.get(table_end - 4..table_end) else {
            return Err(SegmentError::Corrupt("region is truncated"));
        };
        let count = u32::from_le_bytes([count[0], count[1], count[2], count[3]]);
        let needed = (table_end + count as usize * BLOCK_ENTRY_BYTES) as u64;
        if needed > entry.len_bytes {
            return Err(SegmentError::Corrupt("table is past the region"));
        }
        if needed <= head.len() as u64 {
            return extent_of(&head, entry);
        }
        let rest = self.read_range(head_end..entry.offset + needed).await?;
        extent_of(&[head, rest].concat(), entry)
    }

    /// Every region's key and table, without its blocks: one read of the index and last ids,
    /// then one per region. For the startup scan of unrecorded objects (section 7.3).
    pub async fn read_heads(
        &self,
        top: &TopIndex,
    ) -> Result<Vec<(StreamExtent, BlockTable)>, SegmentError> {
        let start = top.data_end;
        let bytes = self
            .read_range(start..top.last_ids_offset + top.last_ids_len)
            .await?;
        let last_ids = decode_last_ids(&bytes[(top.last_ids_offset - start) as usize..])?;
        let mut entries = Vec::new();
        for page in top.pages() {
            let at = (page.offset - start) as usize;
            let page_bytes = bytes
                .get(at..at + page.len_bytes as usize)
                .ok_or(SegmentError::Corrupt("page is outside the index"))?;
            entries.extend(decode_page(page_bytes, page)?);
        }
        if last_ids.len() != entries.len() {
            return Err(SegmentError::Corrupt("last ids don't match the index"));
        }
        let mut heads = Vec::with_capacity(entries.len());
        for (entry, (stream, last_id)) in entries.iter().zip(last_ids) {
            let (extent, table) = self.read_head(entry).await?;
            if extent.stream != stream || extent.id_last != last_id {
                return Err(SegmentError::Corrupt("region doesn't match its last id"));
            }
            heads.push((extent, table));
        }
        Ok(heads)
    }

    /// Every index entry, in order, with the regions' bytes and their last ids: one read from
    /// offset 0 through the last ids. For compaction, recovery, merging checkpoints and tests,
    /// until objects are found through the object table (D275).
    pub(crate) async fn read_all(
        &self,
        top: &TopIndex,
    ) -> Result<(Vec<IndexEntry>, Bytes, Vec<(KeyHash, u64)>), SegmentError> {
        let last_ids_end = top.last_ids_offset + top.last_ids_len;
        let bytes = self.read_range(0..last_ids_end).await?;
        let last_ids = decode_last_ids(&bytes[top.last_ids_offset as usize..])?;
        let mut entries = Vec::new();
        for page in top.pages() {
            let start = page.offset as usize;
            let page_bytes = bytes
                .get(start..start + page.len_bytes as usize)
                .ok_or(SegmentError::Corrupt("page is outside the index"))?;
            entries.extend(decode_page(page_bytes, page)?);
        }
        if last_ids.len() != entries.len()
            || last_ids
                .iter()
                .zip(&entries)
                .any(|((stream, _), entry)| stream_hash(*stream) != entry.hash)
        {
            return Err(SegmentError::Corrupt("last ids don't match the index"));
        }
        Ok((entries, bytes.slice(..top.data_end as usize), last_ids))
    }

    /// Every region in the segment, from one read. A reader looking for one stream uses
    /// `lookup`.
    pub async fn all_extents(&self, top: &TopIndex) -> Result<Vec<StreamExtent>, SegmentError> {
        let (entries, data, last_ids) = self.read_all(top).await?;
        let mut extents = Vec::with_capacity(entries.len());
        for (entry, (stream, last_id)) in entries.iter().zip(last_ids) {
            let region = data
                .get(entry.offset as usize..(entry.offset + entry.len_bytes) as usize)
                .ok_or(SegmentError::Corrupt("region is past the data"))?;
            let (extent, _) = extent_of(region, entry)?;
            if extent.stream != stream || extent.id_last != last_id {
                return Err(SegmentError::Corrupt("region doesn't match its last id"));
            }
            extents.push(extent);
        }
        Ok(extents)
    }

    pub async fn read_table(&self, extent: &StreamExtent) -> Result<BlockTable, SegmentError> {
        let end = extent
            .offset
            .checked_add(KeyHash::BYTES as u64 + extent.table_bytes())
            .ok_or(SegmentError::Corrupt("extent offset"))?;
        let bytes = self.read_range(extent.offset..end).await?;
        let table = decode_block_table(extent.stream, &bytes[KeyHash::BYTES..])?;
        check_table(extent, &table)?;
        Ok(table)
    }

    /// Reads consecutive blocks with one range read, checking each block's CRC. The
    /// result is whole records, byte for byte what the wire carried.
    pub async fn read_blocks(
        &self,
        extent: &StreamExtent,
        table: &BlockTable,
        blocks: Range<usize>,
    ) -> Result<Bytes, SegmentError> {
        let selected = table
            .blocks
            .get(blocks.clone())
            .ok_or(SegmentError::Corrupt("block index"))?;
        let before: u64 = table.blocks[..blocks.start]
            .iter()
            .map(|block| block.summary.bytes)
            .sum();
        let total: u64 = selected.iter().map(|block| block.summary.bytes).sum();
        let start = extent.blocks_offset() + before;
        let bytes = self.read_range(start..start + total).await?;
        let mut offset = 0usize;
        for block in selected {
            let end = offset + block.summary.bytes as usize;
            if crc32c(&bytes[offset..end]) != block.crc32c {
                return Err(SegmentError::Crc("block"));
            }
            offset = end;
        }
        Ok(bytes)
    }

    /// One extent's records, with one read of its region.
    pub async fn read_extent(&self, extent: &StreamExtent) -> Result<Bytes, SegmentError> {
        let end = extent
            .offset
            .checked_add(extent.len_bytes)
            .ok_or(SegmentError::Corrupt("extent offset"))?;
        let region = self.read_range(extent.offset..end).await?;
        let entry = IndexEntry {
            hash: stream_hash(extent.stream),
            offset: extent.offset,
            len_bytes: extent.len_bytes,
        };
        let (found, records) = records_of(&region, &entry)?;
        // The ids may be less than the region's: those of a former generation's limit.
        if (found.stream, found.block_count) != (extent.stream, extent.block_count) {
            return Err(SegmentError::Corrupt("region isn't the extent's"));
        }
        Ok(records)
    }

    /// `stream`'s region and records, if it has one: a page read, then one for the region.
    pub async fn read_region(
        &self,
        top: &TopIndex,
        stream: KeyHash,
    ) -> Result<Option<(StreamExtent, Bytes)>, SegmentError> {
        for entry in self.candidates(top, stream).await? {
            let region = self
                .read_range(entry.offset..entry.offset + entry.len_bytes)
                .await?;
            let (extent, records) = records_of(&region, &entry)?;
            if extent.stream == stream {
                return Ok(Some((extent, records)));
            }
        }
        Ok(None)
    }

    /// The blocks a filter can't rule out, using summaries alone (section 4.1). Each
    /// run of neighbouring blocks is one read. Filtering inside a block is the caller's.
    pub async fn read_matching_blocks(
        &self,
        extent: &StreamExtent,
        table: &BlockTable,
        filter: &Filter,
    ) -> Result<Vec<Bytes>, SegmentError> {
        let mut runs = Vec::new();
        let mut start = None;
        for (index, block) in table.blocks.iter().enumerate() {
            let skip = filter.decide_block(&block.summary) == Decision::None;
            match (skip, start) {
                (false, None) => start = Some(index),
                (true, Some(first)) => {
                    runs.push(first..index);
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(first) = start {
            runs.push(first..table.blocks.len());
        }
        let mut out = Vec::with_capacity(runs.len());
        for run in runs {
            out.push(self.read_blocks(extent, table, run).await?);
        }
        Ok(out)
    }

    /// All of a stream's records, for the common case of a reader without a cached top
    /// index.
    pub async fn read_stream(&self, stream: KeyHash) -> Result<Bytes, SegmentError> {
        let top = self.read_top_index().await?;
        Ok(self
            .read_region(&top, stream)
            .await?
            .map_or_else(Bytes::new, |(_, records)| records))
    }
}
