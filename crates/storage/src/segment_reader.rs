//! Reads a segment with range reads only (SPEC section 7.3). Readers never scan.
//!
//! A lookup is one tail read for the top index, which the caller caches, one read for
//! the page and then two for the data: the extent's table and its blocks. A caller
//! holding a `StreamExtent` starts at the table.

use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use format::{Decision, Filter, KeyHash, crc32c};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};

use crate::segment_format::{
    BlockTable, SegmentError, StreamExtent, TRAILER_BYTES, TopIndex, decode_block_table,
    decode_page, decode_top_index, decode_trailer, stream_hash,
};

/// The tail read is big enough for the top index of a segment under the 128 MB cap of
/// section 7.3 with normal stream ids, so the footer normally takes one read. Nothing
/// enforces that cap yet (Q18). A larger top index takes a second read for the rest
/// (D52).
pub const SEGMENT_TAIL_READ_BYTES: usize = 256 * 1024;

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

    /// Every extent of `stream`: one normally, two if it moved cores during the window.
    pub async fn lookup(
        &self,
        top: &TopIndex,
        stream: KeyHash,
    ) -> Result<Vec<StreamExtent>, SegmentError> {
        let Some(page) = top.page_for(stream_hash(stream)) else {
            return Ok(Vec::new());
        };
        let bytes = self
            .read_range(page.offset..page.offset + u64::from(page.len_bytes))
            .await?;
        let entries = decode_page(&bytes, Some(stream))?;
        Ok(entries.into_iter().map(|(_, extent)| extent).collect())
    }

    /// Every extent in the segment, reading every page. For compaction and tests; a
    /// reader looking for one stream uses `lookup`.
    pub async fn all_extents(&self, top: &TopIndex) -> Result<Vec<StreamExtent>, SegmentError> {
        let mut extents = Vec::new();
        for page in top.pages() {
            let bytes = self
                .read_range(page.offset..page.offset + u64::from(page.len_bytes))
                .await?;
            extents.extend(
                decode_page(&bytes, None)?
                    .into_iter()
                    .map(|(_, extent)| extent),
            );
        }
        Ok(extents)
    }

    pub async fn read_table(&self, extent: &StreamExtent) -> Result<BlockTable, SegmentError> {
        let end = extent
            .offset
            .checked_add(extent.table_bytes())
            .ok_or(SegmentError::Corrupt("extent offset"))?;
        let bytes = self.read_range(extent.offset..end).await?;
        let table = decode_block_table(&bytes)?;
        let blocks_bytes = table
            .blocks
            .iter()
            .try_fold(0u64, |sum, block| sum.checked_add(block.summary.bytes));
        if blocks_bytes.and_then(|sum| sum.checked_add(extent.table_bytes()))
            != Some(extent.len_bytes)
        {
            return Err(SegmentError::Corrupt("table doesn't add up to the extent"));
        }
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
        let start = extent.offset + extent.table_bytes() + before;
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

    /// One extent's records: one read for the table and one for the blocks.
    pub async fn read_extent(&self, extent: &StreamExtent) -> Result<Bytes, SegmentError> {
        let table = self.read_table(extent).await?;
        self.read_blocks(extent, &table, 0..table.blocks.len())
            .await
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
    /// index: extents in id order, concatenated.
    pub async fn read_stream(&self, stream: KeyHash) -> Result<Bytes, SegmentError> {
        let top = self.read_top_index().await?;
        let mut extents = self.lookup(&top, stream).await?;
        extents.sort_by_key(|extent| extent.id_first);
        let mut out = BytesMut::new();
        for extent in &extents {
            out.extend_from_slice(&self.read_extent(extent).await?);
        }
        Ok(out.freeze())
    }
}
