//! Builds a segment from references to stamped records, without copying them
//! (SPEC sections 6.4, 7.3). It reads record headers and takes CRCs over the bytes,
//! and never parses data.

use bytes::Bytes;
use format::{BlockSummary, KeyHash, RecordHeader, crc32c_append};

use crate::segment_format::{
    BlockEntry, DEFAULT_BLOCK_BYTES, DEFAULT_PAGE_BYTES, INDEX_ENTRY_BYTES, PAGE_HEAD_BYTES,
    SegmentError, StreamExtent, TopEntry, TopIndex, encode_block_table, encode_footer, encode_page,
    stream_hash,
};

/// A finished segment, as the pieces of one PUT. Together they are the whole object.
#[derive(Debug, Clone)]
pub struct BuiltSegment {
    pub parts: Vec<Bytes>,
    pub len_bytes: u64,
    /// The lowest id in the segment, for its path. `None` if it holds no records.
    pub first_id: Option<u64>,
    /// The highest id in the segment, for the hour in its path. `None` if it holds no records.
    pub last_id: Option<u64>,
    /// The stream index, sorted by `(key hash, offset)`.
    pub extents: Vec<StreamExtent>,
}

#[derive(Debug)]
pub struct SegmentBuilder {
    parts: Vec<Bytes>,
    offset: u64,
    extents: Vec<(u64, StreamExtent)>,
    first_id: Option<u64>,
    block_bytes: usize,
    page_bytes: usize,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        SegmentBuilder::new()
    }
}

/// The block being filled: its summary, CRC and the slices it will write.
struct OpenBlock {
    summary: BlockSummary,
    crc: u32,
}

impl OpenBlock {
    fn new() -> OpenBlock {
        OpenBlock {
            summary: BlockSummary::new(),
            crc: 0,
        }
    }
}

impl SegmentBuilder {
    pub fn new() -> SegmentBuilder {
        SegmentBuilder::with_limits(DEFAULT_BLOCK_BYTES, DEFAULT_PAGE_BYTES)
    }

    /// `block_bytes` and `page_bytes` are targets: a bigger record gets a block of its
    /// own, and a page can run over to keep equal hashes together (D52).
    pub fn with_limits(block_bytes: usize, page_bytes: usize) -> SegmentBuilder {
        SegmentBuilder {
            parts: Vec::new(),
            offset: 0,
            extents: Vec::new(),
            first_id: None,
            block_bytes,
            page_bytes,
        }
    }

    /// Adds one stream's records, in id order. Each `Bytes` holds whole records.
    /// Nothing is added for a stream with no records. A stream may be added more than
    /// once, which is how a stream that moved cores gets its two entries.
    pub fn add_extent<I>(&mut self, stream: KeyHash, records: I) -> Result<(), SegmentError>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let table_slot = self.parts.len();
        self.parts.push(Bytes::new());
        let mut blocks: Vec<BlockEntry> = Vec::new();
        let mut open = OpenBlock::new();
        let mut blocks_bytes = 0u64;
        let mut id_first = None;
        let mut id_last = 0u64;

        for bytes in records {
            let mut offset = 0;
            let mut piece_start = 0;
            while offset < bytes.len() {
                let header = RecordHeader::read(&bytes[offset..]).map_err(SegmentError::Records)?;
                if header.count == 0 {
                    return Err(SegmentError::Records(format::FormatError::ZeroCount));
                }
                if header.id == 0 || header.id <= id_last {
                    return Err(SegmentError::OutOfOrder("record ids in an extent"));
                }
                let total = header.total_bytes();
                if total > bytes.len() - offset {
                    return Err(SegmentError::Records(format::FormatError::TruncatedData));
                }
                if open.summary.count > 0 && open.summary.bytes as usize + total > self.block_bytes
                {
                    if offset > piece_start {
                        self.push_piece(&bytes, piece_start, offset, &mut open);
                        piece_start = offset;
                    }
                    blocks_bytes += open.summary.bytes;
                    blocks.push(BlockEntry {
                        summary: open.summary,
                        crc32c: open.crc,
                    });
                    open = OpenBlock::new();
                }
                open.summary.add(&header);
                id_first.get_or_insert(header.id);
                id_last = header.last_id();
                offset += total;
            }
            if offset > piece_start {
                self.push_piece(&bytes, piece_start, offset, &mut open);
            }
        }
        let Some(id_first) = id_first else {
            self.parts.truncate(table_slot);
            return Ok(());
        };
        blocks_bytes += open.summary.bytes;
        blocks.push(BlockEntry {
            summary: open.summary,
            crc32c: open.crc,
        });

        let table = encode_block_table(&blocks);
        let len_bytes = table.len() as u64 + blocks_bytes;
        self.parts[table_slot] = Bytes::from(table);
        let extent = StreamExtent {
            stream,
            offset: self.offset,
            len_bytes,
            id_first,
            id_last,
            block_count: blocks.len() as u32,
        };
        self.offset += len_bytes;
        self.first_id = Some(self.first_id.map_or(id_first, |id| id.min(id_first)));
        self.extents.push((stream_hash(stream), extent));
        Ok(())
    }

    fn push_piece(&mut self, bytes: &Bytes, start: usize, end: usize, open: &mut OpenBlock) {
        open.crc = crc32c_append(open.crc, &bytes[start..end]);
        self.parts.push(bytes.slice(start..end));
    }

    /// Writes the footer: sorted pages, the top index and the trailer. Only the index
    /// is sorted here, and the data stays where it is (section 6.4).
    pub fn finish(mut self) -> Result<BuiltSegment, SegmentError> {
        let (end_offset, _) = encode_index_parts(
            &mut self.extents,
            self.page_bytes,
            self.offset,
            &mut self.parts,
        )?;
        self.offset = end_offset;
        let extents: Vec<StreamExtent> =
            self.extents.into_iter().map(|(_, extent)| extent).collect();
        Ok(BuiltSegment {
            parts: self.parts,
            len_bytes: self.offset,
            first_id: self.first_id,
            last_id: extents.iter().map(|extent| extent.id_last).max(),
            extents,
        })
    }
}

/// Writes sorted footer pages, the top index and the trailer after the bytes so far, and says
/// where the object ends. Checkpoints use this too (D18).
pub(crate) fn encode_index_parts(
    extents: &mut [(u64, StreamExtent)],
    page_bytes: usize,
    mut offset: u64,
    parts: &mut Vec<Bytes>,
) -> Result<(u64, Vec<TopEntry>), SegmentError> {
    extents.sort_unstable_by(|(hash_a, a), (hash_b, b)| {
        (hash_a, &a.stream, a.offset).cmp(&(hash_b, &b.stream, b.offset))
    });
    let mut top = Vec::new();
    let mut start = 0;
    while start < extents.len() {
        let mut end = start;
        let mut page_used_bytes = PAGE_HEAD_BYTES;
        while end < extents.len() {
            let hash = extents[end].0;
            // A run of one hash, from two-section streams, stays whole.
            let mut run_end = end;
            let mut run_bytes = 0;
            while run_end < extents.len() && extents[run_end].0 == hash {
                run_bytes += INDEX_ENTRY_BYTES;
                run_end += 1;
            }
            if end > start && page_used_bytes + run_bytes > page_bytes {
                break;
            }
            page_used_bytes += run_bytes;
            end = run_end;
        }
        let entries: Vec<(u64, &StreamExtent)> = extents[start..end]
            .iter()
            .map(|(hash, extent)| (*hash, extent))
            .collect();
        let page = encode_page(&entries);
        let len_bytes =
            u32::try_from(page.len()).map_err(|_| SegmentError::TooLong("footer page"))?;
        top.push(TopEntry {
            first_hash: extents[start].0,
            offset,
            len_bytes,
        });
        offset += u64::from(len_bytes);
        parts.push(Bytes::from(page));
        start = end;
    }
    let footer = encode_footer(&top, offset);
    offset += footer.len() as u64;
    parts.push(Bytes::from(footer));
    Ok((offset, top))
}

/// As `encode_index_parts`, for a caller that wants the top index back.
pub(crate) fn encode_index(
    extents: &mut [(u64, StreamExtent)],
    page_bytes: usize,
    offset: u64,
    parts: &mut Vec<Bytes>,
) -> Result<TopIndex, SegmentError> {
    let (_, pages) = encode_index_parts(extents, page_bytes, offset, parts)?;
    Ok(TopIndex::from_pages(pages))
}
