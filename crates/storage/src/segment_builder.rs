//! Builds a segment from references to stamped records, without copying them
//! (SPEC sections 6.4, 7.3). It reads record headers and takes CRCs over the bytes,
//! and never parses data.

use std::collections::BTreeMap;

use bytes::Bytes;
use format::{BlockSummary, KeyHash, RecordHeader, crc32c_append};

use crate::segment_format::{
    BlockEntry, DEFAULT_BLOCK_BYTES, DEFAULT_PAGE_BYTES, IndexEntry, OFFSET_EVERY, PAGE_HEAD_BYTES,
    SegmentError, StreamExtent, TopEntry, TopIndex, encode_block_table, encode_footer,
    encode_last_ids, encode_page, stream_hash,
};

/// A finished segment, as the pieces of one PUT. Together they are the whole object.
#[derive(Debug, Clone)]
pub struct BuiltSegment {
    pub parts: Vec<Bytes>,
    pub len_bytes: u64,
    /// The lowest id in the segment. `None` if it holds no records.
    pub first_id: Option<u64>,
    /// The highest id in the segment. `None` if it holds no records.
    pub last_id: Option<u64>,
    /// One per stream, in key hash order, which is also offset order.
    pub extents: Vec<StreamExtent>,
}

#[derive(Debug)]
pub struct SegmentBuilder {
    /// Each stream's sections, one per `add_extent`, in the order added.
    regions: BTreeMap<KeyHash, Vec<Section>>,
    block_bytes: usize,
    page_bytes: usize,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        SegmentBuilder::new()
    }
}

/// The block being filled: its summary and CRC.
#[derive(Debug)]
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

/// One `add_extent`'s records: its closed blocks, the one being filled and the slices they
/// will write.
#[derive(Debug)]
struct Section {
    pieces: Vec<Bytes>,
    blocks: Vec<BlockEntry>,
    open: OpenBlock,
    blocks_bytes: u64,
    id_first: u64,
    id_last: u64,
}

impl Section {
    fn close_block(&mut self) {
        let open = std::mem::replace(&mut self.open, OpenBlock::new());
        self.blocks_bytes += open.summary.bytes;
        self.blocks.push(BlockEntry {
            summary: open.summary,
            crc32c: open.crc,
        });
    }

    fn push_piece(&mut self, bytes: &Bytes, start: usize, end: usize) {
        self.open.crc = crc32c_append(self.open.crc, &bytes[start..end]);
        self.pieces.push(bytes.slice(start..end));
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
            regions: BTreeMap::new(),
            block_bytes,
            page_bytes,
        }
    }

    /// Adds one stream's records, in id order. Each `Bytes` holds whole records.
    /// Nothing is added for a stream with no records. A stream added again, as one that moved
    /// cores is, gets one region still: its sections go in id order, and mustn't overlap.
    pub fn add_extent<I>(&mut self, stream: KeyHash, records: I) -> Result<(), SegmentError>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut section = Section {
            pieces: Vec::new(),
            blocks: Vec::new(),
            open: OpenBlock::new(),
            blocks_bytes: 0,
            id_first: 0,
            id_last: 0,
        };
        for bytes in records {
            let mut offset = 0;
            let mut piece_start = 0;
            while offset < bytes.len() {
                let header = RecordHeader::read(&bytes[offset..]).map_err(SegmentError::Records)?;
                if header.count == 0 {
                    return Err(SegmentError::Records(format::FormatError::ZeroCount));
                }
                if header.id == 0 || header.id <= section.id_last {
                    return Err(SegmentError::OutOfOrder("record ids in an extent"));
                }
                let total = header.total_bytes();
                if total > bytes.len() - offset {
                    return Err(SegmentError::Records(format::FormatError::TruncatedData));
                }
                if section.open.summary.count > 0
                    && section.open.summary.bytes as usize + total > self.block_bytes
                {
                    if offset > piece_start {
                        section.push_piece(&bytes, piece_start, offset);
                        piece_start = offset;
                    }
                    section.close_block();
                }
                section.open.summary.add(&header);
                if section.id_first == 0 {
                    section.id_first = header.id;
                }
                section.id_last = header.last_id();
                offset += total;
            }
            if offset > piece_start {
                section.push_piece(&bytes, piece_start, offset);
            }
        }
        if section.id_first != 0 {
            section.close_block();
            self.regions.entry(stream).or_default().push(section);
        }
        Ok(())
    }

    /// Writes the regions in key hash order, then the footer: pages, the last ids, the top
    /// index and the trailer (section 7.3).
    pub fn finish(self) -> Result<BuiltSegment, SegmentError> {
        let mut parts = Vec::new();
        let mut extents = Vec::with_capacity(self.regions.len());
        let mut offset = 0u64;
        for (stream, mut sections) in self.regions {
            sections.sort_unstable_by_key(|section| section.id_first);
            if sections
                .windows(2)
                .any(|pair| pair[0].id_last >= pair[1].id_first)
            {
                return Err(SegmentError::OutOfOrder("record ids in an extent"));
            }
            let blocks: Vec<BlockEntry> = sections
                .iter()
                .flat_map(|section| section.blocks.iter().copied())
                .collect();
            let blocks_bytes: u64 = sections.iter().map(|section| section.blocks_bytes).sum();
            let table = encode_block_table(stream, &blocks);
            let len_bytes = (KeyHash::BYTES + table.len()) as u64 + blocks_bytes;
            parts.push(Bytes::copy_from_slice(&stream.0));
            parts.push(Bytes::from(table));
            extents.push(StreamExtent {
                stream,
                offset,
                len_bytes,
                id_first: sections[0].id_first,
                id_last: sections[sections.len() - 1].id_last,
                block_count: blocks.len() as u32,
            });
            for section in sections {
                parts.extend(section.pieces);
            }
            offset += len_bytes;
        }
        let regions: Vec<(KeyHash, u64, u64)> = extents
            .iter()
            .map(|extent| (extent.stream, extent.len_bytes, extent.id_last))
            .collect();
        let (len_bytes, _) = encode_index(&regions, self.page_bytes, &mut parts)?;
        Ok(BuiltSegment {
            parts,
            len_bytes,
            first_id: extents.iter().map(|extent| extent.id_first).min(),
            last_id: extents.iter().map(|extent| extent.id_last).max(),
            extents,
        })
    }
}

fn varint_len(value: u64) -> usize {
    (64 - value.max(1).leading_zeros() as usize).div_ceil(7)
}

/// Writes the footer after regions already in `parts`: `(key hash, length, last id)` each, in
/// key hash order from offset 0. Says where the object ends, with its top index. Checkpoints
/// use this too (D18).
pub(crate) fn encode_index(
    regions: &[(KeyHash, u64, u64)],
    page_bytes: usize,
    parts: &mut Vec<Bytes>,
) -> Result<(u64, TopIndex), SegmentError> {
    if regions.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(SegmentError::OutOfOrder("regions by key hash"));
    }
    let mut entries = Vec::with_capacity(regions.len());
    let mut offset = 0u64;
    for (stream, len_bytes, _) in regions {
        entries.push(IndexEntry {
            hash: stream_hash(*stream),
            offset,
            len_bytes: *len_bytes,
        });
        offset += len_bytes;
    }
    let data_end = offset;
    let mut pages = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start;
        let mut used_bytes = PAGE_HEAD_BYTES;
        while end < entries.len() {
            // A run of one hash stays in one page.
            let mut run_end = end;
            let mut run_bytes = 0;
            while run_end < entries.len() && entries[run_end].hash == entries[end].hash {
                let entry = &entries[run_end];
                let previous = if run_end == start {
                    entry.hash
                } else {
                    entries[run_end - 1].hash
                };
                run_bytes += varint_len(entry.hash - previous) + varint_len(entry.len_bytes);
                if (run_end as u64).is_multiple_of(OFFSET_EVERY) {
                    run_bytes += varint_len(entry.offset);
                }
                run_end += 1;
            }
            if end > start && used_bytes + run_bytes > page_bytes {
                break;
            }
            used_bytes += run_bytes;
            end = run_end;
        }
        let page = encode_page(&entries[start..end], start as u64);
        let len_bytes =
            u32::try_from(page.len()).map_err(|_| SegmentError::TooLong("footer page"))?;
        pages.push(TopEntry {
            first_hash: entries[start].hash,
            first_offset: entries[start].offset,
            first_entry: start as u64,
            offset,
            len_bytes,
            data_end,
        });
        offset += u64::from(len_bytes);
        parts.push(Bytes::from(page));
        start = end;
    }
    let last_ids: Vec<(KeyHash, u64)> = regions
        .iter()
        .map(|(stream, _, last_id)| (*stream, *last_id))
        .collect();
    let last_ids = encode_last_ids(&last_ids);
    let last_ids_offset = offset;
    let last_ids_len = last_ids.len() as u64;
    offset += last_ids_len;
    parts.push(Bytes::from(last_ids));
    let top = TopIndex::new(pages, data_end, last_ids_offset, last_ids_len);
    let footer = encode_footer(&top, offset);
    offset += footer.len() as u64;
    parts.push(Bytes::from(footer));
    Ok((offset, top))
}
