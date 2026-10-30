//! Per-block summaries (SPEC section 4.1).

use crate::RecordHeader;

/// The block's `author` field, or "mixed".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockAuthor {
    Single(u64),
    Mixed,
}

/// What a summary says about a filter, for a whole block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// No record matches: skip the block.
    None,
    /// Every record matches: no need to check them one at a time.
    All,
    /// Check each record.
    Maybe,
}

/// A summary of one block. `count` is items and `bytes` is whole records,
/// including headers and padding (D35).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSummary {
    pub options_or: u64,
    pub options_and: u64,
    pub id_first: u64,
    pub id_last: u64,
    pub count: u64,
    pub bytes: u64,
    pub author: BlockAuthor,
}

impl Default for BlockSummary {
    fn default() -> Self {
        BlockSummary::new()
    }
}

impl BlockSummary {
    /// The summary of an empty block.
    pub fn new() -> BlockSummary {
        BlockSummary {
            options_or: 0,
            options_and: u64::MAX,
            id_first: u64::MAX,
            id_last: 0,
            count: 0,
            bytes: 0,
            author: BlockAuthor::Single(0),
        }
    }

    /// Adds one record, read from its header alone.
    pub fn add(&mut self, header: &RecordHeader) {
        self.author = match self.author {
            _ if self.count == 0 => BlockAuthor::Single(header.author),
            BlockAuthor::Single(author) if author == header.author => self.author,
            _ => BlockAuthor::Mixed,
        };
        self.options_or |= header.options;
        self.options_and &= header.options;
        self.id_first = self.id_first.min(header.id);
        self.id_last = self.id_last.max(header.last_id());
        self.count += u64::from(header.count);
        self.bytes += header.total_bytes() as u64;
    }
}
