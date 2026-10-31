//! The owner's side of delivery (SPEC sections 5.1, 5.2): which stamped records are visible
//! yet, the filter table, named cursors and where a stream's sealed records are.
//!
//! All of this is a stream's own, and moves with it. The connection's side, on each home
//! core, is in `connection.rs`.

use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use format::{Filter, RecordHeader, make_id};
use object_store::path::Path;
use storage::StreamExtent;

use crate::dedupe::DedupeTable;
use crate::slab::SlabBytes;

/// The record bytes a delivery shares: slab memory while they are recent, and what a read
/// returned after that.
#[derive(Debug, Clone)]
pub(crate) enum SharedBytes {
    Slab(SlabBytes),
    Plain(Bytes),
}

impl Deref for SharedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            SharedBytes::Slab(bytes) => bytes,
            SharedBytes::Plain(bytes) => bytes,
        }
    }
}

impl SharedBytes {
    /// The allocation these bytes keep alive, as `(identity, size)`: the whole slab, or the
    /// whole read, however little of it matched (D96).
    pub(crate) fn held(&self) -> (usize, u64) {
        match self {
            SharedBytes::Slab(bytes) => bytes.held(),
            SharedBytes::Plain(bytes) => (bytes.as_ptr() as usize, bytes.len() as u64),
        }
    }
}

/// Where a record is in some bytes, read from its header alone.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordSpan {
    pub start: usize,
    pub end: usize,
    pub header: RecordHeader,
}

/// Walks headers without looking at any data. What it is given was validated at publish,
/// or passed a block's CRC. It stops at anything that doesn't fit.
pub(crate) fn walk_records(bytes: &[u8], spans: &mut Vec<RecordSpan>) {
    spans.clear();
    let mut start = 0;
    while start < bytes.len() {
        let Ok(header) = RecordHeader::read(&bytes[start..]) else {
            return;
        };
        let end = start + header.total_bytes();
        if end > bytes.len() {
            return;
        }
        spans.push(RecordSpan { start, end, header });
        start = end;
    }
}

/// A contiguous stretch of matching records.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Run {
    pub start: usize,
    pub end: usize,
    pub first_id: u64,
    pub last_id: u64,
}

/// One filter's answer for one lot of bytes. Every subscriber under that filter holds the
/// same one, so a subscriber costs one reference and no copy.
#[derive(Debug)]
pub struct Matched {
    pub(crate) bytes: SharedBytes,
    pub(crate) runs: Vec<Run>,
    pub(crate) total_bytes: u64,
    pub(crate) first_id: u64,
    pub(crate) last_id: u64,
}

impl Matched {
    /// The records `filter` picks. `None` if none. Each record looked at adds one to
    /// `check_count`, which is what makes that counter a count (section 5.2).
    pub(crate) fn select(
        bytes: &SharedBytes,
        spans: &[RecordSpan],
        filter: &Filter,
        check_count: &mut u64,
    ) -> Option<Arc<Matched>> {
        let mut runs: Vec<Run> = Vec::new();
        let mut total_bytes = 0;
        for span in spans {
            *check_count += 1;
            if !filter.matches(&span.header) {
                continue;
            }
            total_bytes += (span.end - span.start) as u64;
            match runs.last_mut() {
                Some(run) if run.end == span.start => {
                    run.end = span.end;
                    run.last_id = span.header.last_id();
                }
                _ => runs.push(Run {
                    start: span.start,
                    end: span.end,
                    first_id: span.header.id,
                    last_id: span.header.last_id(),
                }),
            }
        }
        let (first_id, last_id) = (runs.first()?.first_id, runs.last()?.last_id);
        Some(Arc::new(Matched {
            bytes: bytes.clone(),
            runs,
            total_bytes,
            first_id,
            last_id,
        }))
    }
}

/// What one newly visible batch means for one home core's subscribers.
#[derive(Debug)]
pub(crate) struct FanOut {
    /// Nothing at or below this id is still to come.
    pub through_id: u64,
    pub matches: Vec<(u32, Arc<Matched>)>,
    /// `(version, doc)`, if this batch changed state.
    pub state: Option<(u64, Bytes)>,
}

/// The filter id of a `STATE_ONLY` subscriber, which has no filter.
pub(crate) const STATE_ONLY_FILTER_ID: u32 = u32::MAX;

/// A filter id, what it picks, and who is under it on each core.
#[derive(Debug)]
struct FilterEntry {
    id: u32,
    filter: Filter,
    subs_by_core: Vec<u32>,
}

/// Section 5.2: a stream's distinct filters, with subscribers listed under each. Subscribers
/// themselves are listed on their home cores, so a record is checked once per filter on
/// the owner whichever core they are on.
#[derive(Debug)]
pub(crate) struct FilterTable {
    entries: Vec<FilterEntry>,
    index: HashMap<Filter, usize>,
    next_id: u32,
    subs_by_core: Vec<u32>,
    state_subs_by_core: Vec<u32>,
}

impl FilterTable {
    fn new(core_count: usize) -> FilterTable {
        FilterTable {
            entries: Vec::new(),
            index: HashMap::new(),
            next_id: 0,
            subs_by_core: vec![0; core_count],
            state_subs_by_core: vec![0; core_count],
        }
    }

    /// Adds a subscriber, and returns its filter's id. An id isn't reused, so that a late
    /// message can't be taken for a later filter.
    pub fn attach(&mut self, filter: Filter, home: usize) -> u32 {
        let at = match self.index.get(&filter) {
            Some(at) => *at,
            None => {
                let at = self.entries.len();
                self.entries.push(FilterEntry {
                    id: self.next_id,
                    filter,
                    subs_by_core: vec![0; self.subs_by_core.len()],
                });
                self.next_id += 1;
                self.index.insert(filter, at);
                at
            }
        };
        self.entries[at].subs_by_core[home] += 1;
        self.subs_by_core[home] += 1;
        self.entries[at].id
    }

    pub fn attach_state_only(&mut self, home: usize) {
        self.state_subs_by_core[home] += 1;
    }

    pub fn detach(&mut self, filter_id: u32, home: usize) {
        if filter_id == STATE_ONLY_FILTER_ID {
            self.state_subs_by_core[home] = self.state_subs_by_core[home].saturating_sub(1);
            return;
        }
        let Some(at) = self.entries.iter().position(|entry| entry.id == filter_id) else {
            return;
        };
        let entry = &mut self.entries[at];
        if entry.subs_by_core[home] == 0 {
            return;
        }
        entry.subs_by_core[home] -= 1;
        self.subs_by_core[home] -= 1;
        if entry.subs_by_core.iter().all(|count| *count == 0) {
            self.index.remove(&entry.filter);
            self.entries.swap_remove(at);
            if let Some(moved) = self.entries.get(at) {
                self.index.insert(moved.filter, at);
            }
        }
    }

    /// Nobody is subscribed from any core, so the stream may leave RAM (section 7.6).
    pub fn is_unwatched(&self) -> bool {
        self.subs_by_core.iter().all(|count| *count == 0)
            && self.state_subs_by_core.iter().all(|count| *count == 0)
    }

    pub fn wants(&self, home: usize, has_state: bool) -> bool {
        self.subs_by_core[home] > 0 || (has_state && self.state_subs_by_core[home] > 0)
    }

    /// Each filter's answer, one check per record per filter, and the fan-out bytes that
    /// come to. Returns `(matches, checks, fan_out_bytes)`.
    pub fn check(
        &self,
        bytes: &SharedBytes,
        spans: &[RecordSpan],
    ) -> (Vec<(u32, Arc<Matched>)>, u64, u64) {
        let mut matches = Vec::with_capacity(self.entries.len());
        let mut fan_out_bytes = 0;
        let mut check_count = 0;
        for entry in &self.entries {
            if let Some(matched) = Matched::select(bytes, spans, &entry.filter, &mut check_count) {
                let sub_count: u64 = entry.subs_by_core.iter().map(|n| u64::from(*n)).sum();
                fan_out_bytes += matched.total_bytes * sub_count;
                matches.push((entry.id, matched));
            }
        }
        (matches, check_count, fan_out_bytes)
    }
}

/// A stamped batch not yet visible (D84).
#[derive(Debug)]
pub(crate) struct PendingVisible {
    /// The core that stamped it, whose ticket this is.
    pub core: usize,
    pub ticket: u64,
    /// Asked for level 2 or 3, so it waits for the WAL. Level 0 and 1 wait only for what
    /// came before it in the stream.
    pub needs_wal: bool,
    pub last_id: u64,
    pub bytes: SlabBytes,
    pub state: Option<(u64, Bytes)>,
    /// `(producer_id, producer_seq, first_id, count)`, if it has a producer.
    pub dedupe: Option<(u64, u64, u64, u32)>,
}

/// A stream's sealed records in one object.
#[derive(Debug, Clone)]
pub(crate) struct ExtentRef {
    pub path: Path,
    pub extent: StreamExtent,
}

impl ExtentRef {
    fn extent_path(&self) -> &Path {
        &self.path
    }
}

/// What came of asking an owner to replace extents with a compacted one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplaceResult {
    Applied,
    /// None of them are there any more, as after a crash that lost only the cursor.
    Gone,
    /// They are there but this can't be done yet. The cursor waits.
    Retry,
}

/// What a visible batch changes in the saved view, once its ticket is durable.
#[derive(Debug)]
pub(crate) struct UnsavedStep {
    pub core: usize,
    pub ticket: u64,
    pub last_id: u64,
    pub state: Option<(u64, Bytes)>,
    pub dedupe: Option<(u64, u64, u64, u32)>,
}

/// A stream's delivery state. Moves with the stream.
#[derive(Debug)]
pub(crate) struct StreamDelivery {
    pub pending: VecDeque<PendingVisible>,
    /// The id of the last visible record.
    pub visible_through: u64,
    /// `(version, doc)` as of the last visible change, which is all a reader may see. The
    /// stream's own state runs ahead of it, by what is stamped and waiting for the WAL (D98).
    pub visible_state: (u64, Bytes),
    pub table: FilterTable,
    /// The next message number for each home core. Continuous across moves, so that a home
    /// core can put two owners' messages back in order (D85).
    pub seq_by_home: Vec<u64>,
    /// `(stream, name) → id` of section 5.1.
    pub cursors: HashMap<Arc<str>, u64>,
    /// In id order, and not overlapping (D68).
    pub extents: Vec<ExtentRef>,
    /// The last id of a deleted stream of this name. Nothing at or below it is readable.
    pub floor_id: u64,
    /// The highest id in an extent taken away with nothing in its place. A position below this
    /// has lost records. Not saved, so a restart or an eviction forgets it (D178, Q32).
    pub expired_through: u64,
    pub sinks: std::collections::BTreeMap<u64, crate::message::SinkSpec>,
    /// The highest epoch of a sink deleted here. Not saved: no delivery outlives a restart.
    pub deleted_sink_epoch: u64,
    /// What a checkpoint may save: the stream as of its last record in the WAL. An entry for a
    /// publish a crash can erase would ack it on the retry, unwritten (D136, D154).
    pub saved_through: u64,
    pub saved_state: (u64, Bytes),
    pub saved_dedupe: DedupeTable,
    /// Visible, and not yet in the WAL, in id order. Only with `track_meta`.
    pub unsaved: VecDeque<UnsavedStep>,
    /// The last id each core held when this stream left it. Its extent still has to reach
    /// here, and until it does this stream's owner list is the only way to that core (D153).
    pub held_elsewhere: Vec<u64>,
    /// Whether this stream's metadata is saved at seals. Off on a node without a store.
    pub track_meta: bool,
    pub meta_dirty: bool,
    /// The last checkpoint gather that took this stream's changes.
    pub saved_gather: u64,
    /// Extents no checkpoint has yet.
    pub new_extents: Vec<ExtentRef>,
    /// `id_first` of extents gone since the last checkpoint, for it to take away.
    pub dropped_extents: Vec<u64>,
    /// When it was last used, on its core's own count, for eviction.
    pub last_used: u64,
}

impl StreamDelivery {
    pub fn new(core_count: usize) -> StreamDelivery {
        StreamDelivery {
            pending: VecDeque::new(),
            visible_through: 0,
            visible_state: (0, Bytes::new()),
            table: FilterTable::new(core_count),
            seq_by_home: vec![0; core_count],
            cursors: HashMap::new(),
            extents: Vec::new(),
            floor_id: 0,
            expired_through: 0,
            sinks: std::collections::BTreeMap::new(),
            deleted_sink_epoch: 0,
            saved_through: 0,
            saved_state: (0, Bytes::new()),
            saved_dedupe: DedupeTable::new(),
            unsaved: VecDeque::new(),
            held_elsewhere: Vec::new(),
            track_meta: false,
            meta_dirty: false,
            saved_gather: 0,
            new_extents: Vec::new(),
            dropped_extents: Vec::new(),
            last_used: 0,
        }
    }

    /// The head a reader may be told: the last id, except while some are stamped and not
    /// yet visible (D98).
    pub fn visible_head(&self, last_id: u64) -> u64 {
        if self.pending.is_empty() {
            last_id
        } else {
            self.visible_through
        }
    }

    /// Both cores of a seal report its extents, so one may arrive twice.
    pub fn add_extent(&mut self, path: &Path, extent: &StreamExtent) {
        self.held_elsewhere
            .retain(|id| !(extent.id_first <= *id && *id <= extent.id_last));
        let at = self
            .extents
            .partition_point(|known| known.extent.id_first < extent.id_first);
        // A report arriving late can also find a compacted extent already covering it.
        let covered = at
            .checked_sub(1)
            .and_then(|before| self.extents.get(before))
            .is_some_and(|known| known.extent.id_last >= extent.id_first);
        if covered
            || self
                .extents
                .get(at)
                .is_some_and(|known| known.extent.id_first <= extent.id_last)
        {
            return;
        }
        let extent_ref = ExtentRef {
            path: path.clone(),
            extent: extent.clone(),
        };
        if self.track_meta {
            self.meta_dirty = true;
            self.new_extents.push(extent_ref.clone());
        }
        self.extents.insert(at, extent_ref);
    }

    /// Takes away every extent in the object at `path`. How many there were.
    pub fn drop_path(&mut self, path: &Path) -> usize {
        let dropped = &mut self.dropped_extents;
        let expired_through = &mut self.expired_through;
        let before = self.extents.len();
        self.extents.retain(|known| {
            let keep = known.extent_path() != path;
            if !keep {
                dropped.push(known.extent.id_first);
                *expired_through = (*expired_through).max(known.extent.id_last);
            }
            keep
        });
        self.new_extents.retain(|known| known.extent_path() != path);
        let count = before - self.extents.len();
        if count > 0 {
            self.meta_dirty = self.track_meta;
        }
        count
    }

    /// Puts `new` where the extents in `sources` are, all of it or none (section 7.5). They must
    /// still be there, side by side. Anything may be still unsealed, which a merged extent
    /// mustn't go around.
    pub fn replace_extents(
        &mut self,
        path: &Path,
        sources: &[(Path, u64, u64)],
        new: &StreamExtent,
        merge_below: u64,
    ) -> ReplaceResult {
        let Some((_, first, _)) = sources.first() else {
            return ReplaceResult::Gone;
        };
        let at = self
            .extents
            .partition_point(|known| known.extent.id_first < *first);
        let found = self.extents.get(at..at + sources.len()).filter(|known| {
            known.iter().zip(sources).all(|(known, source)| {
                known.path == source.0
                    && known.extent.id_first == source.1
                    && known.extent.id_last == source.2
            })
        });
        if found.is_none() {
            let any_left = sources.iter().any(|source| {
                self.extents
                    .iter()
                    .any(|known| known.path == source.0 && known.extent.id_first == source.1)
            });
            return if any_left {
                ReplaceResult::Retry
            } else {
                ReplaceResult::Gone
            };
        }
        if sources.len() > 1 && new.id_last >= merge_below {
            return ReplaceResult::Retry;
        }
        let new_ref = ExtentRef {
            path: path.clone(),
            extent: new.clone(),
        };
        for known in self
            .extents
            .splice(at..at + sources.len(), [new_ref.clone()])
        {
            self.dropped_extents.push(known.extent.id_first);
            self.new_extents
                .retain(|fresh| fresh.extent.id_first != known.extent.id_first);
        }
        if self.track_meta {
            self.meta_dirty = true;
            self.new_extents.push(new_ref);
        }
        ReplaceResult::Applied
    }

    /// An id known to be above `after` in the extents, and the lowest one if the extent
    /// starts above it. An extent holding `after` shows only its own last id: what comes
    /// next inside it isn't known without reading it (D89).
    pub fn sealed_id_above(&self, after: u64) -> Option<u64> {
        self.first_extent_after(after).map(|known| {
            if known.extent.id_first > after {
                known.extent.id_first
            } else {
                known.extent.id_last
            }
        })
    }

    /// The first extent holding any id above `after`.
    pub fn first_extent_after(&self, after: u64) -> Option<&ExtentRef> {
        let at = self
            .extents
            .partition_point(|known| known.extent.id_last <= after);
        self.extents.get(at)
    }
}

/// The lowest id still in retention, for an ingest time `retention_ms` ago.
pub(crate) fn retention_start_id(now_ms: u64, retention_ms: u64) -> u64 {
    make_id(now_ms.saturating_sub(retention_ms), 0)
}

/// A cursor name for clients. `sink:` names belong to sinks (D27), `compact:` to the compactor,
/// since moving one on would let the delete job expire what was never copied.
pub(crate) fn cursor_name_is_allowed(name: &str) -> bool {
    !name.starts_with("sink:") && !name.starts_with("compact:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use format::Clause;

    fn filter(value: u64) -> Filter {
        Filter::new(&[Clause::Eq { mask: 1, value }]).unwrap()
    }

    /// Thousands of subscribers under each filter change nothing: the count comes from where
    /// a filter is evaluated, so another evaluation anywhere in `check` shows here.
    #[test]
    fn a_check_counts_one_evaluation_per_record_per_filter() {
        let mut table = FilterTable::new(2);
        let mut stored = Vec::new();
        for (id, options) in [(10, 0), (11, 1), (12, 1), (13, 0), (14, 1)] {
            let header = RecordHeader {
                id,
                options,
                author: 5,
                len: 4,
                count: 1,
                codec: 0,
            };
            format::encode_record(&mut stored, &header, b"data").unwrap();
        }
        for at in 0..3_000 {
            table.attach(filter(at % 2), at as usize % 2);
        }
        let mut other_author = filter(0);
        other_author.author = Some(7);
        table.attach(other_author, 0);

        let bytes = SharedBytes::Plain(Bytes::from(stored));
        let mut spans = Vec::new();
        walk_records(&bytes, &mut spans);
        assert_eq!(spans.len(), 5);
        let (matches, check_count, _) = table.check(&bytes, &spans);
        assert_eq!(check_count, 5 * 3);
        assert_eq!(
            matches.len(),
            2,
            "filter 0, filter 1, and the author matches none"
        );
    }
}
