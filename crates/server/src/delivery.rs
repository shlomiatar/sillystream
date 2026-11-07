//! The owner's side of delivery (SPEC sections 5.1, 5.2): which stamped records are visible
//! yet, the filter table, named cursors and where a stream's sealed records are.
//!
//! All of this is a stream's own, and moves with it. The connection's side, on each home
//! core, is in `connection.rs`.

use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use format::{Filter, KeyHash, RecordHeader, make_id};
use storage::{ObjectId, ObjectKey, ObjectTable, RetentionClass};

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

    /// Adds a subscriber, and returns its filter's id. An id isn't reused while the stream
    /// lives, so that a late message can't be taken for a later filter. A stream made again
    /// after a delete starts again from 0, and a late detach from before is only taken for
    /// one of its filters if its home core has a subscriber there to lose.
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

    /// Whether it took a filter's subscriber away. A late detach finds nothing to take.
    pub fn detach(&mut self, filter_id: u32, home: usize) -> bool {
        if filter_id == STATE_ONLY_FILTER_ID {
            self.state_subs_by_core[home] = self.state_subs_by_core[home].saturating_sub(1);
            return false;
        }
        let Some(at) = self.entries.iter().position(|entry| entry.id == filter_id) else {
            return false;
        };
        let entry = &mut self.entries[at];
        if entry.subs_by_core[home] == 0 {
            return false;
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
        true
    }

    /// Filter subscribers from every core, as a shard's `subscriber_count` counts them.
    pub fn subscriber_count(&self) -> u64 {
        self.subs_by_core
            .iter()
            .map(|count| u64::from(*count))
            .sum()
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
}

/// An object in a stream's list, and where a reader reads it: itself, or the compacted object
/// replacing it for the stream's class (section 7.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObjectRef {
    pub stream: KeyHash,
    pub listed: ObjectId,
    pub key: ObjectKey,
}

/// A stream's changes for a checkpoint: its metadata, whether any of it beyond the last id and
/// the list changed (which the object store's checkpoint takes), and its list's changes (which
/// only the local store takes).
#[derive(Debug, Clone)]
pub(crate) struct Gathered {
    pub meta: storage::StreamMeta,
    pub meta_changed: bool,
    /// A rewrite has the whole list in `appended`.
    pub list: ListChange,
}

/// How a stream's object list changed since a checkpoint gather took it, for the local store
/// (section 7.6): ids added at the end, the front cut by expiry, or a rewrite.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ListChange {
    pub appended: Vec<ObjectId>,
    /// Every id below this one is gone.
    pub expired_below: Option<ObjectId>,
    /// The list changed some other way, so the whole of it is written.
    pub rewritten: bool,
}

impl ListChange {
    pub fn is_empty(&self) -> bool {
        self.appended.is_empty() && self.expired_below.is_none() && !self.rewritten
    }
}

/// What a visible batch changes in the saved view, once its ticket is durable.
#[derive(Debug)]
pub(crate) struct UnsavedStep {
    pub core: usize,
    pub ticket: u64,
    pub last_id: u64,
    pub state: Option<(u64, Bytes)>,
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
    pub cursors: HashMap<KeyHash, u64>,
    /// The object list (section 7.6): ascending, which is the order of the records.
    pub objects: Vec<ObjectId>,
    /// The last id of a deleted stream of this name. Nothing at or below it is readable.
    pub floor_id: u64,
    /// The stored highest expired id: what an object found gone took away. Retention's cutoff
    /// adds to it when used (section 7.4). A position below this has lost records.
    pub expired_through: u64,
    pub sinks: std::collections::BTreeMap<u64, crate::message::SinkSpec>,
    /// The highest epoch of a sink deleted here. Not saved: no delivery outlives a restart.
    pub deleted_sink_epoch: u64,
    /// What a checkpoint may save: the stream as of its last record in the WAL. An entry for a
    /// publish a crash can erase would ack it on the retry, unwritten (D136, D154).
    pub saved_through: u64,
    pub saved_state: (u64, Bytes),
    /// Visible, and not yet in the WAL, in id order. Only with `track_meta`.
    pub unsaved: VecDeque<UnsavedStep>,
    /// The last id each core held when this stream left it. Its object still has to reach
    /// here, and until it does this stream's owner list is the only way to that core (D153).
    pub held_elsewhere: Vec<u64>,
    /// Whether this stream's metadata is saved at seals. Off on a node without a store.
    pub track_meta: bool,
    /// State, config, cursors, sinks or the tombstone changed, which a checkpoint saves.
    pub meta_dirty: bool,
    /// The last checkpoint gather that took this stream's changes.
    pub saved_gather: u64,
    /// What the list did since the last gather, which only the local store saves.
    pub list_change: ListChange,
    /// The last id as the last gather took it.
    pub saved_last_id: u64,
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
            objects: Vec::new(),
            floor_id: 0,
            expired_through: 0,
            sinks: std::collections::BTreeMap::new(),
            deleted_sink_epoch: 0,
            saved_through: 0,
            saved_state: (0, Bytes::new()),
            unsaved: VecDeque::new(),
            held_elsewhere: Vec::new(),
            track_meta: false,
            meta_dirty: false,
            saved_gather: 0,
            list_change: ListChange::default(),
            saved_last_id: 0,
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

    /// An object holding `id_first..=id_last` of the stream. Both cores of a seal report it, so
    /// it may arrive twice.
    pub fn add_object(&mut self, id: ObjectId, id_first: u64, id_last: u64) {
        self.held_elsewhere
            .retain(|held| !(id_first <= *held && *held <= id_last));
        let Err(at) = self.objects.binary_search(&id) else {
            return;
        };
        self.objects.insert(at, id);
        if self.track_meta {
            if at + 1 == self.objects.len() && !self.list_change.rewritten {
                self.list_change.appended.push(id);
            } else {
                self.list_change.rewritten = true;
            }
        }
    }

    /// An object found gone from the store: what it held is lost. The highest expired id goes
    /// up to below its time only, since the next object or memory can hold ids of that ms and
    /// later, and memory's are still readable. Whether it was there.
    pub fn drop_object(&mut self, id: ObjectId, in_memory: Option<u64>) -> bool {
        let Ok(at) = self.objects.binary_search(&id) else {
            return false;
        };
        self.objects.remove(at);
        let mut through = make_id(id.time_ms(), 0).saturating_sub(1);
        if let Some(first) = in_memory {
            through = through.min(first.saturating_sub(1));
        }
        self.expired_through = self.expired_through.max(through);
        if self.track_meta {
            self.meta_dirty = true;
            self.list_change.rewritten = true;
        }
        true
    }

    /// Drops the objects whose time is before the cutoff, which hold nothing still in
    /// retention. The highest expired id becomes at least the last before the cutoff (section
    /// 7.4).
    pub fn expire(&mut self, now_ms: u64, retention_ms: u64) {
        let cutoff_ms = now_ms.saturating_sub(retention_ms);
        let expired = self.objects.partition_point(|id| id.time_ms() < cutoff_ms);
        if expired == 0 {
            return;
        }
        self.objects.drain(..expired);
        let through = make_id(cutoff_ms, 0).saturating_sub(1);
        if through > self.expired_through {
            self.expired_through = through;
            self.meta_dirty |= self.track_meta;
        }
        if self.track_meta {
            let below = self.objects.first().copied().unwrap_or(ObjectId(u64::MAX));
            self.list_change.appended.retain(|id| *id >= below);
            self.list_change.expired_below = Some(below);
        }
    }

    /// Puts a stream's compacted ids in place of an hour's sealed ones, once every one of them
    /// has been replaced (section 7.6). A part-compacted hour stays as it is: the table leads a
    /// reader from each sealed id to its copy, and only whole hours keep the list in order.
    pub fn rewrite_compacted(&mut self, table: &ObjectTable, class: RetentionClass, hash: u64) {
        let mut out: Vec<ObjectId> = Vec::with_capacity(self.objects.len());
        let mut changed = false;
        let mut at = 0;
        while at < self.objects.len() {
            let hour = self.objects[at].hour();
            let end = at + self.objects[at..].partition_point(|id| id.hour() == hour);
            let hour_ids = &self.objects[at..end];
            let replaced: Option<Vec<ObjectId>> = hour_ids
                .iter()
                .map(|id| {
                    if id.is_compacted() {
                        Some(*id)
                    } else {
                        table
                            .get(ObjectKey::sealed(*id))
                            .and_then(|row| row.replaced_by(class, hash))
                    }
                })
                .collect();
            match replaced {
                Some(mut ids) if hour_ids.iter().any(|id| !id.is_compacted()) => {
                    ids.sort_unstable();
                    ids.dedup();
                    out.extend(ids);
                    changed = true;
                }
                _ => out.extend_from_slice(hour_ids),
            }
            at = end;
        }
        if changed {
            self.objects = out;
            if self.track_meta {
                self.list_change.rewritten = true;
            }
        }
    }

    /// An id at or below the lowest the list holds: an object's records are from the hour
    /// before its time at the earliest, since a seal window is at most 120 s and a compacted
    /// object's sources are of its hour.
    pub fn earliest_id_bound(&self) -> Option<u64> {
        let first = self.objects.first()?;
        let from_ms = first.time_ms().saturating_sub(storage::HOUR_MS);
        Some(make_id(from_ms, 0).max(self.expired_through.saturating_add(1)))
    }

    /// The first object that may hold an id above `after`, past `past` if given: its time is at
    /// least `after`'s ms, since an object's time is at least its newest record's.
    pub fn next_object(&self, after: u64, past: Option<ObjectId>) -> Option<ObjectId> {
        let after_ms = format::id_ms(after);
        let start = past.map_or(0, |past| self.objects.partition_point(|id| *id <= past));
        self.objects[start..]
            .iter()
            .find(|id| id.time_ms() >= after_ms)
            .copied()
    }
}

/// The lowest id still in retention, for an ingest time `retention_ms` ago.
pub(crate) fn retention_start_id(now_ms: u64, retention_ms: u64) -> u64 {
    make_id(now_ms.saturating_sub(retention_ms), 0)
}

/// The cursors clients can name on one stream, and how long a name can be. `read` is enough to
/// write one, and they last as long as the stream, so nothing else limits them (D218).
pub const NAMED_CURSOR_CAP_COUNT: usize = 1000;
pub const CURSOR_NAME_MAX_BYTES: usize = 256;

/// What a client hears for a name `cursor_name_is_allowed` refuses.
pub(crate) const CURSOR_NAME_REFUSED: &str =
    "a cursor name is at most 256 bytes, and sink: and compact: names are reserved";

/// A cursor name for clients. `sink:` names belong to sinks (D27), `compact:` to the compactor,
/// since moving one on would let the delete job expire what was never copied.
pub(crate) fn cursor_name_is_allowed(name: &str) -> bool {
    name.len() <= CURSOR_NAME_MAX_BYTES
        && !name.starts_with("sink:")
        && !name.starts_with("compact:")
}

/// Whether `name` can be stored: it is there already, it is one of `internal`, the cursors of
/// the stream's sinks and of the compactor, or the stream has room for one more of the clients'.
/// Names are key hashes by now, so which are whose is what `internal` says.
pub(crate) fn cursor_has_room(
    cursors: &HashMap<KeyHash, u64>,
    name: KeyHash,
    internal: &[KeyHash],
) -> bool {
    cursors.contains_key(&name)
        || internal.contains(&name)
        || cursors
            .keys()
            .filter(|name| !internal.contains(name))
            .count()
            < NAMED_CURSOR_CAP_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;
    use format::Clause;

    fn filter(value: u64) -> Filter {
        Filter::new(&[Clause::Eq { mask: 1, value }]).unwrap()
    }

    /// A detach that comes late takes nothing, and says so, so that the shard's count of
    /// subscribers only goes down for one that was there.
    #[test]
    fn a_detach_says_whether_it_took_a_subscriber() {
        let mut table = FilterTable::new(2);
        let id = table.attach(filter(1), 0);
        table.attach(filter(1), 1);
        assert_eq!(table.subscriber_count(), 2);
        assert!(table.detach(id, 0));
        assert!(!table.detach(id, 0));
        assert!(!table.detach(id + 1, 1));
        assert!(!table.detach(STATE_ONLY_FILTER_ID, 1));
        assert_eq!(table.subscriber_count(), 1);
        assert!(table.detach(id, 1));
        assert_eq!(table.subscriber_count(), 0);
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
