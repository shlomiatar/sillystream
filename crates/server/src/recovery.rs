//! Startup (SPEC section 8.1): one path for a fresh node, a process crash and a machine loss.
//! Read who owned last, what the WAL holds, what the checkpoints hold and any object they
//! don't know of, take ownership, and put back every stream that has something to replay.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use format::{BlockAuthor, KeyHash, Records};
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use storage::{
    BlockEntry, BlockTable, CheckpointSet, MappedSegments, MetaSink, MetaSinkGap, NewerObjects,
    ObjectId, ObjectKey, Ownership, SegmentError, SegmentReader, StreamExtent, StreamMeta,
    claim_generation, list_pending, list_sealed_objects, list_wal_files, object_generation,
    read_ownership, replay_files, sync_wal_files,
};

use crate::dedupe::DedupeTable;
use crate::message::{SinkGap, SinkSpec, StreamConfig};
use crate::metadata::{LocalMeta, load_stream, put_owed_checkpoint};
use crate::state::StreamState;

/// Where a recovering node's files are.
#[derive(Debug, Clone)]
pub struct RecoverConfig {
    pub wal_dir: PathBuf,
    /// The local key-value store (section 7.6).
    pub metadata_dir: PathBuf,
    pub wal_preallocate_bytes: u64,
}

/// What a startup found, which is what the binary logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub generation: u64,
    pub last_valid_id: u64,
    /// `wal_dir` is empty, and a generation already existed: unsealed records may be gone, and
    /// there is no telling how many (section 8.1).
    pub wal_lost: bool,
    pub replayed_entry_count: u64,
    /// WAL files found and made durable before anything could be acked from them (D227).
    pub synced_wal_file_count: u64,
    /// WAL entries above their former generation's limit, left out of replay (section 8.1).
    pub ignored_entry_count: u64,
    /// The files holding those. Never removed, since no segment covers them (D228).
    pub kept_wal_seqs: Vec<u64>,
    /// Closed WAL files with something other than zeros after their valid entries (D252).
    pub damaged_wal_seqs: Vec<u64>,
    pub restored_stream_count: u64,
    /// Segments a former process left waiting to upload, uploaded before anything else.
    /// Segments a former process left in `pending/`, which upload in the background.
    pub pending_segment_count: u64,
    /// Objects no checkpoint knew of, whose extents were looked at.
    pub scanned_object_count: u64,
    /// Extents taken from them, and those a former generation's limit ruled out entirely.
    pub adopted_extent_count: u64,
    pub ignored_extent_count: u64,
}

impl RecoveryReport {
    /// The warning of section 8.1, if there is one to give.
    pub fn warning(&self) -> Option<String> {
        self.wal_lost.then(|| {
            format!(
                "warning: wal_dir is empty but generation {} already existed: the WAL was lost, \
                 and unsealed records may be gone. It can't tell how many.",
                self.generation.saturating_sub(1)
            )
        })
    }

    /// What a former generation's limit ruled out (section 8.1). A former owner still running
    /// wrote it, or a start with `wal_dir` empty, say before its volume was mounted, fenced off
    /// what was on that volume. In the second case acked records are among it (D228).
    /// A closed file's later entries may be gone while later files replay (Q15, D252).
    pub fn damaged_warning(&self) -> Option<String> {
        (!self.damaged_wal_seqs.is_empty()).then(|| {
            format!(
                "warning: WAL files {:?} have unreadable bytes after their valid entries. Entries \
                 after that point were not replayed, so a stream can have a gap in its unsealed \
                 records. This is bit rot, or a torn write left by an earlier crash, and they \
                 can't be told apart.",
                self.damaged_wal_seqs
            )
        })
    }

    pub fn fenced_warning(&self) -> Option<String> {
        (self.ignored_entry_count > 0 || self.ignored_extent_count > 0).then(|| {
            format!(
                "warning: {} WAL entries are above their generation's last valid id, and so are \
                 {} object extents. They were left out and can't be read, and any acked records \
                 among them are gone from the stream. An earlier start with an empty wal_dir, \
                 such as one before its volume was mounted, causes this. WAL files {:?} are kept \
                 and not removed; remove them by hand when they are not wanted.",
                self.ignored_entry_count, self.ignored_extent_count, self.kept_wal_seqs
            )
        })
    }
}

pub(crate) struct RestoredBatch {
    pub bytes: Bytes,
    pub first_id: u64,
    pub last_id: u64,
}

/// One stream, ready for its core.
pub(crate) struct RestoredStream {
    pub meta: StreamMeta,
    /// What only the WAL holds, to go into the next seal.
    pub batches: Vec<RestoredBatch>,
    /// Dedupe entries from the WAL's entries, the only place they survive a restart (section 3).
    pub dedupe: DedupeTable,
}

impl std::fmt::Debug for RestoredStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RestoredStream")
    }
}

pub(crate) struct Prepared {
    pub report: RecoveryReport,
    pub first_seq: u64,
    pub closed_seqs: Vec<u64>,
    pub checkpoints: CheckpointSet,
    pub max_id: u64,
    pub restored: Vec<RestoredStream>,
    /// Objects this startup looked at or put, for the next checkpoint to record. They are in
    /// the lists in `restored`, or hold nothing anybody should read.
    pub scanned_objects: Vec<ScannedObject>,
    /// The last ids of the objects checkpoints record since their full file (section 7.6).
    pub newer: NewerObjects,
    /// The time in the id of the last object this startup put, for the uploader's to follow.
    pub last_object_ms: u64,
}

/// One object no checkpoint recorded.
pub(crate) struct ScannedObject {
    pub id: ObjectId,
    pub len_bytes: u64,
    /// Each stream's last accepted id in it, in key hash order.
    pub last_ids: Vec<(KeyHash, u64)>,
    /// It holds accepted records that aren't the manifest stream's, so it needs a manifest: a
    /// crash can have lost the one it had (section 7.5).
    pub needs_manifest: bool,
    /// Only in `pending/` so far: the uploader puts it up, and until then it is read locally
    /// and nothing records it (section 7.3).
    pub local: bool,
}

pub(crate) fn sink_spec(sink: &MetaSink) -> SinkSpec {
    SinkSpec {
        kind: sink.kind.clone(),
        url: sink.url.clone(),
        target: sink.target,
        filter: sink.filter.clone(),
        on_expire: sink.on_expire.clone(),
        paused: sink.paused,
        secret: sink.secret.clone(),
        budget_bytes: sink.budget_bytes,
        pause_reason: sink.pause_reason.clone(),
        last_gap: sink.last_gap.map(|gap| SinkGap {
            first_id: gap.first_id,
            last_id: gap.last_id,
        }),
        gap_count: sink.gap_count,
        ..SinkSpec::default()
    }
}

pub(crate) fn meta_sink(sink_id: u64, spec: &SinkSpec) -> MetaSink {
    MetaSink {
        sink_id,
        kind: spec.kind.clone(),
        url: spec.url.clone(),
        target: spec.target,
        filter: spec.filter.clone(),
        on_expire: spec.on_expire.clone(),
        paused: spec.paused,
        secret: spec.secret.clone(),
        budget_bytes: spec.budget_bytes,
        pause_reason: spec.pause_reason.clone(),
        last_gap: spec.last_gap.map(|gap| MetaSinkGap {
            first_id: gap.first_id,
            last_id: gap.last_id,
        }),
        gap_count: spec.gap_count,
    }
}

fn new_meta(name: KeyHash) -> StreamMeta {
    let config = StreamConfig::default();
    StreamMeta {
        stream: name,
        default_ack_level: config.default_ack_level,
        min_ack_level: config.min_ack_level,
        retention_ms: config.retention_ms,
        ..StreamMeta::default()
    }
}

/// How much of an extent readers accept of a former generation's object: every whole record
/// at or below `limit` (section 8.1, D138). `None` if nothing is. Reads only headers.
pub(crate) fn accepted_prefix(bytes: &Bytes, limit: u64) -> Option<(u64, Bytes)> {
    let mut accepted_bytes = 0;
    let mut last_id = None;
    for record in Records::new(bytes) {
        let Ok(record) = record else {
            break;
        };
        if record.header.last_id() > limit {
            break;
        }
        accepted_bytes += record.header.total_bytes();
        last_id = Some(record.header.last_id());
    }
    last_id.map(|last_id| (last_id, bytes.slice(..accepted_bytes)))
}

/// A carried dedupe entry's `(first_id, count)`, if that is what this WAL entry is (section 3).
fn dedupe_carry(entry: &storage::ReplayedEntry) -> Option<(u64, u32)> {
    let record = Records::new(&entry.records).next()?.ok()?;
    if !record.header.is_system() || record.header.options != format::ENTRY_DEDUPE_CARRY {
        return None;
    }
    let system = format::SystemRecord::parse(&record).ok()?;
    format::decode_dedupe_carry(system.doc).ok()
}

/// A publish commits as one unit (section 3), so any of it over the limit rules out all of it.
fn entry_is_above(entry: &storage::ReplayedEntry, limit: u64) -> bool {
    Records::new(&entry.records)
        .any(|record| record.is_ok_and(|record| record.header.last_id() > limit))
}

struct ScannedExtent {
    stream: KeyHash,
    object: ObjectId,
    id_first: u64,
    /// The last accepted id.
    id_last: u64,
    /// The blocks that may hold system records, in order. Replay needs nothing else (D264).
    system_blocks: Vec<Bytes>,
}

/// Objects a scan reads at once. Their order is kept.
const CONCURRENT_OBJECT_SCANS: usize = 16;

/// Reads the runs of an extent's blocks that may hold system records.
async fn read_system_blocks(
    reader: &SegmentReader,
    extent: &StreamExtent,
    table: &BlockTable,
) -> Result<Vec<Bytes>, SegmentError> {
    let may_hold = |block: &BlockEntry| !matches!(block.summary.author, BlockAuthor::Single(author) if author != 0);
    let mut blocks = Vec::new();
    let mut index = 0;
    while index < table.blocks.len() {
        if !may_hold(&table.blocks[index]) {
            index += 1;
            continue;
        }
        let start = index;
        while table.blocks.get(index).is_some_and(may_hold) {
            index += 1;
        }
        blocks.push(reader.read_blocks(extent, table, start..index).await?);
    }
    Ok(blocks)
}

/// One object of a scan.
struct ScannedOne {
    extents: Vec<ScannedExtent>,
    object: ScannedObject,
    ignored_extent_count: u64,
}

async fn scan_object(
    store: Arc<dyn ObjectStore>,
    path: Path,
    len_bytes: u64,
    limit: Option<u64>,
    manifest: KeyHash,
) -> Result<ScannedOne, String> {
    let describe = |error: SegmentError| format!("{path}: {error}");
    let id = ObjectId::of_path(&path).ok_or_else(|| format!("{path}: no id in the path"))?;
    let reader = SegmentReader::new(store, path.clone());
    let top = reader.read_top_index().await.map_err(describe)?;
    let heads = reader.read_heads(&top).await.map_err(describe)?;
    let mut out = Vec::new();
    let mut last_ids = Vec::new();
    let mut needs_manifest = false;
    let mut ignored_extent_count = 0;
    for (extent, table) in heads {
        if limit.is_some_and(|limit| extent.id_first > limit) {
            ignored_extent_count += 1;
            continue;
        }
        let (id_last, system_blocks) = match limit {
            // Where the accepted part ends takes the records themselves.
            Some(limit) if extent.id_last > limit => {
                let records = reader.read_extent(&extent).await.map_err(describe)?;
                match accepted_prefix(&records, limit) {
                    Some((id_last, records)) => (id_last, vec![records]),
                    None => {
                        ignored_extent_count += 1;
                        continue;
                    }
                }
            }
            _ => (
                extent.id_last,
                read_system_blocks(&reader, &extent, &table)
                    .await
                    .map_err(describe)?,
            ),
        };
        needs_manifest |= extent.stream != manifest;
        last_ids.push((extent.stream, id_last));
        out.push(ScannedExtent {
            stream: extent.stream,
            object: id,
            id_first: extent.id_first,
            id_last,
            system_blocks,
        });
    }
    last_ids.sort_unstable();
    Ok(ScannedOne {
        extents: out,
        object: ScannedObject {
            id,
            len_bytes,
            last_ids,
            needs_manifest,
            local: false,
        },
        ignored_extent_count,
    })
}

/// Objects no checkpoint records, which a crash left between upload and checkpoint or a
/// former owner uploaded late. Oldest generation first, since one object's extent keeps a
/// later one's out.
async fn scan_objects(
    store: &Arc<dyn ObjectStore>,
    local: Option<PendingSegments>,
    checkpoints: &CheckpointSet,
    ownership: &Ownership,
    objects_seen: &mut Vec<ScannedObject>,
    report: &mut RecoveryReport,
    manifest: KeyHash,
) -> Result<Vec<ScannedExtent>, String> {
    let known: HashSet<ObjectKey> = checkpoints.rows().into_keys().collect();
    let mut objects: Vec<(u64, Path, u64, bool)> = list_sealed_objects(store.as_ref())
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|object| {
            ObjectId::of_path(&object.location)
                .is_some_and(|id| !known.contains(&ObjectKey::sealed(id)))
        })
        .filter_map(|object| {
            Some((
                object_generation(&object.location)?,
                object.location,
                object.size as u64,
                false,
            ))
        })
        .collect();
    // A segment a crash left before its upload: in `pending/` and not in the store, or there
    // already when the crash came before its rename (section 7.3).
    let mut local_store = None;
    if let Some((reader, pending)) = local {
        for (path, len_bytes) in pending {
            let Some(generation) = object_generation(&path) else {
                continue;
            };
            let unknown =
                ObjectId::of_path(&path).is_some_and(|id| !known.contains(&ObjectKey::sealed(id)));
            if unknown && !objects.iter().any(|(_, listed, _, _)| *listed == path) {
                objects.push((generation, path, len_bytes, true));
            }
        }
        local_store = Some(reader);
    }
    objects.sort();
    let mut scans = futures_util::stream::iter(objects.into_iter().map(
        |(generation, path, len_bytes, local)| {
            let limit = ownership.limit_for(generation);
            let from = match (&local_store, local) {
                (Some(reader), true) => Arc::clone(reader),
                _ => Arc::clone(store),
            };
            async move {
                let mut scan = scan_object(from, path, len_bytes, limit, manifest).await?;
                scan.object.local = local;
                Ok::<_, String>(scan)
            }
        },
    ))
    .buffered(CONCURRENT_OBJECT_SCANS);
    let mut out = Vec::new();
    while let Some(scan) = scans.next().await {
        let scan = scan?;
        report.scanned_object_count += 1;
        report.ignored_extent_count += scan.ignored_extent_count;
        out.extend(scan.extents);
        objects_seen.push(scan.object);
    }
    Ok(out)
}

/// What replay has of one stream, in any order until `finish` sorts it.
#[derive(Default)]
struct Pending {
    scanned: Vec<ScannedExtent>,
    wal: Vec<storage::ReplayedEntry>,
}

/// A reader of the local segment directory, and what `pending/` holds.
type PendingSegments = (Arc<dyn ObjectStore>, Vec<(Path, u64)>);

/// A stream put together, and the id ranges of the scanned objects holding it.
type Replayed = (RestoredStream, Vec<(u64, u64)>);

/// Puts one stream together: its saved metadata, then every record above it, in id order.
/// `recorded` is each object recorded since the full file holding it, with its last id there.
/// Says the id ranges of the scanned objects holding it, which the WAL's entries may be in.
fn replay_stream(
    mut meta: StreamMeta,
    pending: Pending,
    recorded: &[(ObjectId, u64)],
    sealed_through: u64,
    now_ms: u64,
    report: &mut RecoveryReport,
) -> Result<Option<Replayed>, String> {
    let name = meta.stream;
    let was_deleted = meta.deleted;
    let floor_id = if was_deleted {
        meta.last_id
    } else {
        meta.floor_id
    };
    // What the scanned objects hold of it, which covers a WAL entry that is in one.
    let mut covering: Vec<(u64, u64)> = Vec::new();

    enum Item {
        Scanned {
            blocks: Vec<Bytes>,
            last_id: u64,
        },
        Wal(storage::ReplayedEntry),
        /// A dedupe entry, which says nothing else of the stream (section 3).
        Carried {
            producer_id: u64,
            producer_seq: u64,
            count: u32,
        },
    }
    let mut items: Vec<(u64, Item)> = Vec::new();
    let listed: HashSet<ObjectId> = meta.objects.iter().copied().collect();
    for scanned in pending.scanned {
        let overlaps = covering
            .iter()
            .any(|(first, last)| *first <= scanned.id_last && scanned.id_first <= *last);
        // A former generation's upload, late, of what a later one already sealed (section 8.1).
        let superseded = recorded.iter().any(|(id, last_id)| {
            id.generation() > scanned.object.generation() && scanned.id_first <= *last_id
        });
        if scanned.id_last <= floor_id || overlaps || superseded {
            continue;
        }
        covering.push((scanned.id_first, scanned.id_last));
        // Listed already: the local store had it, and replay applied what it holds.
        if listed.contains(&scanned.object) {
            continue;
        }
        report.adopted_extent_count += 1;
        items.push((
            scanned.id_first,
            Item::Scanned {
                blocks: scanned.system_blocks,
                last_id: scanned.id_last,
            },
        ));
        meta.objects.push(scanned.object);
    }
    meta.objects.sort_unstable();
    meta.objects.dedup();
    for entry in pending.wal {
        if let Some((first_id, count)) = dedupe_carry(&entry) {
            let carried = Item::Carried {
                producer_id: entry.producer_id,
                producer_seq: entry.producer_seq,
                count,
            };
            items.push((first_id, carried));
            continue;
        }
        let first_id = format::RecordHeader::read(&entry.records)
            .map_err(|error| format!("a WAL entry for {name}: {error}"))?
            .id;
        items.push((first_id, Item::Wal(entry)));
    }
    items.sort_by_key(|(first_id, _)| *first_id);

    let saved_applied_id = meta.applied_id.max(floor_id);
    let mut applied_id = saved_applied_id;
    let mut state = StreamState::new();
    let mut dedupe = DedupeTable::new();
    let mut alive = !was_deleted;
    let mut batches = Vec::new();
    if alive {
        state.install(meta.state.clone(), meta.state_ver);
    }
    for (first_id, item) in items {
        let (parts, producer, known_last_id) = match &item {
            Item::Scanned { blocks, last_id } => (blocks.as_slice(), None, Some(*last_id)),
            Item::Wal(entry) => (
                std::slice::from_ref(&entry.records),
                Some((entry.producer_id, entry.producer_seq)),
                None,
            ),
            Item::Carried {
                producer_id,
                producer_seq,
                count,
            } => {
                let last_id = first_id + u64::from((*count).max(1)) - 1;
                // None outlives its stream's delete.
                if alive && last_id > floor_id {
                    dedupe.record(*producer_id, *producer_seq, first_id, *count, now_ms);
                }
                continue;
            }
        };
        let mut last_id = first_id;
        let mut item_count = 0u64;
        for bytes in parts {
            for record in Records::new(bytes) {
                let record = record.map_err(|error| format!("a record of {name}: {error}"))?;
                last_id = record.header.last_id();
                item_count += u64::from(record.header.count);
                // The one case the runtime reads a record's data (section 2.2).
                if record.header.is_system()
                    && record.header.options != format::ENTRY_SEAL_MANIFEST
                    && record.header.id > applied_id
                {
                    state
                        .replay(&record)
                        .map_err(|error| format!("a state op of {name}: {error}"))?;
                }
            }
        }
        let last_id = known_last_id.unwrap_or(last_id);
        if last_id <= floor_id {
            continue;
        }
        if !alive {
            // Made again after the delete: what the tombstone said is the floor now.
            alive = true;
            let fresh = new_meta(name);
            meta.default_ack_level = fresh.default_ack_level;
            meta.min_ack_level = fresh.min_ack_level;
            meta.retention_ms = fresh.retention_ms;
            meta.deleted = false;
            meta.floor_id = floor_id;
            meta.cursors.clear();
            meta.sinks.clear();
        }
        applied_id = applied_id.max(last_id);
        meta.last_id = meta.last_id.max(last_id);
        if let Some((producer_id, producer_seq)) = producer {
            if producer_id != 0 {
                dedupe.record(
                    producer_id,
                    producer_seq,
                    first_id,
                    item_count as u32,
                    now_ms,
                );
            }
            // A recorded object holding the stream up to an id holds every entry below it,
            // since a stream's records are sealed in id order.
            let covered = last_id <= sealed_through
                || covering
                    .iter()
                    .any(|(first, last)| *first <= first_id && last_id <= *last);
            if !covered && let Item::Wal(entry) = item {
                batches.push(RestoredBatch {
                    bytes: entry.records,
                    first_id,
                    last_id,
                });
            }
        }
    }
    if !alive {
        return Ok(None);
    }
    meta.applied_id = applied_id;
    meta.state_ver = state.ver();
    meta.state = state.doc().clone();
    Ok(Some((
        RestoredStream {
            meta,
            batches,
            dedupe,
        },
        covering,
    )))
}

/// Everything up to and including taking ownership. A process that loses that race has written
/// nothing but a checkpoint its own crash owed, has emptied at most a stale local store (D151),
/// and must stop.
pub(crate) async fn prepare(
    wal_dir: &FsPath,
    segment_dir: Option<&FsPath>,
    local_store: storage::MetadataStore,
    store: &Arc<dyn ObjectStore>,
    now_ms: u64,
    manifest: KeyHash,
) -> Result<(Prepared, Arc<LocalMeta>), String> {
    let mut report = RecoveryReport::default();
    // The uploader puts these up in the background. The scan reads them locally and finds them
    // as it finds any object no checkpoint records (section 7.3).
    let pending_segments = match segment_dir {
        Some(dir) => {
            let pending = list_pending(dir)?;
            report.pending_segment_count = pending.len() as u64;
            let reader: Arc<dyn ObjectStore> = Arc::new(MappedSegments::new(dir));
            Some((reader, pending))
        }
        None => None,
    };
    let mut ownership = read_ownership(store.as_ref())
        .await
        .map_err(|error| error.to_string())?;
    let wal_files = list_wal_files(wal_dir).map_err(|error| error.to_string())?;
    report.wal_lost = wal_files.is_empty() && ownership.current_generation().is_some();
    // A crash can leave entries no sync covered, and they count as durable from here on (D227).
    sync_wal_files(wal_dir, &wal_files).map_err(|error| error.to_string())?;
    report.synced_wal_file_count = wal_files.len() as u64;

    put_owed_checkpoint(&local_store, store).await?;
    let mut checkpoints = CheckpointSet::read(store, ownership.clone())
        .await
        .map_err(|error| error.to_string())?;
    let local = Arc::new(
        LocalMeta::open(
            local_store,
            ownership.current_generation(),
            checkpoints.files.is_empty(),
        )
        .map_err(|error| error.to_string())?,
    );
    let mut scanned_objects = Vec::new();
    let scanned = scan_objects(
        store,
        pending_segments,
        &checkpoints,
        &ownership,
        &mut scanned_objects,
        &mut report,
        manifest,
    )
    .await?;

    let mut pending: BTreeMap<KeyHash, Pending> = BTreeMap::new();
    for extent in scanned {
        pending
            .entry(extent.stream)
            .or_default()
            .scanned
            .push(extent);
    }
    let mut closed_seqs = Vec::with_capacity(wal_files.len());
    let mut empty_files = Vec::new();
    let mut first_seq = 1;
    for (file, replayed) in wal_files.iter().zip(replay_files(&wal_files)) {
        closed_seqs.push(file.seq);
        first_seq = first_seq.max(file.seq + 1);
        let replayed = replayed.map_err(|error| error.to_string())?;
        // The newest file's tail is the torn write the CRC is for.
        if replayed.damaged_tail && wal_files.last().is_some_and(|last| last.seq != file.seq) {
            report.damaged_wal_seqs.push(file.seq);
        }
        // Carried dedupe entries count as nothing here: replay reads them, and the new file
        // carries them again, so a crash loop doesn't keep a file per start.
        if replayed
            .entries
            .iter()
            .all(|entry| dedupe_carry(entry).is_some())
        {
            empty_files.push(file);
        }
        // A file from before generations were known to it has nothing to say.
        let limit = (replayed.generation != 0)
            .then(|| ownership.limit_for(replayed.generation))
            .flatten();
        for entry in replayed.entries {
            if limit.is_some_and(|limit| entry_is_above(&entry, limit)) {
                report.ignored_entry_count += 1;
                if report.kept_wal_seqs.last() != Some(&file.seq) {
                    report.kept_wal_seqs.push(file.seq);
                    closed_seqs.retain(|seq| *seq != file.seq);
                }
                continue;
            }
            if dedupe_carry(&entry).is_none() {
                report.replayed_entry_count += 1;
            }
            pending.entry(entry.stream).or_default().wal.push(entry);
        }
    }

    let newer = NewerObjects::read(store, &checkpoints.newer_objects())
        .await
        .map_err(|error| error.to_string())?;
    // A recorded object can hold what the WAL still has, when a crash came after its checkpoint
    // was owed or written and before the WAL file went. Such an object is no older than the
    // oldest entry, since an object's time is at least its newest record's.
    let oldest_entry_ms = pending
        .values()
        .flat_map(|one| one.wal.iter())
        .filter(|entry| dedupe_carry(entry).is_none())
        .filter_map(|entry| format::RecordHeader::read(&entry.records).ok())
        .map(|header| format::id_ms(header.id))
        .min();
    let recent: Vec<ObjectId> = match oldest_entry_ms {
        Some(from_ms) => checkpoints
            .rows()
            .into_keys()
            .filter(|key| key.class.is_none() && key.id.time_ms() >= from_ms)
            .map(|key| key.id)
            .collect(),
        None => Vec::new(),
    };
    let recent = NewerObjects::read(store, &recent)
        .await
        .map_err(|error| error.to_string())?;
    let mut restored = Vec::with_capacity(pending.len());
    let mut covering_by_stream = HashMap::new();
    for (name, one) in pending {
        let (meta, _) = load_stream(&local, &checkpoints, &newer, store, name).await?;
        // Carried dedupe entries alone don't make a stream (section 3).
        let only_carried =
            one.scanned.is_empty() && one.wal.iter().all(|entry| dedupe_carry(entry).is_some());
        if meta.is_none() && only_carried {
            continue;
        }
        let meta = meta.unwrap_or_else(|| new_meta(name));
        let recorded = newer.of(name, &checkpoints);
        let sealed_through = recent
            .of(name, &checkpoints)
            .into_iter()
            .map(|(_, last_id)| last_id)
            .max()
            .unwrap_or(0);
        if let Some((stream, covering)) =
            replay_stream(meta, one, &recorded, sealed_through, now_ms, &mut report)?
        {
            covering_by_stream.insert(name, covering);
            restored.push(stream);
        }
    }
    report.restored_stream_count = restored.len() as u64;

    let max_id = restored
        .iter()
        .map(|stream| stream.meta.last_id)
        .chain([
            checkpoints.max_id(),
            local.max_id().map_err(|error| error.to_string())?,
        ])
        .max()
        .unwrap_or(0);
    let (generation, last_valid_id) = claim_generation(store.as_ref(), &ownership, max_id)
        .await
        .map_err(|error| error.to_string())?;
    local.claim(generation).map_err(|error| error.to_string())?;
    // Only now that this process owns the disks. The newest file stays, whatever it holds, so
    // that a crash right here doesn't look like a lost WAL (D155).
    for file in empty_files {
        if wal_files.last().is_some_and(|last| last.seq != file.seq)
            && std::fs::remove_file(&file.path).is_ok()
        {
            closed_seqs.retain(|seq| *seq != file.seq);
        }
    }
    // Seals within one ms move each object's time on, so a known object can be timed past the
    // clock. This generation's objects come after them all, or a list would be out of record
    // order.
    let known_ms = checkpoints
        .rows()
        .into_keys()
        .filter(|key| key.class.is_none())
        .map(|key| key.id)
        .chain(scanned_objects.iter().map(|object| object.id))
        .map(|id| id.time_ms())
        .max()
        .unwrap_or(0);
    let last_object_ms = put_restored_parts(
        store.as_ref(),
        generation,
        manifest,
        &mut restored,
        &covering_by_stream,
        &mut scanned_objects,
    )
    .await?
    .max(known_ms);
    ownership.record(generation, last_valid_id);
    checkpoints.ownership = ownership;
    report.generation = generation;
    report.last_valid_id = last_valid_id;
    Ok((
        Prepared {
            report,
            first_seq,
            closed_seqs,
            checkpoints,
            max_id: last_valid_id,
            restored,
            scanned_objects,
            newer,
            last_object_ms,
        },
        local,
    ))
}

/// A stream's restored records split wherever a scanned object's records lie between them, and
/// an object holds one region per stream (D157, D275). A list is in record order, so a part
/// with a scanned object above it goes up here as its own object, under this generation, at
/// its newest record's ms or a listed object's below it if later, and joins the stream's list;
/// only a last part with nothing above it goes into the first seal. Says the latest time put.
async fn put_restored_parts(
    store: &dyn ObjectStore,
    generation: u64,
    manifest: KeyHash,
    restored: &mut [RestoredStream],
    covering_by_stream: &HashMap<KeyHash, Vec<(u64, u64)>>,
    scanned_objects: &mut Vec<ScannedObject>,
) -> Result<u64, String> {
    let mut last_object_ms = 0;
    // The index each time has given out: objects of this generation at one ms differ by it.
    let mut used: HashMap<u64, u8> = HashMap::new();
    for stream in restored {
        let name = stream.meta.stream;
        let covering = covering_by_stream.get(&name).map_or(&[][..], Vec::as_slice);
        let mut parts: Vec<Vec<RestoredBatch>> = Vec::new();
        for batch in std::mem::take(&mut stream.batches) {
            let split = parts
                .last()
                .and_then(|part| part.last())
                .is_none_or(|previous| {
                    covering
                        .iter()
                        .any(|(first, _)| previous.last_id < *first && *first < batch.first_id)
                });
            if split {
                parts.push(Vec::new());
            }
            parts.last_mut().expect("pushed above").push(batch);
        }
        let above = |part: &[RestoredBatch]| {
            let last_id = part.last().map_or(0, |batch| batch.last_id);
            covering.iter().any(|(first, _)| *first > last_id)
        };
        if parts.last().is_some_and(|part| !above(part)) {
            stream.batches = parts.pop().unwrap_or_default();
        }
        for part in parts {
            let mut builder = storage::SegmentBuilder::new();
            builder
                .add_extent(name, part.iter().map(|batch| batch.bytes.clone()))
                .map_err(|error| format!("restoring {name}: {error}"))?;
            let built = builder.finish().map_err(|error| error.to_string())?;
            let last_id = built.last_id.expect("a part has records");
            // At or after every listed object below it, which a seal within one ms can have
            // timed past its records. Only a scanned object can be above.
            let below_ms = stream
                .meta
                .objects
                .iter()
                .filter(|id| {
                    !scanned_objects.iter().any(|object| {
                        object.id == **id
                            && object
                                .last_ids
                                .iter()
                                .any(|(key, last)| *key == name && *last > last_id)
                    })
                })
                .map(|id| id.time_ms())
                .max()
                .unwrap_or(0);
            let mut time_ms = storage::sealed_time_ms(
                format::id_ms(last_id)
                    .max(below_ms)
                    .max(storage::OBJECT_EPOCH_MS),
            );
            let index = loop {
                let next = used.entry(time_ms).or_insert(0);
                if let Some(after) = next.checked_add(1) {
                    let index = *next;
                    *next = after;
                    break index;
                }
                time_ms = storage::sealed_time_ms(time_ms + 1);
            };
            last_object_ms = last_object_ms.max(time_ms);
            let id = ObjectId::new(time_ms, generation, index);
            let path = storage::sealed_path(id);
            storage::put_object(store, &path, &built.parts).await?;
            stream.meta.objects.push(id);
            stream.meta.objects.sort_unstable();
            scanned_objects.push(ScannedObject {
                id,
                len_bytes: built.len_bytes,
                last_ids: vec![(name, last_id)],
                needs_manifest: name != manifest,
                local: false,
            });
        }
    }
    Ok(last_object_ms)
}
