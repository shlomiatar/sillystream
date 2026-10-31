//! Startup (SPEC section 8.1): one path for a fresh node, a process crash and a machine loss.
//! Read who owned last, what the WAL holds, what the checkpoints hold and any object they
//! don't know of, take ownership, and put back every stream that has something to replay.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use format::Records;
use object_store::ObjectStore;
use object_store::path::Path;
use storage::{
    CheckpointSet, MetaDedupe, MetaExtent, MetaSink, MetaSinkGap, Ownership, SegmentReader,
    StreamExtent, StreamMeta, claim_generation, list_sealed_objects, list_wal_files,
    object_generation, read_ownership, replay_file, sync_wal_files,
};

use crate::compaction::MANIFEST_STREAM;
use crate::dedupe::DedupeTable;
use crate::delivery::ExtentRef;
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
    /// The files holding those. Never recycled, since no segment covers them (D228).
    pub kept_wal_seqs: Vec<u64>,
    pub restored_stream_count: u64,
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
    pub fn fenced_warning(&self) -> Option<String> {
        (self.ignored_entry_count > 0 || self.ignored_extent_count > 0).then(|| {
            format!(
                "warning: {} WAL entries are above their generation's last valid id, and so are \
                 {} object extents. They were left out and can't be read, and any acked records \
                 among them are gone from the stream. An earlier start with an empty wal_dir, \
                 such as one before its volume was mounted, causes this. WAL files {:?} are kept \
                 and not recycled; remove them by hand when they are not wanted.",
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
    /// Those of `meta.extents` no checkpoint has yet.
    pub new_extents: Vec<MetaExtent>,
    /// What only the WAL holds, to go into the next seal.
    pub batches: Vec<RestoredBatch>,
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
    /// Objects this startup looked at, for the next checkpoint to record. Their extents are in
    /// `restored`, or are ones nobody should read.
    pub scanned_objects: Vec<ScannedObject>,
}

/// One object no checkpoint recorded.
pub(crate) struct ScannedObject {
    pub path: String,
    pub len_bytes: u64,
    /// It holds accepted records that aren't the manifest stream's, so it needs a manifest: a
    /// crash can have lost the one it had (section 7.5).
    pub needs_manifest: bool,
}

pub(crate) fn extent_ref(stream: &Arc<str>, extent: &MetaExtent) -> ExtentRef {
    ExtentRef {
        path: Path::parse(&extent.path).unwrap_or_else(|_| Path::from(extent.path.as_str())),
        extent: StreamExtent {
            stream: Arc::clone(stream),
            offset: extent.offset,
            len_bytes: extent.len_bytes,
            id_first: extent.id_first,
            id_last: extent.id_last,
            block_count: extent.block_count,
        },
    }
}

pub(crate) fn meta_extent(known: &ExtentRef) -> MetaExtent {
    MetaExtent {
        path: known.path.to_string(),
        offset: known.extent.offset,
        len_bytes: known.extent.len_bytes,
        id_first: known.extent.id_first,
        id_last: known.extent.id_last,
        block_count: known.extent.block_count,
    }
}

pub(crate) fn sink_spec(sink: &MetaSink) -> SinkSpec {
    SinkSpec {
        kind: sink.kind.clone(),
        url: sink.url.clone(),
        target: sink.target.clone(),
        filter: sink.filter.clone(),
        on_expire: sink.on_expire.clone(),
        concurrency: sink.concurrency,
        paused: sink.paused,
        secret: sink.secret.clone(),
        interval_ms: sink.interval_ms,
        budget_bytes: sink.budget_bytes,
        pause_reason: sink.pause_reason.clone(),
        gaps: sink
            .gaps
            .iter()
            .map(|gap| SinkGap {
                first_id: gap.first_id,
                last_id: gap.last_id,
            })
            .collect(),
        gap_count: sink.gap_count,
        ..SinkSpec::default()
    }
}

pub(crate) fn meta_sink(sink_id: u64, spec: &SinkSpec) -> MetaSink {
    MetaSink {
        sink_id,
        kind: spec.kind.clone(),
        url: spec.url.clone(),
        target: spec.target.clone(),
        filter: spec.filter.clone(),
        on_expire: spec.on_expire.clone(),
        concurrency: spec.concurrency,
        paused: spec.paused,
        secret: spec.secret.clone(),
        interval_ms: spec.interval_ms,
        budget_bytes: spec.budget_bytes,
        pause_reason: spec.pause_reason.clone(),
        gaps: spec
            .gaps
            .iter()
            .map(|gap| MetaSinkGap {
                first_id: gap.first_id,
                last_id: gap.last_id,
            })
            .collect(),
        gap_count: spec.gap_count,
    }
}

fn new_meta(name: &str) -> StreamMeta {
    let config = StreamConfig::default();
    StreamMeta {
        name: name.to_owned(),
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

/// A publish commits as one unit (section 3), so any of it over the limit rules out all of it.
fn entry_is_above(entry: &storage::ReplayedEntry, limit: u64) -> bool {
    Records::new(&entry.records)
        .any(|record| record.is_ok_and(|record| record.header.last_id() > limit))
}

struct ScannedExtent {
    stream: Arc<str>,
    extent: MetaExtent,
    records: Bytes,
}

/// Objects no checkpoint records, which a crash left between upload and checkpoint or a
/// former owner uploaded late. Oldest generation first, since one object's extent keeps a
/// later one's out.
async fn scan_objects(
    store: &Arc<dyn ObjectStore>,
    checkpoints: &CheckpointSet,
    ownership: &Ownership,
    objects_seen: &mut Vec<ScannedObject>,
    report: &mut RecoveryReport,
) -> Result<Vec<ScannedExtent>, String> {
    let known: HashSet<&str> = checkpoints.recorded_objects().collect();
    let mut objects: Vec<(u64, Path, u64)> = list_sealed_objects(store.as_ref())
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|object| !known.contains(object.location.as_ref()))
        .filter_map(|object| {
            Some((
                object_generation(&object.location)?,
                object.location,
                object.size as u64,
            ))
        })
        .collect();
    objects.sort();
    let mut out = Vec::new();
    for (generation, path, len_bytes) in objects {
        report.scanned_object_count += 1;
        let mut needs_manifest = false;
        let limit = ownership.limit_for(generation);
        let reader = SegmentReader::new(Arc::clone(store), path.clone());
        let top = reader
            .read_top_index()
            .await
            .map_err(|error| format!("{path}: {error}"))?;
        let extents = reader
            .all_extents(&top)
            .await
            .map_err(|error| format!("{path}: {error}"))?;
        for extent in extents {
            if limit.is_some_and(|limit| extent.id_first > limit) {
                report.ignored_extent_count += 1;
                continue;
            }
            let records = reader
                .read_extent(&extent)
                .await
                .map_err(|error| format!("{path}: {error}"))?;
            let (id_last, records) = match limit {
                Some(limit) if extent.id_last > limit => match accepted_prefix(&records, limit) {
                    Some(accepted) => accepted,
                    None => {
                        report.ignored_extent_count += 1;
                        continue;
                    }
                },
                _ => (extent.id_last, records),
            };
            needs_manifest |= &*extent.stream != MANIFEST_STREAM;
            out.push(ScannedExtent {
                stream: Arc::clone(&extent.stream),
                extent: MetaExtent {
                    path: path.to_string(),
                    offset: extent.offset,
                    len_bytes: extent.len_bytes,
                    id_first: extent.id_first,
                    id_last,
                    block_count: extent.block_count,
                },
                records,
            });
        }
        objects_seen.push(ScannedObject {
            path: path.to_string(),
            len_bytes,
            needs_manifest,
        });
    }
    Ok(out)
}

/// What replay has of one stream, in any order until `finish` sorts it.
#[derive(Default)]
struct Pending {
    scanned: Vec<ScannedExtent>,
    wal: Vec<storage::ReplayedEntry>,
}

/// Puts one stream together: its saved metadata, then every record above it, in id order.
fn replay_stream(
    mut meta: StreamMeta,
    pending: Pending,
    now_ms: u64,
    report: &mut RecoveryReport,
) -> Result<Option<RestoredStream>, String> {
    let name = meta.name.clone();
    let was_deleted = meta.deleted;
    let floor_id = if was_deleted {
        meta.last_id
    } else {
        meta.floor_id
    };
    let mut new_extents = Vec::new();

    enum Item {
        Scanned(Bytes),
        Wal(storage::ReplayedEntry),
    }
    let mut items: Vec<(u64, Item)> = Vec::new();
    for scanned in pending.scanned {
        let extent = scanned.extent;
        let overlaps = meta
            .extents
            .iter()
            .any(|known| known.id_first <= extent.id_last && extent.id_first <= known.id_last);
        if extent.id_last <= floor_id || overlaps {
            continue;
        }
        report.adopted_extent_count += 1;
        items.push((extent.id_first, Item::Scanned(scanned.records)));
        new_extents.push(extent.clone());
        meta.extents.push(extent);
    }
    meta.extents.sort_by_key(|extent| extent.id_first);
    for entry in pending.wal {
        let first_id = format::RecordHeader::read(&entry.records)
            .map_err(|error| format!("a WAL entry for {name}: {error}"))?
            .id;
        items.push((first_id, Item::Wal(entry)));
    }
    items.sort_by_key(|(first_id, _)| *first_id);

    let saved_applied_id = meta.applied_id.max(floor_id);
    let mut applied_id = saved_applied_id;
    let mut state = StreamState::new();
    let mut dedupe = DedupeTable::from_entries(meta.dedupe.iter().map(|entry| {
        (
            entry.producer_id,
            entry.producer_seq,
            entry.first_id,
            entry.count,
        )
    }));
    let mut alive = !was_deleted;
    let mut batches = Vec::new();
    if alive {
        state.install(meta.state.clone(), meta.state_ver);
    }
    for (first_id, item) in items {
        let (bytes, producer) = match &item {
            Item::Scanned(bytes) => (bytes, None),
            Item::Wal(entry) => (
                &entry.records,
                Some((entry.producer_id, entry.producer_seq)),
            ),
        };
        let mut last_id = first_id;
        let mut item_count = 0u64;
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
        if last_id <= floor_id {
            continue;
        }
        if !alive {
            // Made again after the delete: what the tombstone said is the floor now.
            alive = true;
            let fresh = new_meta(&name);
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
            if producer_id != 0 && last_id > saved_applied_id {
                dedupe.record(
                    producer_id,
                    producer_seq,
                    first_id,
                    item_count as u32,
                    now_ms,
                );
            }
            let covered = meta
                .extents
                .iter()
                .any(|extent| extent.id_first <= first_id && last_id <= extent.id_last);
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
    meta.dedupe = dedupe
        .entries()
        .iter()
        .map(|entry| MetaDedupe {
            producer_id: entry.producer_id(),
            producer_seq: entry.producer_seq(),
            first_id: entry.first_id(),
            count: entry.count(),
        })
        .collect();
    Ok(Some(RestoredStream {
        meta,
        new_extents,
        batches,
    }))
}

/// Everything up to and including taking ownership. A process that loses that race has written
/// nothing but a checkpoint its own crash owed, has emptied at most a stale local store (D151),
/// and must stop.
pub(crate) async fn prepare(
    wal_dir: &FsPath,
    local_store: storage::MetadataStore,
    store: &Arc<dyn ObjectStore>,
    now_ms: u64,
) -> Result<(Prepared, Arc<LocalMeta>), String> {
    let mut report = RecoveryReport::default();
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
        &checkpoints,
        &ownership,
        &mut scanned_objects,
        &mut report,
    )
    .await?;

    let mut pending: BTreeMap<String, Pending> = BTreeMap::new();
    for extent in scanned {
        pending
            .entry(extent.stream.to_string())
            .or_default()
            .scanned
            .push(extent);
    }
    let mut closed_seqs = Vec::with_capacity(wal_files.len());
    let mut empty_files = Vec::new();
    let mut first_seq = 1;
    for file in &wal_files {
        closed_seqs.push(file.seq);
        first_seq = first_seq.max(file.seq + 1);
        let replayed = replay_file(file).map_err(|error| error.to_string())?;
        if replayed.entries.is_empty() {
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
            report.replayed_entry_count += 1;
            pending
                .entry(entry.stream.clone())
                .or_default()
                .wal
                .push(entry);
        }
    }

    let mut restored = Vec::with_capacity(pending.len());
    for (name, one) in pending {
        let (meta, _) = load_stream(&local, &checkpoints, store, &name).await?;
        let meta = meta.unwrap_or_else(|| new_meta(&name));
        if let Some(stream) = replay_stream(meta, one, now_ms, &mut report)? {
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
        },
        local,
    ))
}
