//! Checkpoints (SPEC section 7.6, D18): `checkpoints/<generation>-<seq>.ckpt`, one per seal,
//! shared by every stream that changed, in the segment's format. Each region is a stream's key
//! hash and then its metadata, and its last id is the stream's.
//!
//! ```text
//! file := (key hash | metadata)* page* last_ids top_index trailer
//! ```
//!
//! The head, under the all-zero key hash that no stream has in practice, holds the object
//! table rows that changed, for uploaded objects only, and which rows went. An entry holds a
//! stream's metadata, from a stream whose state, config, cursors, sinks or tombstone changed.
//! Object lists are left out, and rebuilt from the last ids of the objects newer than the
//! newest full file. A *full* file holds every row and every stream's whole metadata, lists
//! included, and nothing older is needed once a reader can trust one.
//!
//! A former generation can still upload one of these after losing ownership. An entry from
//! generation `g` whose last id is above `limit_for(g)` is ignored (section 8.1).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use format::{KeyHash, crc32c};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

use crate::meta_format::{MetaError, StreamMeta, decode_meta, encode_meta};
use crate::object::ObjectId;
use crate::object_table::{ObjectKey, ObjectRow, ROW_KEY_BYTES, decode_rows, encode_rows};
use crate::ownership::Ownership;
use crate::segment_builder::encode_index;
use crate::segment_format::{DEFAULT_PAGE_BYTES, SegmentError, TopIndex};
use crate::segment_reader::SegmentReader;

const CHECKPOINT_PREFIX: &str = "checkpoints";
const HEAD_KEY: KeyHash = KeyHash([0; KeyHash::BYTES]);

pub fn checkpoint_path(generation: u64, seq: u64) -> Path {
    Path::from(format!(
        "{CHECKPOINT_PREFIX}/{generation:020}-{seq:020}.ckpt"
    ))
}

fn parse_checkpoint_path(path: &Path) -> Option<(u64, u64)> {
    let name = path.filename()?.strip_suffix(".ckpt")?;
    let (generation, seq) = name.split_once('-')?;
    Some((generation.parse().ok()?, seq.parse().ok()?))
}

/// What a file says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CheckpointHead {
    pub full: bool,
    /// The highest stream last id it knows of, including through earlier files.
    pub max_id: u64,
    /// Object table rows that changed, or every row in a full file (section 7.6).
    pub rows: Vec<ObjectRow>,
    /// Rows gone with their objects since the file before. A full file has none.
    pub removed: Vec<ObjectKey>,
    /// Every stream with a sink as of this file, so a startup can start them without loading
    /// every stream (D198).
    pub sink_streams: Vec<KeyHash>,
}

fn encode_head(head: &CheckpointHead) -> Vec<u8> {
    let mut out = vec![0; 4];
    out.push(u8::from(head.full));
    out.extend_from_slice(&[0; 3]);
    out.extend_from_slice(&head.max_id.to_le_bytes());
    encode_rows(&mut out, &head.rows);
    out.extend_from_slice(&(head.removed.len() as u32).to_le_bytes());
    for key in &head.removed {
        out.extend_from_slice(&key.encode());
    }
    out.extend_from_slice(&(head.sink_streams.len() as u32).to_le_bytes());
    for stream in &head.sink_streams {
        out.extend_from_slice(&stream.0);
    }
    let crc = crc32c(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_head(bytes: &[u8]) -> Result<CheckpointHead, MetaError> {
    const BAD: MetaError = MetaError("a checkpoint's head is corrupt");
    if bytes.len() < 16
        || u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) != crc32c(&bytes[4..])
        || bytes[4] > 1
    {
        return Err(BAD);
    }
    let max_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let (rows, taken) = decode_rows(&bytes[16..])?;
    let mut at = 16 + taken;
    let count = |at: usize| -> Result<usize, MetaError> {
        let field = bytes.get(at..at + 4).ok_or(BAD)?;
        Ok(u32::from_le_bytes(field.try_into().unwrap()) as usize)
    };
    let removed_count = count(at)?;
    at += 4;
    let mut removed = Vec::with_capacity(removed_count.min(bytes.len() / ROW_KEY_BYTES));
    for _ in 0..removed_count {
        let key = bytes.get(at..at + ROW_KEY_BYTES).ok_or(BAD)?;
        removed.push(ObjectKey::decode(key).ok_or(BAD)?);
        at += ROW_KEY_BYTES;
    }
    let sink_count = count(at)?;
    at += 4;
    if sink_count.checked_mul(KeyHash::BYTES) != bytes.len().checked_sub(at) {
        return Err(BAD);
    }
    let sink_streams = bytes[at..]
        .as_chunks::<{ KeyHash::BYTES }>()
        .0
        .iter()
        .map(|key| KeyHash(*key))
        .collect();
    Ok(CheckpointHead {
        full: bytes[4] == 1,
        max_id,
        rows,
        removed,
        sink_streams,
    })
}

/// The pieces of one PUT.
pub struct BuiltCheckpoint {
    pub parts: Vec<Bytes>,
    pub top: TopIndex,
}

/// One file from `head` and the metadata of each changed stream.
pub fn build_checkpoint(
    head: &CheckpointHead,
    metas: &[StreamMeta],
) -> Result<BuiltCheckpoint, SegmentError> {
    let mut sorted: Vec<&StreamMeta> = metas.iter().collect();
    sorted.sort_by_key(|meta| meta.stream);
    let mut parts = Vec::with_capacity(2 * metas.len() + 5);
    let mut regions = Vec::with_capacity(metas.len() + 1);
    let mut add = |stream: KeyHash, bytes: Vec<u8>, id_last: u64| {
        regions.push((stream, (KeyHash::BYTES + bytes.len()) as u64, id_last));
        parts.push(Bytes::copy_from_slice(&stream.0));
        parts.push(Bytes::from(bytes));
    };
    add(HEAD_KEY, encode_head(head), 0);
    for meta in sorted {
        add(meta.stream, encode_meta(meta), meta.last_id);
    }
    let (_, top) = encode_index(&regions, DEFAULT_PAGE_BYTES, &mut parts)?;
    Ok(BuiltCheckpoint { parts, top })
}

#[derive(Debug)]
pub enum CheckpointError {
    Segment(SegmentError),
    Meta(MetaError),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::Segment(error) => write!(f, "checkpoint: {error}"),
            CheckpointError::Meta(error) => write!(f, "checkpoint: {error}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl From<SegmentError> for CheckpointError {
    fn from(error: SegmentError) -> Self {
        CheckpointError::Segment(error)
    }
}

impl From<MetaError> for CheckpointError {
    fn from(error: MetaError) -> Self {
        CheckpointError::Meta(error)
    }
}

impl From<object_store::Error> for CheckpointError {
    fn from(error: object_store::Error) -> Self {
        CheckpointError::Segment(SegmentError::Store(error))
    }
}

/// One conditional create, which is one PUT. Finding the path taken means an earlier attempt
/// succeeded: a generation's seqs are its own.
pub async fn put_checkpoint(
    store: &dyn ObjectStore,
    path: &Path,
    built: &BuiltCheckpoint,
) -> object_store::Result<()> {
    put_checkpoint_payload(
        store,
        path,
        PutPayload::from_iter(built.parts.iter().cloned()),
    )
    .await
}

/// The same for a file's bytes, kept while its PUT was still owed.
pub async fn put_checkpoint_payload(
    store: &dyn ObjectStore,
    path: &Path,
    payload: PutPayload,
) -> object_store::Result<()> {
    let options = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store.put_opts(path, payload, options).await {
        Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

/// A file and its top index, cached (section 7.3).
#[derive(Debug, Clone)]
pub struct CheckpointFile {
    pub generation: u64,
    pub seq: u64,
    pub path: Path,
    pub top: TopIndex,
    pub head: CheckpointHead,
}

impl CheckpointFile {
    pub fn new(generation: u64, seq: u64, top: TopIndex, head: CheckpointHead) -> CheckpointFile {
        CheckpointFile {
            generation,
            seq,
            path: checkpoint_path(generation, seq),
            top,
            head,
        }
    }
}

/// A stream's metadata in `file`: a page read, then the region's.
async fn read_entry(
    store: &Arc<dyn ObjectStore>,
    file: &CheckpointFile,
    stream: KeyHash,
) -> Result<Option<Bytes>, CheckpointError> {
    let reader = SegmentReader::new(Arc::clone(store), file.path.clone());
    for entry in reader.candidates(&file.top, stream).await? {
        let start = entry.offset as usize;
        let region = store
            .get_range(&file.path, start..start + entry.len_bytes as usize)
            .await?;
        let key = check_region_key(&region)?;
        if key == stream {
            return Ok(Some(region.slice(KeyHash::BYTES..)));
        }
    }
    Ok(None)
}

/// A region's key, checked against what its CRC covers: the head's own, or the metadata's
/// stream. Nothing else covers the key.
fn check_region_key(region: &[u8]) -> Result<KeyHash, CheckpointError> {
    let key = KeyHash::from_slice(region.get(..KeyHash::BYTES).unwrap_or_default())
        .ok_or(SegmentError::Corrupt("region is truncated"))?;
    let body = &region[KeyHash::BYTES..];
    let matches = if key == HEAD_KEY {
        decode_head(body).is_ok()
    } else {
        decode_meta(body)?.stream == key
    };
    if !matches {
        return Err(SegmentError::Corrupt("region's key isn't its metadata's").into());
    }
    Ok(key)
}

/// The checkpoints that matter, newest first: down to the newest full file a reader can trust.
#[derive(Debug, Clone, Default)]
pub struct CheckpointSet {
    pub files: Vec<CheckpointFile>,
    pub ownership: Ownership,
    /// Files older than the newest full one a reader can trust, which nothing needs (D18). Only
    /// a startup that has just claimed ownership takes these, since it alone knows the limits
    /// that make a full file trustworthy for every later reader.
    pub superseded: Vec<Path>,
}

impl CheckpointSet {
    /// Whether an entry with this last id is one readers accept.
    fn accepts(&self, generation: u64, id_last: u64) -> bool {
        self.ownership
            .limit_for(generation)
            .is_none_or(|limit| id_last <= limit)
    }

    /// Lists and reads every file's top index and head.
    pub async fn read(
        store: &Arc<dyn ObjectStore>,
        ownership: Ownership,
    ) -> Result<CheckpointSet, CheckpointError> {
        let listed = store
            .list_with_delimiter(Some(&Path::from(CHECKPOINT_PREFIX)))
            .await?;
        let mut found: BTreeMap<(u64, u64), Path> = BTreeMap::new();
        for object in listed.objects {
            if let Some(key) = parse_checkpoint_path(&object.location) {
                found.insert(key, object.location);
            }
        }
        let mut set = CheckpointSet {
            files: Vec::new(),
            ownership,
            superseded: Vec::new(),
        };
        let mut reached_full = false;
        for ((generation, seq), path) in found.into_iter().rev() {
            if reached_full {
                set.superseded.push(path);
                continue;
            }
            let top = SegmentReader::new(Arc::clone(store), path.clone())
                .read_top_index()
                .await?;
            let mut file = CheckpointFile {
                generation,
                seq,
                path,
                top,
                head: CheckpointHead::default(),
            };
            if let Some(blob) = read_entry(store, &file, HEAD_KEY).await? {
                file.head = decode_head(&blob)?;
            }
            // A late full file from a former generation may hold what readers ignore.
            reached_full = file.head.full && set.accepts(generation, file.head.max_id);
            set.files.push(file);
        }
        Ok(set)
    }

    /// The newest list of streams with sinks that a reader can trust (section 8.1).
    pub fn sink_streams(&self) -> Vec<KeyHash> {
        self.files
            .iter()
            .find(|file| self.accepts(file.generation, file.head.max_id))
            .map_or_else(Vec::new, |file| file.head.sink_streams.clone())
    }

    /// Goes in front: it is the newest. A running node doesn't need what a full one replaces,
    /// and leaves the files themselves: it may no longer own the bucket, and if so a reader may
    /// refuse that full file and need them (section 8.1).
    pub fn add_newest(&mut self, file: CheckpointFile) {
        if file.head.full {
            self.files.clear();
        }
        self.files.insert(0, file);
    }

    /// The rows these files hold, oldest first, with the removals applied: what the node's
    /// table starts from. A file with anything over its generation's limit doesn't count, since
    /// part of what it records is refused (section 8.1).
    pub fn rows(&self) -> BTreeMap<ObjectKey, ObjectRow> {
        let mut rows = BTreeMap::new();
        for file in self.files.iter().rev() {
            if !self.accepts(file.generation, file.head.max_id) {
                continue;
            }
            for key in &file.head.removed {
                rows.remove(key);
            }
            for row in &file.head.rows {
                rows.insert(row.key, row.clone());
            }
        }
        rows
    }

    pub fn max_id(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| self.accepts(file.generation, file.head.max_id))
            .map(|file| file.head.max_id)
            .max()
            .unwrap_or(0)
    }

    /// The full file lists come from, if the oldest file is one that counts. A fenced-off
    /// generation's file counts for nothing, here as in `rows`.
    fn full_file(&self) -> Option<&CheckpointFile> {
        self.files
            .last()
            .filter(|file| file.head.full && self.accepts(file.generation, file.head.max_id))
    }

    /// Sealed objects these files record that the full file doesn't, whose last ids lists are
    /// rebuilt from. A startup reads their last ids once (section 7.6).
    pub fn newer_objects(&self) -> Vec<ObjectId> {
        let in_full: HashSet<ObjectKey> = self
            .full_file()
            .map(|file| file.head.rows.iter().map(|row| row.key).collect())
            .unwrap_or_default();
        let mut ids: Vec<ObjectId> = self
            .rows()
            .into_values()
            .filter(|row| row.key.class.is_none() && !in_full.contains(&row.key))
            .map(|row| row.key.id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// A stream's metadata as the files have it: the newest entry a reader accepts, with the
    /// list and last id of the full file's entry, then every newer object holding the stream
    /// (section 7.6). `newer` is each such object's last ids.
    pub async fn lookup(
        &self,
        store: &Arc<dyn ObjectStore>,
        stream: KeyHash,
        newer: &NewerObjects,
    ) -> Result<Option<StreamMeta>, CheckpointError> {
        let mut newest = None;
        for file in &self.files {
            let Some(blob) = read_entry(store, file, stream).await? else {
                continue;
            };
            let meta = decode_meta(&blob)?;
            if self.accepts(file.generation, meta.last_id) {
                newest = Some((file.seq, file.generation, meta));
                break;
            }
        }
        let full = match self.full_file() {
            Some(file)
                if newest.as_ref().is_some_and(|(seq, generation, _)| {
                    (*generation, *seq) != (file.generation, file.seq)
                }) =>
            {
                match read_entry(store, file, stream).await? {
                    Some(blob) => Some(decode_meta(&blob)?),
                    None => None,
                }
            }
            _ => None,
        };
        let from_newer = newer.of(stream, self);
        let meta = match newest {
            Some((_, _, meta)) => meta,
            // A stream that only ever appended records since the full file has no entry.
            None if !from_newer.is_empty() => StreamMeta {
                stream,
                ..StreamMeta::default()
            },
            None => return Ok(None),
        };
        Ok(Some(rebuild(meta, full, &from_newer)))
    }

    /// Every stream's whole metadata, for a new full file. Holds it all in memory, which is
    /// fine at the rate these happen (D18's period) and is noted in BUILD_NOTES.
    pub async fn merge_all(
        &self,
        store: &Arc<dyn ObjectStore>,
        newer: &NewerObjects,
    ) -> Result<BTreeMap<KeyHash, StreamMeta>, CheckpointError> {
        let mut full: BTreeMap<KeyHash, StreamMeta> = BTreeMap::new();
        let mut newest: BTreeMap<KeyHash, StreamMeta> = BTreeMap::new();
        for file in self.files.iter().rev() {
            let into = if self
                .full_file()
                .is_some_and(|full| full.seq == file.seq && full.generation == file.generation)
            {
                &mut full
            } else {
                &mut newest
            };
            let reader = SegmentReader::new(Arc::clone(store), file.path.clone());
            let (entries, data, last_ids) = reader.read_all(&file.top).await?;
            for (entry, (stream, last_id)) in entries.iter().zip(last_ids) {
                let start = entry.offset as usize;
                let region = data
                    .get(start..start + entry.len_bytes as usize)
                    .ok_or(SegmentError::Corrupt("region is past the data"))?;
                if check_region_key(region)? != stream {
                    return Err(SegmentError::Corrupt("region isn't its last id's").into());
                }
                if stream == HEAD_KEY {
                    continue;
                }
                let meta = decode_meta(&region[KeyHash::BYTES..])?;
                if meta.last_id != last_id {
                    return Err(SegmentError::Corrupt("metadata isn't its last id").into());
                }
                if self.accepts(file.generation, meta.last_id) {
                    into.insert(stream, meta);
                }
            }
        }
        let mut all = BTreeMap::new();
        let mut streams: HashSet<KeyHash> = full.keys().chain(newest.keys()).copied().collect();
        let by_stream = newer.by_stream(self);
        streams.extend(by_stream.keys().copied());
        for stream in streams {
            let from_newer = by_stream.get(&stream).cloned().unwrap_or_default();
            let full_meta = full.remove(&stream);
            let meta = match newest.remove(&stream) {
                Some(meta) => meta,
                None => match &full_meta {
                    Some(meta) => meta.clone(),
                    None => StreamMeta {
                        stream,
                        ..StreamMeta::default()
                    },
                },
            };
            all.insert(stream, rebuild(meta, full_meta, &from_newer));
        }
        Ok(all)
    }
}

/// `meta`'s fields with its list and last id: those of `full`'s entry unless a tombstone came
/// between, then the newer objects holding the stream above its floor.
fn rebuild(
    mut meta: StreamMeta,
    full: Option<StreamMeta>,
    newer: &[(ObjectId, u64)],
) -> StreamMeta {
    if meta.deleted {
        meta.objects.clear();
        return meta;
    }
    if let Some(full) = full
        && !full.deleted
        && full.floor_id == meta.floor_id
    {
        meta.objects = full.objects;
        meta.last_id = meta.last_id.max(full.last_id);
    }
    for (id, last_id) in newer {
        if *last_id > meta.floor_id {
            meta.objects.push(*id);
            meta.last_id = meta.last_id.max(*last_id);
        }
    }
    meta.objects.sort_unstable();
    meta.objects.dedup();
    meta
}

/// The last ids of the sealed objects newer than the full file, read once at startup.
#[derive(Debug, Clone, Default)]
pub struct NewerObjects {
    objects: BTreeMap<ObjectId, Arc<Vec<(KeyHash, u64)>>>,
}

impl NewerObjects {
    /// Reads each object's last ids.
    pub async fn read(
        store: &Arc<dyn ObjectStore>,
        ids: &[ObjectId],
    ) -> Result<NewerObjects, CheckpointError> {
        let mut newer = NewerObjects::default();
        for id in ids {
            let reader = SegmentReader::new(Arc::clone(store), ObjectKey::sealed(*id).path());
            let top = match reader.read_top_index().await {
                Ok(top) => top,
                // Expired since: it holds nothing a list needs.
                Err(SegmentError::Store(object_store::Error::NotFound { .. })) => continue,
                Err(error) => return Err(error.into()),
            };
            newer.add(*id, reader.read_last_ids(&top).await?);
        }
        Ok(newer)
    }

    pub fn add(&mut self, id: ObjectId, last_ids: Vec<(KeyHash, u64)>) {
        self.objects.insert(id, Arc::new(last_ids));
    }

    /// After a full file, which holds them all.
    pub fn clear(&mut self) {
        self.objects.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// The objects holding `stream`, with its last id in each, capped at what the object's
    /// generation may have (section 8.1).
    pub fn of(&self, stream: KeyHash, set: &CheckpointSet) -> Vec<(ObjectId, u64)> {
        let mut found = Vec::new();
        for (id, last_ids) in &self.objects {
            if let Ok(at) = last_ids.binary_search_by_key(&stream, |(key, _)| *key) {
                found.push((*id, set.capped(*id, last_ids[at].1)));
            }
        }
        found
    }

    fn by_stream(&self, set: &CheckpointSet) -> BTreeMap<KeyHash, Vec<(ObjectId, u64)>> {
        let mut by_stream: BTreeMap<KeyHash, Vec<(ObjectId, u64)>> = BTreeMap::new();
        for (id, last_ids) in &self.objects {
            for (stream, last_id) in last_ids.iter() {
                by_stream
                    .entry(*stream)
                    .or_default()
                    .push((*id, set.capped(*id, *last_id)));
            }
        }
        by_stream
    }
}

impl CheckpointSet {
    fn capped(&self, id: ObjectId, last_id: u64) -> u64 {
        self.ownership
            .limit_for(id.generation())
            .map_or(last_id, |limit| last_id.min(limit))
    }
}

/// The wait before a retry of store work that must not be given up on.
pub fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1 << attempt.min(7)))
}
