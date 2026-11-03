//! Checkpoints (SPEC section 7.6, D18): `checkpoints/<generation>-<seq>.ckpt`, one per seal,
//! shared by every stream that changed, in the segment's paged footer format. An entry's
//! `offset, len` point at the stream's metadata, and its `id_last` is the stream's last id.
//!
//! ```text
//! file := blob* page* top_index trailer
//! ```
//!
//! An entry holds only the extents new since the stream's last entry. A *full* file holds
//! every stream's whole metadata and every object recorded so far, and nothing older is needed
//! once a reader can trust one. A file's entry under the all-zero key hash, which no stream has
//! in practice, holds its head.
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
use crate::ownership::Ownership;
use crate::segment_builder::encode_index;
use crate::segment_format::{
    DEFAULT_PAGE_BYTES, SegmentError, StreamExtent, TopIndex, decode_page, stream_hash,
};
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
    /// Segment objects whose extents are recorded by now, so a startup needn't look in them.
    /// A full file's list includes those of the files it replaces.
    pub objects: Vec<String>,
    /// Every stream with a sink as of this file, so a startup can start them without loading
    /// every stream (D198).
    pub sink_streams: Vec<KeyHash>,
}

fn encode_head(head: &CheckpointHead) -> Vec<u8> {
    let mut out = vec![0; 4];
    out.push(u8::from(head.full));
    out.extend_from_slice(&[0; 3]);
    out.extend_from_slice(&head.max_id.to_le_bytes());
    out.extend_from_slice(&(head.objects.len() as u32).to_le_bytes());
    for object in &head.objects {
        out.extend_from_slice(&(object.len() as u32).to_le_bytes());
        out.extend_from_slice(object.as_bytes());
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
    if bytes.len() < 20
        || u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) != crc32c(&bytes[4..])
    {
        return Err(BAD);
    }
    let mut max_id = [0u8; 8];
    max_id.copy_from_slice(&bytes[8..16]);
    let count = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]) as usize;
    let mut at = 20;
    let read_list = |count: usize, at: &mut usize| -> Result<Vec<String>, MetaError> {
        let mut list = Vec::new();
        for _ in 0..count {
            let len_end = *at + 4;
            let len = bytes.get(*at..len_end).ok_or(BAD)?;
            let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
            let end = len_end.checked_add(len).ok_or(BAD)?;
            let text = bytes.get(len_end..end).ok_or(BAD)?;
            list.push(std::str::from_utf8(text).map_err(|_| BAD)?.to_owned());
            *at = end;
        }
        Ok(list)
    };
    let objects = read_list(count, &mut at)?;
    let sink_count = bytes.get(at..at + 4).ok_or(BAD)?;
    let sink_count =
        u32::from_le_bytes([sink_count[0], sink_count[1], sink_count[2], sink_count[3]]) as usize;
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
        max_id: u64::from_le_bytes(max_id),
        objects,
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
    let mut parts = Vec::with_capacity(metas.len() + 3);
    let mut offset = 0u64;
    let mut index: Vec<(u64, StreamExtent)> = Vec::with_capacity(metas.len() + 1);
    let mut add = |stream: KeyHash, bytes: Vec<u8>, id_last: u64| {
        let len_bytes = bytes.len() as u64;
        index.push((
            stream_hash(stream),
            StreamExtent {
                stream,
                offset,
                len_bytes,
                id_first: 0,
                id_last,
                block_count: 0,
            },
        ));
        offset += len_bytes;
        parts.push(Bytes::from(bytes));
    };
    add(HEAD_KEY, encode_head(head), 0);
    for meta in metas {
        add(meta.stream, encode_meta(meta), meta.last_id);
    }
    let top = encode_index(&mut index, DEFAULT_PAGE_BYTES, offset, &mut parts)?;
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

async fn read_entry(
    store: &Arc<dyn ObjectStore>,
    file: &CheckpointFile,
    stream: KeyHash,
) -> Result<Option<(u64, Bytes)>, CheckpointError> {
    let Some(page) = file.top.page_for(stream_hash(stream)) else {
        return Ok(None);
    };
    let start = page.offset as usize;
    let bytes = store
        .get_range(&file.path, start..start + page.len_bytes as usize)
        .await?;
    let Some((_, entry)) = decode_page(&bytes, Some(stream))?.into_iter().next() else {
        return Ok(None);
    };
    let start = entry.offset as usize;
    let blob = store
        .get_range(&file.path, start..start + entry.len_bytes as usize)
        .await?;
    Ok(Some((entry.id_last, blob)))
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
            if let Some((_, blob)) = read_entry(store, &file, HEAD_KEY).await? {
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

    /// The objects whose extents these files record. A file with anything over its generation's
    /// limit doesn't count: part of what it records is refused, and its objects have to be
    /// looked at for what readers accept of them (section 8.1).
    pub fn recorded_objects(&self) -> impl Iterator<Item = &str> {
        self.files
            .iter()
            .filter(|file| self.accepts(file.generation, file.head.max_id))
            .flat_map(|file| file.head.objects.iter().map(String::as_str))
    }

    pub fn max_id(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| self.accepts(file.generation, file.head.max_id))
            .map(|file| file.head.max_id)
            .max()
            .unwrap_or(0)
    }

    /// A stream's metadata with all its extents, walking newest to oldest (D18).
    pub async fn lookup(
        &self,
        store: &Arc<dyn ObjectStore>,
        stream: KeyHash,
    ) -> Result<Option<StreamMeta>, CheckpointError> {
        let mut found: Option<StreamMeta> = None;
        let mut extents = Vec::new();
        // Taken away by a newer entry: compaction or retention (section 7.5, 7.4).
        let mut dropped: HashSet<u64> = HashSet::new();
        for file in &self.files {
            let Some((id_last, blob)) = read_entry(store, file, stream).await? else {
                continue;
            };
            if !self.accepts(file.generation, id_last) {
                continue;
            }
            let meta = decode_meta(&blob)?;
            extents.extend(
                meta.extents
                    .iter()
                    .filter(|extent| !dropped.contains(&extent.id_first))
                    .cloned(),
            );
            dropped.extend(meta.dropped_extents.iter().copied());
            let stop = meta.deleted;
            if found.is_none() {
                found = Some(meta);
            }
            if stop {
                break;
            }
        }
        let Some(mut meta) = found else {
            return Ok(None);
        };
        extents.retain(|extent| extent.id_last > meta.floor_id);
        extents.sort_by_key(|extent| extent.id_first);
        extents.dedup_by_key(|extent| extent.id_first);
        meta.extents = if meta.deleted { Vec::new() } else { extents };
        meta.dropped_extents.clear();
        Ok(Some(meta))
    }

    /// Every stream's whole metadata, for a new full file. Holds it all in memory, which is
    /// fine at the rate these happen (D18's period) and is noted in BUILD_NOTES.
    pub async fn merge_all(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<BTreeMap<KeyHash, StreamMeta>, CheckpointError> {
        let mut all: BTreeMap<KeyHash, StreamMeta> = BTreeMap::new();
        for file in self.files.iter().rev() {
            let reader = SegmentReader::new(Arc::clone(store), file.path.clone());
            for entry in reader.all_extents(&file.top).await? {
                if entry.stream == HEAD_KEY || !self.accepts(file.generation, entry.id_last) {
                    continue;
                }
                let start = entry.offset as usize;
                let blob = store
                    .get_range(&file.path, start..start + entry.len_bytes as usize)
                    .await?;
                merge_meta(&mut all, decode_meta(&blob)?);
            }
        }
        Ok(all)
    }
}

/// `newer` on top of what `all` has for its stream: its fields, and the union of extents
/// above its floor, less what it drops. A tombstone clears them.
pub fn merge_meta(all: &mut BTreeMap<KeyHash, StreamMeta>, newer: StreamMeta) {
    let mut extents = match all.remove(&newer.stream) {
        Some(older) if !newer.deleted => older.extents,
        _ => Vec::new(),
    };
    let mut newer = newer;
    extents.retain(|extent| !newer.dropped_extents.contains(&extent.id_first));
    newer.dropped_extents.clear();
    extents.append(&mut newer.extents);
    extents.retain(|extent| extent.id_last > newer.floor_id);
    extents.sort_by_key(|extent| extent.id_first);
    extents.dedup_by_key(|extent| extent.id_first);
    newer.extents = extents;
    all.insert(newer.stream, newer);
}

/// The wait before a retry of store work that must not be given up on.
pub fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1 << attempt.min(7)))
}
