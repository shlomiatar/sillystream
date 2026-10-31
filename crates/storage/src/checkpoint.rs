//! Checkpoints (SPEC section 7.6, D18): `checkpoints/<generation>-<seq>.ckpt`, one per seal,
//! shared by every stream that changed, in the segment's paged footer format. An entry's
//! `offset, len` point at the stream's metadata, and its `id_last` is the stream's last id.
//!
//! ```text
//! file := blob* page* top_index trailer
//! ```
//!
//! An entry holds only the extents new since the stream's last entry. A *full* file holds
//! every stream's whole metadata, and nothing older is needed once one exists. A file's
//! entry under the empty name, which no stream can have, holds its head.
//!
//! A former generation can still upload one of these after losing ownership. An entry from
//! generation `g` whose last id is above `limit_for(g)` is ignored (section 8.1).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use format::crc32c;
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
const HEAD_NAME: &str = "";

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
    pub objects: Vec<String>,
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
    let mut objects = Vec::new();
    for _ in 0..count {
        let len_end = at + 4;
        let len = bytes.get(at..len_end).ok_or(BAD)?;
        let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
        let end = len_end.checked_add(len).ok_or(BAD)?;
        let text = bytes.get(len_end..end).ok_or(BAD)?;
        objects.push(std::str::from_utf8(text).map_err(|_| BAD)?.to_owned());
        at = end;
    }
    if at != bytes.len() {
        return Err(BAD);
    }
    Ok(CheckpointHead {
        full: bytes[4] == 1,
        max_id: u64::from_le_bytes(max_id),
        objects,
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
    let mut add = |name: &str, bytes: Vec<u8>, id_last: u64| {
        let len_bytes = bytes.len() as u64;
        index.push((
            stream_hash(name),
            StreamExtent {
                stream: Arc::from(name),
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
    add(HEAD_NAME, encode_head(head), 0);
    for meta in metas {
        add(&meta.name, encode_meta(meta), meta.last_id);
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
    let options = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    let payload = PutPayload::from_iter(built.parts.iter().cloned());
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
    pub last_modified_ms: i64,
}

impl CheckpointFile {
    pub fn new(generation: u64, seq: u64, top: TopIndex, head: CheckpointHead) -> CheckpointFile {
        CheckpointFile {
            generation,
            seq,
            path: checkpoint_path(generation, seq),
            top,
            head,
            last_modified_ms: 0,
        }
    }
}

async fn read_entry(
    store: &Arc<dyn ObjectStore>,
    file: &CheckpointFile,
    name: &str,
) -> Result<Option<(u64, Bytes)>, CheckpointError> {
    let Some(page) = file.top.page_for(stream_hash(name)) else {
        return Ok(None);
    };
    let start = page.offset as usize;
    let bytes = store
        .get_range(&file.path, start..start + page.len_bytes as usize)
        .await?;
    let Some((_, entry)) = decode_page(&bytes, Some(name))?.into_iter().next() else {
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
    /// Files older than the newest full one, which nothing needs (D18).
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
        let mut found: BTreeMap<(u64, u64), (Path, i64)> = BTreeMap::new();
        for object in listed.objects {
            if let Some(key) = parse_checkpoint_path(&object.location) {
                found.insert(
                    key,
                    (object.location, object.last_modified.timestamp_millis()),
                );
            }
        }
        let mut set = CheckpointSet {
            files: Vec::new(),
            ownership,
            superseded: Vec::new(),
        };
        let mut reached_full = false;
        for ((generation, seq), (path, last_modified_ms)) in found.into_iter().rev() {
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
                last_modified_ms,
            };
            if let Some((_, blob)) = read_entry(store, &file, HEAD_NAME).await? {
                file.head = decode_head(&blob)?;
            }
            // A late full file from a former generation may hold what readers ignore.
            reached_full = file.head.full && set.accepts(generation, file.head.max_id);
            set.files.push(file);
        }
        Ok(set)
    }

    /// Goes in front: it is the newest. A full one makes the rest superseded.
    pub fn add_newest(&mut self, file: CheckpointFile) {
        if file.head.full {
            let older = std::mem::take(&mut self.files);
            self.superseded
                .extend(older.into_iter().map(|file| file.path));
        }
        self.files.insert(0, file);
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
        name: &str,
    ) -> Result<Option<StreamMeta>, CheckpointError> {
        let mut found: Option<StreamMeta> = None;
        let mut extents = Vec::new();
        for file in &self.files {
            let Some((id_last, blob)) = read_entry(store, file, name).await? else {
                continue;
            };
            if !self.accepts(file.generation, id_last) {
                continue;
            }
            let meta = decode_meta(&blob)?;
            extents.extend(meta.extents.iter().cloned());
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
        Ok(Some(meta))
    }

    /// Every stream's whole metadata, for a new full file. Holds it all in memory, which is
    /// fine at the rate these happen (D18's period) and is noted in BUILD_NOTES.
    pub async fn merge_all(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<BTreeMap<String, StreamMeta>, CheckpointError> {
        let mut all: BTreeMap<String, StreamMeta> = BTreeMap::new();
        for file in self.files.iter().rev() {
            let reader = SegmentReader::new(Arc::clone(store), file.path.clone());
            for entry in reader.all_extents(&file.top).await? {
                if entry.stream.is_empty() || !self.accepts(file.generation, entry.id_last) {
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
/// above its floor. A tombstone clears them.
pub fn merge_meta(all: &mut BTreeMap<String, StreamMeta>, newer: StreamMeta) {
    let mut extents = match all.remove(&newer.name) {
        Some(older) if !newer.deleted => older.extents,
        _ => Vec::new(),
    };
    let mut newer = newer;
    extents.append(&mut newer.extents);
    extents.retain(|extent| extent.id_last > newer.floor_id);
    extents.sort_by_key(|extent| extent.id_first);
    extents.dedup_by_key(|extent| extent.id_first);
    newer.extents = extents;
    all.insert(newer.name.clone(), newer);
}

/// The wait before a retry of store work that must not be given up on.
pub fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1 << attempt.min(7)))
}
