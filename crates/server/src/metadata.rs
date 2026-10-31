//! Stream metadata outside the cores (SPEC section 7.6): the local store, the loader thread
//! that brings a stream back, and the thread that saves what changed at each seal. Cores reach
//! both with messages and never wait for them, which keeps the store off the write path.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use object_store::{ObjectStore, PutPayload};
use storage::{
    CheckpointError, CheckpointFile, CheckpointHead, CheckpointSet, MetadataError, MetadataStore,
    StreamMeta, build_checkpoint, checkpoint_path, decode_extent, decode_meta, encode_extent,
    encode_meta, merge_meta, put_checkpoint, put_checkpoint_payload, retry_delay,
};
use tokio::sync::mpsc;
use xxhash_rust::xxh3::xxh3_128;

use crate::message::ControlMsg;

const TAG_STREAM: u8 = b's';
const TAG_EXTENT: u8 = b'x';
const KEY_COMPLETE: &[u8] = b"c";
const KEY_MAX_ID: &[u8] = b"g";
/// The file of the seal saved last: `generation u64 | seq u64 | file`. It goes in with that
/// seal's local write, since a crash before its PUT would leave the store ahead of S3 with
/// nothing to say what S3 lacks.
const KEY_OWED: &[u8] = b"p";
/// A stream id can be as long as a whole key (D57), so the longest ones are shortened. The
/// value holds the whole name, which a read checks (D135).
const KEY_NAME_BYTES: usize = 65_000;

fn stream_key(tag: u8, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(32 + name.len().min(KEY_NAME_BYTES));
    key.push(tag);
    key.extend_from_slice(&(name.len() as u16).to_be_bytes());
    if name.len() <= KEY_NAME_BYTES {
        key.extend_from_slice(name.as_bytes());
    } else {
        key.extend_from_slice(&name.as_bytes()[..KEY_NAME_BYTES]);
        key.extend_from_slice(&xxh3_128(name.as_bytes()).to_be_bytes());
    }
    key
}

fn extent_key(name: &str, id_first: u64) -> Vec<u8> {
    let mut key = stream_key(TAG_EXTENT, name);
    key.extend_from_slice(&id_first.to_be_bytes());
    key
}

#[derive(Debug)]
pub(crate) enum LocalError {
    Store(MetadataError),
    Corrupt(String),
}

impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalError::Store(error) => error.fmt(f),
            LocalError::Corrupt(text) => write!(f, "local metadata: {text}"),
        }
    }
}

impl From<MetadataError> for LocalError {
    fn from(error: MetadataError) -> Self {
        LocalError::Store(error)
    }
}

/// Finishes the PUT a crash may have left owed, before anything reads the checkpoints. If it did
/// succeed, or a full file took the path, this changes nothing: the path is only ever created.
pub(crate) async fn put_owed_checkpoint(
    local: &MetadataStore,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), String> {
    let Some(value) = local.get(KEY_OWED).map_err(|error| error.to_string())? else {
        return Ok(());
    };
    let (Some(generation), Some(seq), Some(file)) =
        (value.get(..8), value.get(8..16), value.get(16..))
    else {
        return Err("local metadata: the owed checkpoint is truncated".to_owned());
    };
    let number = |bytes: &[u8]| {
        let mut field = [0u8; 8];
        field.copy_from_slice(bytes);
        u64::from_le_bytes(field)
    };
    let path = checkpoint_path(number(generation), number(seq));
    let payload = PutPayload::from(Bytes::copy_from_slice(file));
    put_checkpoint_payload(store.as_ref(), &path, payload)
        .await
        .map_err(|error| error.to_string())?;
    local
        .apply([(KEY_OWED, None)])
        .map_err(|error| error.to_string())
}

/// The local key-value store, with a stream's fields under one key and each extent under its
/// own, so a seal writes what changed and not everything the stream has.
pub(crate) struct LocalMeta {
    store: MetadataStore,
    /// Whether this store has seen every stream, so a miss means no such stream. After the
    /// store is lost it hasn't, and a miss has to ask the checkpoints.
    pub complete: bool,
}

impl LocalMeta {
    /// `no_checkpoints`: nothing exists that this store could be missing.
    pub(crate) fn open(
        store: MetadataStore,
        no_checkpoints: bool,
    ) -> Result<LocalMeta, MetadataError> {
        let mut complete = store.get(KEY_COMPLETE)?.is_some();
        if !complete && no_checkpoints {
            store.apply([(KEY_COMPLETE, Some(&b"1"[..]))])?;
            complete = true;
        }
        Ok(LocalMeta { store, complete })
    }

    pub(crate) fn get(&self, name: &str) -> Result<Option<StreamMeta>, LocalError> {
        let Some(bytes) = self.store.get(&stream_key(TAG_STREAM, name))? else {
            return Ok(None);
        };
        let mut meta =
            decode_meta(&bytes).map_err(|error| LocalError::Corrupt(error.to_string()))?;
        if meta.name != name {
            return Err(LocalError::Corrupt(format!(
                "two stream ids share a key (D135): {} bytes long",
                name.len()
            )));
        }
        for (_, value) in self.store.scan_prefix(&stream_key(TAG_EXTENT, name))? {
            let extent =
                decode_extent(&value).map_err(|error| LocalError::Corrupt(error.to_string()))?;
            if extent.id_last > meta.floor_id {
                meta.extents.push(extent);
            }
        }
        meta.extents.sort_by_key(|extent| extent.id_first);
        Ok(Some(meta))
    }

    pub(crate) fn max_id(&self) -> Result<u64, MetadataError> {
        Ok(self.store.get(KEY_MAX_ID)?.map_or(0, |bytes| {
            let mut field = [0u8; 8];
            if bytes.len() == 8 {
                field.copy_from_slice(&bytes);
            }
            u64::from_le_bytes(field)
        }))
    }

    /// One atomic write of everything in `metas`, and of the file that owes S3 the same. A
    /// meta's extents are added to what the store has, and a tombstone clears them.
    pub(crate) fn apply(
        &self,
        metas: &[StreamMeta],
        max_id: u64,
        owed: Option<&[u8]>,
    ) -> Result<(), MetadataError> {
        let mut changes: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(metas.len() + 1);
        for meta in metas {
            if meta.deleted {
                for (key, _) in self
                    .store
                    .scan_prefix(&stream_key(TAG_EXTENT, &meta.name))?
                {
                    changes.push((key, None));
                }
            }
            for extent in &meta.extents {
                let mut value = Vec::with_capacity(64 + extent.path.len());
                encode_extent(&mut value, extent);
                changes.push((extent_key(&meta.name, extent.id_first), Some(value)));
            }
            let fields = StreamMeta {
                extents: Vec::new(),
                ..meta.clone()
            };
            changes.push((
                stream_key(TAG_STREAM, &meta.name),
                Some(encode_meta(&fields)),
            ));
        }
        changes.push((KEY_MAX_ID.to_vec(), Some(max_id.to_le_bytes().to_vec())));
        if let Some(owed) = owed {
            changes.push((KEY_OWED.to_vec(), Some(owed.to_vec())));
        }
        self.store.apply(
            changes
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_deref())),
        )
    }
}

/// What the loader does.
pub(crate) enum MetaMsg {
    Load {
        core: usize,
        stream: Arc<str>,
        ticket: u64,
    },
    /// A new checkpoint file, for lookups to walk.
    Installed(Box<CheckpointFile>),
}

/// Brings a stream back: the local store, then the checkpoints (section 7.6). A failed read is
/// retried, not reported as "no such stream", since that would make a new stream over an old
/// one's name.
pub(crate) fn run_loader(
    mut rx: mpsc::UnboundedReceiver<MetaMsg>,
    local: Arc<LocalMeta>,
    store: Arc<dyn ObjectStore>,
    mut checkpoints: CheckpointSet,
    control: Vec<mpsc::UnboundedSender<ControlMsg>>,
    stop: Arc<AtomicBool>,
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    runtime.block_on(async {
        // Beside the loads, which needn't wait for however many a long run left behind.
        tokio::spawn(delete_superseded(
            Arc::clone(&store),
            std::mem::take(&mut checkpoints.superseded),
        ));
        while let Some(message) = rx.recv().await {
            match message {
                MetaMsg::Installed(file) => {
                    checkpoints.add_newest(*file);
                }
                MetaMsg::Load {
                    core,
                    stream,
                    ticket,
                } => {
                    let mut attempt = 0;
                    let found = loop {
                        match load_stream(&local, &checkpoints, &store, &stream).await {
                            Ok(found) => break Some(found),
                            Err(error) if stop.load(Ordering::SeqCst) => {
                                eprintln!("loading stream metadata: {error}");
                                break None;
                            }
                            Err(error) => {
                                eprintln!("loading stream metadata, will retry: {error}");
                                tokio::time::sleep(retry_delay(attempt)).await;
                                attempt += 1;
                            }
                        }
                    };
                    let Some((meta, rebuilt)) = found else {
                        return;
                    };
                    let _ = control[core].send(ControlMsg::Loaded {
                        stream,
                        ticket,
                        meta: meta.map(Box::new),
                        rebuilt,
                    });
                }
            }
        }
    });
}

async fn delete_superseded(store: Arc<dyn ObjectStore>, superseded: Vec<object_store::path::Path>) {
    for path in superseded {
        // One left behind costs nothing: a startup passes over it and tries again.
        let _ = store.delete(&path).await;
    }
}

/// `(metadata, whether it came from a checkpoint)`.
pub(crate) async fn load_stream(
    local: &LocalMeta,
    checkpoints: &CheckpointSet,
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> Result<(Option<StreamMeta>, bool), String> {
    if let Some(meta) = local.get(name).map_err(|error| error.to_string())? {
        return Ok((Some(meta), false));
    }
    if local.complete {
        return Ok((None, false));
    }
    let found = checkpoints
        .lookup(store, name)
        .await
        .map_err(|error: CheckpointError| error.to_string())?;
    let Some(meta) = found else {
        return Ok((None, false));
    };
    // What a later seal adds has the rest of the stream's extents to sit beside.
    local
        .apply(
            std::slice::from_ref(&meta),
            local.max_id().map_err(|e| e.to_string())?.max(meta.last_id),
            None,
        )
        .map_err(|error| error.to_string())?;
    Ok((Some(meta), true))
}

/// One seal's metadata.
pub(crate) struct CheckpointJob {
    pub seal: u64,
    pub gather: u64,
    pub metas: Vec<StreamMeta>,
    pub objects: Vec<String>,
    /// The WAL file this seal closed, if all it holds is now safely elsewhere (D137).
    pub recycle_seq: Option<u64>,
}

/// One seal's job, finished.
pub(crate) struct CheckpointDone {
    pub seal: u64,
    pub gather: u64,
    pub recycle_seq: Option<u64>,
}

pub(crate) struct Checkpointer {
    pub generation: u64,
    pub local: Arc<LocalMeta>,
    pub store: Arc<dyn ObjectStore>,
    pub checkpoints: CheckpointSet,
    pub full_every_count: usize,
    pub max_id: u64,
    pub loader: mpsc::UnboundedSender<MetaMsg>,
    pub stop: Arc<AtomicBool>,
}

/// A stream gathered twice in one job, as a tombstone on the core it left and alive on the
/// next: the one with the most recent ids wins, and a live one above the tombstone beats it.
fn latest_per_stream(metas: Vec<StreamMeta>) -> Vec<StreamMeta> {
    let mut latest: BTreeMap<String, StreamMeta> = BTreeMap::new();
    for meta in metas {
        match latest.get(&meta.name) {
            Some(known) if (known.last_id, !known.deleted) >= (meta.last_id, !meta.deleted) => {}
            _ => {
                latest.insert(meta.name.clone(), meta);
            }
        }
    }
    latest.into_values().collect()
}

impl Checkpointer {
    async fn retry<T, E: std::fmt::Display, F: AsyncFnMut() -> Result<T, E>>(
        &self,
        what: &str,
        mut work: F,
    ) -> Option<T> {
        let mut attempt = 0;
        loop {
            match work().await {
                Ok(value) => return Some(value),
                Err(error) => {
                    eprintln!("{what}, will retry: {error}");
                    if self.stop.load(Ordering::SeqCst) {
                        return None;
                    }
                    tokio::time::sleep(retry_delay(attempt).min(Duration::from_secs(10))).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Local first, with the file it owes S3, so a crash leaves S3 trailing by one file that the
    /// next startup puts there. `None` if the node stopped first.
    pub(crate) async fn save(&mut self, job: CheckpointJob) -> Option<CheckpointDone> {
        let metas = latest_per_stream(job.metas);
        for meta in &metas {
            self.max_id = self.max_id.max(meta.last_id);
        }
        let changed = !metas.is_empty() || !job.objects.is_empty();
        let head = CheckpointHead {
            full: false,
            max_id: self.max_id,
            objects: job.objects,
        };
        let built = if changed {
            match build_checkpoint(&head, &metas) {
                Ok(built) => Some(built),
                Err(error) => {
                    // Only a stream id or doc past what the format holds, which validation stops.
                    eprintln!("building a checkpoint: {error}");
                    return None;
                }
            }
        } else {
            None
        };
        {
            let (local, max_id) = (Arc::clone(&self.local), self.max_id);
            let owed = built.as_ref().map(|built| {
                let mut owed =
                    Vec::with_capacity(16 + built.parts.iter().map(Bytes::len).sum::<usize>());
                owed.extend_from_slice(&self.generation.to_le_bytes());
                owed.extend_from_slice(&job.seal.to_le_bytes());
                built
                    .parts
                    .iter()
                    .for_each(|part| owed.extend_from_slice(part));
                owed
            });
            self.retry("writing the local metadata store", async || {
                local.apply(&metas, max_id, owed.as_deref())
            })
            .await?;
        }
        storage::crash_point("checkpoint.before_put");
        if let Some(built) = built {
            self.put_file(job.seal, head, metas, built).await?;
        }
        Some(CheckpointDone {
            seal: job.seal,
            gather: job.gather,
            recycle_seq: job.recycle_seq,
        })
    }

    /// One file, which is one PUT, however many streams changed.
    async fn put_file(
        &mut self,
        seq: u64,
        delta: CheckpointHead,
        metas: Vec<StreamMeta>,
        delta_file: storage::BuiltCheckpoint,
    ) -> Option<()> {
        let full = self.checkpoints.files.len() + 1 >= self.full_every_count;
        let (head, built) = if full {
            let store = Arc::clone(&self.store);
            let checkpoints = self.checkpoints.clone();
            let mut all = self
                .retry("merging checkpoints", async || {
                    checkpoints.merge_all(&store).await
                })
                .await?;
            for meta in metas {
                merge_meta(&mut all, meta);
            }
            // Everything recorded so far: nothing older than this file will be read, and an
            // object in no list gets looked at on the next startup.
            let mut known: HashSet<String> = delta.objects.into_iter().collect();
            known.extend(self.checkpoints.recorded_objects().map(str::to_owned));
            let head = CheckpointHead {
                full,
                max_id: delta.max_id,
                objects: known.into_iter().collect(),
            };
            let metas: Vec<StreamMeta> = all.into_values().collect();
            match build_checkpoint(&head, &metas) {
                Ok(built) => (head, built),
                Err(error) => {
                    eprintln!("building a checkpoint: {error}");
                    return None;
                }
            }
        } else {
            (delta, delta_file)
        };
        let path = checkpoint_path(self.generation, seq);
        let store = Arc::clone(&self.store);
        self.retry("writing a checkpoint", async || {
            put_checkpoint(store.as_ref(), &path, &built).await
        })
        .await?;
        let file = CheckpointFile::new(self.generation, seq, built.top, head);
        self.checkpoints.add_newest(file.clone());
        let _ = self.loader.send(MetaMsg::Installed(Box::new(file)));
        Some(())
    }
}

/// How the object-store work is run on its own thread.
pub(crate) fn run_checkpointer(
    mut checkpointer: Checkpointer,
    mut rx: mpsc::UnboundedReceiver<CheckpointJob>,
    done: impl Fn(CheckpointDone),
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    runtime.block_on(async {
        while let Some(job) = rx.recv().await {
            match checkpointer.save(job).await {
                Some(finished) => done(finished),
                None => return,
            }
        }
    });
}
