//! The local segment directory: `pending/` holds segments waiting to upload, and `cached/` holds
//! uploaded ones under a byte cap shared by both, oldest written out first (SPEC section 7.3).
//!
//! `SegmentCache` has one owner, the uploader thread, and takes `&mut self`: it shares no lock
//! with the cores, which read `cached/` through `MappedSegments`. A startup uploads what
//! `pending/` holds before anything else reads the store.

use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::uploader::put_object;

const PENDING_DIR: &str = "pending";
const CACHED_DIR: &str = "cached";
const TEMP_PREFIX: &str = "tmp-";
const WRITE_BUFFER_BYTES: usize = 1 << 20;
const STORE_NAME: &str = "local segments";
/// Pending segments a startup uploads at once.
const CONCURRENT_STARTUP_PUTS: usize = 8;

#[derive(Debug)]
pub struct CacheError(io::Error);

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "segment directory: {}", self.0)
    }
}

impl std::error::Error for CacheError {}

impl From<io::Error> for CacheError {
    fn from(error: io::Error) -> Self {
        CacheError(error)
    }
}

/// `path` under `dir`. A `Path`'s parts are already encoded so that each is safe as a file name.
fn file_for(dir: &FsPath, path: &Path) -> PathBuf {
    let mut file = dir.to_path_buf();
    file.extend(path.parts().map(|part| part.as_ref().to_owned()));
    file
}

/// Lists every file under `dir`, removing directories that turn out to be empty.
fn walk(dir: &FsPath, found: &mut Vec<(PathBuf, std::fs::Metadata)>) -> io::Result<()> {
    for item in std::fs::read_dir(dir)? {
        let item = item?;
        let metadata = item.metadata()?;
        if metadata.is_dir() {
            walk(&item.path(), found)?;
            // Fails if it has anything in it, which is the answer wanted.
            let _ = std::fs::remove_dir(item.path());
        } else {
            found.push((item.path(), metadata));
        }
    }
    Ok(())
}

/// Maps a whole file. Segments are never written or truncated once they have their name, and
/// removing one leaves a map of it working, so this can't end the process that way. A read the
/// disk fails does end it (SPEC section 7.3).
fn map_file(file: &File) -> io::Result<Bytes> {
    if file.metadata()?.len() == 0 {
        return Ok(Bytes::new());
    }
    // SAFETY: as above. Nothing here changes a file that has its final name.
    let map = unsafe { memmap2::Mmap::map(file) }?;
    Ok(Bytes::from_owner(map))
}

/// Creates `target`'s parents, moves `from` there and makes the name durable.
fn rename_into(from: &FsPath, target: &FsPath) -> io::Result<()> {
    let parent = target.parent().unwrap_or(FsPath::new("."));
    std::fs::create_dir_all(parent)?;
    std::fs::rename(from, target)?;
    File::open(parent)?.sync_all()
}

/// What a PUT task needs to write a segment, apart from the owner's accounting.
#[derive(Debug, Clone)]
pub struct PendingWriter {
    root: PathBuf,
    next_temp: Arc<AtomicU64>,
}

impl PendingWriter {
    /// Writes under a temporary name, fsyncs and renames, so that anything in `pending/` is
    /// complete, and returns the file mapped. A retry writes it again, and has the same bytes.
    pub fn write(&self, path: &Path, parts: &[Bytes]) -> io::Result<Bytes> {
        let target = file_for(&self.root.join(PENDING_DIR), path);
        let temp = self.root.join(format!(
            "{TEMP_PREFIX}{}",
            self.next_temp.fetch_add(1, Ordering::Relaxed)
        ));
        let written = (|| {
            // Readable too, or it can't be mapped.
            let file = File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&temp)?;
            let mut out = BufWriter::with_capacity(WRITE_BUFFER_BYTES, file);
            for part in parts {
                out.write_all(part)?;
            }
            let file = out.into_inner().map_err(|error| error.into_error())?;
            file.sync_all()?;
            rename_into(&temp, &target)?;
            map_file(&file)
        })();
        if written.is_err() {
            // Out of the accounting's sight, so it must not stay, least of all on a full disk.
            let _ = std::fs::remove_file(&temp);
        }
        written
    }
}

pub struct SegmentCache {
    root: PathBuf,
    store: Arc<MappedSegments>,
    cap_bytes: u64,
    pending_bytes: u64,
    cached_bytes: u64,
    /// Oldest written first, with its length.
    cached: VecDeque<(PathBuf, u64)>,
    writer: PendingWriter,
}

impl SegmentCache {
    /// Opens the directory, removing interrupted writes and anything else that isn't a segment,
    /// and keeping `cached/` by modified time. Whatever `pending/` holds stays and counts, and
    /// eviction goes down to `cap_bytes` as far as `cached/` allows.
    pub fn open(root: &FsPath, cap_bytes: u64) -> Result<SegmentCache, CacheError> {
        std::fs::create_dir_all(root.join(PENDING_DIR))?;
        std::fs::create_dir_all(root.join(CACHED_DIR))?;
        let root = root.canonicalize()?;
        for item in std::fs::read_dir(&root)? {
            let item = item?;
            let name = item.file_name();
            if name == PENDING_DIR || name == CACHED_DIR {
                continue;
            }
            if item.metadata()?.is_dir() {
                std::fs::remove_dir_all(item.path())?;
            } else {
                std::fs::remove_file(item.path())?;
            }
        }
        let mut found = Vec::new();
        walk(&root.join(PENDING_DIR), &mut found)?;
        let pending_bytes = found.iter().map(|(_, metadata)| metadata.len()).sum();
        found.clear();
        walk(&root.join(CACHED_DIR), &mut found)?;
        let mut existing = Vec::with_capacity(found.len());
        for (path, metadata) in found {
            let modified = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .map_or(0, |age| age.as_nanos());
            existing.push((modified, path, metadata.len()));
        }
        existing.sort();
        let cached_bytes = existing.iter().map(|(_, _, len_bytes)| *len_bytes).sum();
        let mut cache = SegmentCache {
            store: Arc::new(MappedSegments {
                dir: root.join(CACHED_DIR),
                pending: root.join(PENDING_DIR),
            }),
            writer: PendingWriter {
                root: root.clone(),
                next_temp: Arc::new(AtomicU64::new(0)),
            },
            root,
            cap_bytes,
            pending_bytes,
            cached_bytes,
            cached: existing
                .into_iter()
                .map(|(_, path, len_bytes)| (path, len_bytes))
                .collect(),
        };
        cache.evict()?;
        Ok(cache)
    }

    /// What a former process left in `pending/`, for the uploader to upload (section 7.3).
    pub fn pending_segments(&self) -> Result<Vec<(Path, u64)>, String> {
        list_pending(&self.root)
    }

    pub fn pending_file(&self, path: &Path) -> PathBuf {
        file_for(&self.root.join(PENDING_DIR), path)
    }

    /// A store over `cached/`, for `SegmentReader`. A missing object means it was evicted or
    /// never got here, and the reader goes to S3.
    pub fn store(&self) -> Arc<MappedSegments> {
        Arc::clone(&self.store)
    }

    pub fn writer(&self) -> PendingWriter {
        self.writer.clone()
    }

    pub fn used_bytes(&self) -> u64 {
        self.pending_bytes + self.cached_bytes
    }

    /// Whether another seal may start. At most one segment past the cap gets in.
    pub fn has_room(&self) -> bool {
        self.pending_bytes < self.cap_bytes
    }

    /// Counts a segment about to be written, and makes what room `cached/` can give.
    pub fn reserve(&mut self, len_bytes: u64) -> Result<(), CacheError> {
        self.pending_bytes += len_bytes;
        self.evict()
    }

    /// A pending segment is in the store, under the same `path`. If this fails, the file stays
    /// in `pending/` and the next startup finds the same bytes in the store.
    pub fn uploaded(&mut self, path: &Path, len_bytes: u64) -> Result<(), CacheError> {
        let from = file_for(&self.root.join(PENDING_DIR), path);
        let target = file_for(&self.root.join(CACHED_DIR), path);
        rename_into(&from, &target)?;
        remove_empty_parents(&self.root.join(PENDING_DIR), &from);
        self.pending_bytes = self.pending_bytes.saturating_sub(len_bytes);
        self.cached_bytes += len_bytes;
        self.cached.push_back((target, len_bytes));
        self.evict()
    }

    fn evict(&mut self) -> Result<(), CacheError> {
        while self.used_bytes() > self.cap_bytes {
            let Some((path, len_bytes)) = self.cached.pop_front() else {
                break;
            };
            self.cached_bytes -= len_bytes;
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            remove_empty_parents(&self.root.join(CACHED_DIR), &path);
        }
        Ok(())
    }
}

/// Removes `file`'s directory and those above it, as far as they are empty and below `root`.
fn remove_empty_parents(root: &FsPath, file: &FsPath) {
    let mut dir = file.parent();
    while let Some(current) = dir {
        if current == root || !current.starts_with(root) || std::fs::remove_dir(current).is_err() {
            return;
        }
        dir = current.parent();
    }
}

async fn upload_one(
    root: &FsPath,
    store: &dyn ObjectStore,
    file: PathBuf,
    written: Path,
) -> Result<(), String> {
    let describe = |error: &dyn fmt::Display| format!("{}: {error}", file.display());
    let bytes = File::open(&file)
        .and_then(|handle| map_file(&handle))
        .map_err(|error| describe(&error))?;
    crate::ObjectId::of_path(&written).ok_or_else(|| describe(&"not an object's name"))?;
    put_object(store, &written, std::slice::from_ref(&bytes))
        .await
        .map_err(|error| describe(&error))?;
    rename_into(&file, &file_for(&root.join(CACHED_DIR), &written))
        .map_err(|error| describe(&error))
}

/// What a former process left in `pending/`: each segment's path in the store and its size.
/// Each one is complete, since it only gets its name once it is fsynced. A file not named by an
/// object id is an error.
pub fn list_pending(root: &FsPath) -> Result<Vec<(Path, u64)>, String> {
    let dir = root.join(PENDING_DIR);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut found = Vec::new();
    walk(&dir, &mut found).map_err(|error| format!("{}: {error}", dir.display()))?;
    let mut out = Vec::with_capacity(found.len());
    for (file, metadata) in found {
        let written = file
            .strip_prefix(&dir)
            .ok()
            .and_then(FsPath::to_str)
            .and_then(|relative| Path::parse(relative).ok())
            .filter(|written| crate::ObjectId::of_path(written).is_some())
            .ok_or_else(|| format!("{}: not a segment's name", file.display()))?;
        out.push((written, metadata.len()));
    }
    out.sort();
    Ok(out)
}

/// Uploads what a former process left in `pending/` and moves it to `cached/`. A former
/// generation's limit applies to these as to any object it uploaded late (section 8.1). What's
/// here is complete, since it only gets its name once it is fsynced. Returns how many there were.
pub async fn upload_pending(root: &FsPath, store: &dyn ObjectStore) -> Result<u64, String> {
    let dir = root.join(PENDING_DIR);
    if !dir.exists() {
        return Ok(0);
    }
    let mut found = Vec::new();
    walk(&dir, &mut found).map_err(|error| format!("{}: {error}", dir.display()))?;
    let mut files = Vec::with_capacity(found.len());
    for (file, _) in found {
        let relative = file
            .strip_prefix(&dir)
            .ok()
            .and_then(FsPath::to_str)
            .ok_or_else(|| format!("{}: not a segment's name", file.display()))?;
        let written = Path::parse(relative).map_err(|error| error.to_string())?;
        files.push((file, written));
    }
    let count = files.len() as u64;
    let mut uploads = futures::stream::iter(
        files
            .into_iter()
            .map(|(file, written)| upload_one(root, store, file, written)),
    )
    .buffer_unordered(CONCURRENT_STARTUP_PUTS);
    while let Some(upload) = uploads.next().await {
        upload?;
    }
    Ok(count)
}

/// Reads `cached/` through memory maps, as a store that does nothing else (section 7.3). Every
/// read touches its pages on a blocking thread, so a page fault can't stall a core, and hands out
/// the mapped bytes with no copy.
#[derive(Debug)]
pub struct MappedSegments {
    dir: PathBuf,
    /// Read too: a segment is readable here from its fsync, before it is uploaded (section 6.2).
    pending: PathBuf,
}

impl MappedSegments {
    /// Reads a local segment directory's files, for a startup before its cache is open.
    pub fn new(root: &FsPath) -> MappedSegments {
        MappedSegments {
            dir: root.join(CACHED_DIR),
            pending: root.join(PENDING_DIR),
        }
    }
}

impl fmt::Display for MappedSegments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{STORE_NAME}({})", self.dir.display())
    }
}

fn not_supported() -> object_store::Error {
    object_store::Error::NotImplemented
}

/// Reads one byte per page, which faults them all in. The page size is a lower bound here, so a
/// larger one only touches a page more than once.
fn touch_pages(bytes: &[u8]) {
    const PAGE_BYTES: usize = 4096;
    let mut sum = 0u8;
    for at in (0..bytes.len()).step_by(PAGE_BYTES) {
        sum = sum.wrapping_add(bytes[at]);
    }
    std::hint::black_box(sum);
}

fn read_mapped(
    file_path: &FsPath,
    location: &Path,
    options: &GetOptions,
) -> object_store::Result<GetResult> {
    let store_error = |source: io::Error| object_store::Error::Generic {
        store: STORE_NAME,
        source: Box::new(source),
    };
    let file = File::open(file_path).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(error),
        },
        _ => store_error(error),
    })?;
    let metadata = file.metadata().map_err(store_error)?;
    let size = metadata.len() as usize;
    let meta = ObjectMeta {
        location: location.clone(),
        last_modified: metadata
            .modified()
            .map(DateTime::<Utc>::from)
            .map_err(store_error)?,
        size,
        e_tag: None,
        version: None,
    };
    let range = match &options.range {
        None => 0..size,
        Some(GetRange::Bounded(range)) => range.start..range.end.min(size),
        Some(GetRange::Offset(start)) => *start..size,
        Some(GetRange::Suffix(len)) => size.saturating_sub(*len)..size,
    };
    if range.start > range.end {
        return Err(store_error(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("range {range:?} of a {size} byte file"),
        )));
    }
    let bytes = if options.head {
        Bytes::new()
    } else {
        let bytes = map_file(&file).map_err(store_error)?.slice(range.clone());
        touch_pages(&bytes);
        bytes
    };
    Ok(GetResult {
        payload: GetResultPayload::Stream(futures::stream::once(async move { Ok(bytes) }).boxed()),
        meta,
        range,
        attributes: Default::default(),
    })
}

#[async_trait]
impl ObjectStore for MappedSegments {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let cached = file_for(&self.dir, location);
        let pending = file_for(&self.pending, location);
        let location = location.clone();
        tokio::task::spawn_blocking(move || {
            match read_mapped(&cached, &location, &options) {
                Err(object_store::Error::NotFound { .. }) => {}
                read => return read,
            }
            match read_mapped(&pending, &location, &options) {
                // Renamed across since the first look.
                Err(object_store::Error::NotFound { .. }) => {
                    read_mapped(&cached, &location, &options)
                }
                read => read,
            }
        })
        .await
        .map_err(|error| object_store::Error::JoinError { source: error })?
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(not_supported())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(not_supported())
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(not_supported())
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        futures::stream::once(async { Err(not_supported()) }).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(not_supported())
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(not_supported())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(not_supported())
    }
}
