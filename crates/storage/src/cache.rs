//! The local tier: copies of uploaded segments under a byte cap, least recently used
//! out first (SPEC section 6.2, D11, D54).
//!
//! Files are laid out as `LocalFileSystem` expects, so a `SegmentReader` works on the
//! cache unchanged through `store()`. This value has one owner, the uploader thread,
//! and takes `&mut self`: it shares no lock with the cores.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::{self, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

const TEMP_PREFIX: &str = "tmp-";

#[derive(Debug)]
pub enum CacheError {
    Io(io::Error),
    Store(object_store::Error),
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::Io(error) => write!(f, "segment cache: {error}"),
            CacheError::Store(error) => write!(f, "segment cache: {error}"),
        }
    }
}

impl std::error::Error for CacheError {}

impl From<io::Error> for CacheError {
    fn from(error: io::Error) -> Self {
        CacheError::Io(error)
    }
}

impl From<object_store::Error> for CacheError {
    fn from(error: object_store::Error) -> Self {
        CacheError::Store(error)
    }
}

#[derive(Debug, Clone, Copy)]
struct CachedFile {
    len_bytes: u64,
    used_at: u64,
}

pub struct SegmentCache {
    root: PathBuf,
    store: Arc<LocalFileSystem>,
    cap_bytes: u64,
    used_bytes: u64,
    files: HashMap<PathBuf, CachedFile>,
    /// `used_at → file`, oldest first.
    by_use: BTreeMap<u64, PathBuf>,
    clock: u64,
    next_temp: u64,
}

fn walk(dir: &FsPath, found: &mut Vec<(PathBuf, std::fs::Metadata)>) -> io::Result<()> {
    for item in std::fs::read_dir(dir)? {
        let item = item?;
        let metadata = item.metadata()?;
        if metadata.is_dir() {
            walk(&item.path(), found)?;
        } else {
            found.push((item.path(), metadata));
        }
    }
    Ok(())
}

impl SegmentCache {
    /// Opens the directory, keeping what is there (oldest modified evicted first) and
    /// removing leftovers of interrupted writes. Evicts down to `cap_bytes` if needed.
    pub fn open(root: &FsPath, cap_bytes: u64) -> Result<SegmentCache, CacheError> {
        std::fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
        let mut found = Vec::new();
        walk(&root, &mut found)?;
        let mut existing = Vec::new();
        for (path, metadata) in found {
            let is_temp = path.parent() == Some(root.as_path())
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(TEMP_PREFIX));
            if is_temp {
                std::fs::remove_file(path)?;
                continue;
            }
            let modified = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .map_or(0, |age| age.as_nanos());
            existing.push((modified, path, metadata.len()));
        }
        existing.sort();
        let mut cache = SegmentCache {
            root,
            store,
            cap_bytes,
            used_bytes: 0,
            files: HashMap::new(),
            by_use: BTreeMap::new(),
            clock: 0,
            next_temp: 0,
        };
        for (_, path, len_bytes) in existing {
            cache.track(path, len_bytes);
        }
        cache.evict()?;
        Ok(cache)
    }

    /// A store over the cached files, for `SegmentReader`. A missing object means
    /// it was never cached or has been evicted, and the reader goes to S3.
    pub fn store(&self) -> Arc<LocalFileSystem> {
        Arc::clone(&self.store)
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn track(&mut self, path: PathBuf, len_bytes: u64) {
        self.clock += 1;
        self.by_use.insert(self.clock, path.clone());
        self.used_bytes += len_bytes;
        let file = CachedFile {
            len_bytes,
            used_at: self.clock,
        };
        if let Some(old) = self.files.insert(path, file) {
            self.by_use.remove(&old.used_at);
            self.used_bytes -= old.len_bytes;
        }
    }

    /// Stores a copy of an object. An object bigger than the cap isn't kept, which
    /// returns `false`. The write is not fsynced: this is a cache, and readers
    /// check CRCs on whatever they read from it.
    pub fn put(&mut self, path: &Path, parts: &[Bytes]) -> Result<bool, CacheError> {
        let len_bytes: u64 = parts.iter().map(|part| part.len() as u64).sum();
        if len_bytes > self.cap_bytes {
            return Ok(false);
        }
        let target = self.store.path_to_filesystem(path)?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp = self.root.join(format!("{TEMP_PREFIX}{}", self.next_temp));
        self.next_temp += 1;
        let mut file = std::fs::File::create(&temp)?;
        for part in parts {
            file.write_all(part)?;
        }
        drop(file);
        std::fs::rename(&temp, &target)?;
        self.track(target, len_bytes);
        self.evict()?;
        Ok(true)
    }

    /// Marks an object as just used. Returns whether it is cached.
    pub fn touch(&mut self, path: &Path) -> Result<bool, CacheError> {
        let target = self.store.path_to_filesystem(path)?;
        let Some(file) = self.files.get(&target).copied() else {
            return Ok(false);
        };
        self.track(target, file.len_bytes);
        Ok(true)
    }

    fn evict(&mut self) -> Result<(), CacheError> {
        while self.used_bytes > self.cap_bytes {
            let Some((_, path)) = self.by_use.pop_first() else {
                break;
            };
            if let Some(file) = self.files.remove(&path) {
                self.used_bytes -= file.len_bytes;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}
