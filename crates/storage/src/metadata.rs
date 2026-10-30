//! The local key-value store for idle stream metadata (SPEC section 7.6, D19).
//!
//! It is Fjall with default features off, which leaves out `lz4`. Values are opaque
//! here, because what a stream's metadata holds arrives with recovery (M7). It stays
//! out of the write path: it is written at seals, not per publish.

use std::fmt;
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

#[derive(Debug)]
pub struct MetadataError(fjall::Error);

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metadata store: {}", self.0)
    }
}

impl std::error::Error for MetadataError {}

impl From<fjall::Error> for MetadataError {
    fn from(error: fjall::Error) -> Self {
        MetadataError(error)
    }
}

/// One key and value, as `scan_prefix` returns them.
pub type MetadataEntry = (Vec<u8>, Vec<u8>);

pub struct MetadataStore {
    db: Database,
    entries: Keyspace,
}

impl MetadataStore {
    pub fn open(path: &Path) -> Result<MetadataStore, MetadataError> {
        let db = Database::builder(path).open()?;
        let entries = db.keyspace("metadata", KeyspaceCreateOptions::default)?;
        Ok(MetadataStore { db, entries })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        Ok(self.entries.get(key)?.map(|value| value.to_vec()))
    }

    /// Applies changes atomically and makes them durable before returning, as a seal
    /// needs. `None` removes the key.
    pub fn apply<'a, I>(&self, changes: I) -> Result<(), MetadataError>
    where
        I: IntoIterator<Item = (&'a [u8], Option<&'a [u8]>)>,
    {
        let mut batch = self.db.batch();
        for (key, value) in changes {
            match value {
                Some(value) => batch.insert(&self.entries, key, value),
                None => batch.remove(&self.entries, key),
            }
        }
        batch.commit()?;
        self.db.persist(PersistMode::SyncData)?;
        Ok(())
    }

    /// Every entry whose key starts with `prefix`, in key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<MetadataEntry>, MetadataError> {
        let mut out = Vec::new();
        for guard in self.entries.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            out.push((key.to_vec(), value.to_vec()));
        }
        Ok(out)
    }
}
