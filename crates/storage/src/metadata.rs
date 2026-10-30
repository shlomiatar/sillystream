//! The local key-value store for idle stream metadata (SPEC section 7.6, D19).
//!
//! It is Fjall with default features off, which leaves out `lz4`. Values are opaque
//! here, because what a stream's metadata holds arrives with recovery (M7). It stays
//! out of the write path: it is written at seals, not per publish.

use std::fmt;
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

/// Fjall's limit. It asserts on a longer or empty key, so both are refused here first.
/// A stream id can be this long too (section 11.1), so a key that adds anything to a
/// stream id must allow for it.
pub const MAX_METADATA_KEY_BYTES: usize = u16::MAX as usize;

/// Fjall's limit on a value, refused here first for the same reason.
const MAX_METADATA_VALUE_BYTES: usize = u32::MAX as usize;

#[derive(Debug)]
pub enum MetadataError {
    Store(fjall::Error),
    /// A key is empty or longer than `MAX_METADATA_KEY_BYTES`.
    KeyLength {
        len_bytes: usize,
    },
    ValueLength {
        len_bytes: usize,
    },
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataError::Store(error) => write!(f, "metadata store: {error}"),
            MetadataError::KeyLength { len_bytes } => {
                write!(f, "metadata store: a key of {len_bytes} bytes")
            }
            MetadataError::ValueLength { len_bytes } => {
                write!(f, "metadata store: a value of {len_bytes} bytes")
            }
        }
    }
}

impl std::error::Error for MetadataError {}

impl From<fjall::Error> for MetadataError {
    fn from(error: fjall::Error) -> Self {
        MetadataError::Store(error)
    }
}

/// One key and value, as `scan_prefix` returns them.
pub type MetadataEntry = (Vec<u8>, Vec<u8>);

fn check_key(key: &[u8]) -> Result<(), MetadataError> {
    if key.is_empty() || key.len() > MAX_METADATA_KEY_BYTES {
        return Err(MetadataError::KeyLength {
            len_bytes: key.len(),
        });
    }
    Ok(())
}

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
        check_key(key)?;
        Ok(self.entries.get(key)?.map(|value| value.to_vec()))
    }

    /// Applies changes atomically and makes them durable before returning, as a seal
    /// needs. `None` removes the key. An invalid key or value applies nothing.
    pub fn apply<'a, I>(&self, changes: I) -> Result<(), MetadataError>
    where
        I: IntoIterator<Item = (&'a [u8], Option<&'a [u8]>)>,
    {
        let mut batch = self.db.batch();
        for (key, value) in changes {
            check_key(key)?;
            match value {
                Some(value) if value.len() > MAX_METADATA_VALUE_BYTES => {
                    return Err(MetadataError::ValueLength {
                        len_bytes: value.len(),
                    });
                }
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
        // No key can start with it, and Fjall asserts on one this long.
        if prefix.len() > MAX_METADATA_KEY_BYTES {
            return Ok(out);
        }
        for guard in self.entries.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            out.push((key.to_vec(), value.to_vec()));
        }
        Ok(out)
    }
}
