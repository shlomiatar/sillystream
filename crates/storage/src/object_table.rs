//! The object table (SPEC section 7.6): one row per object, kept per node. A row says where the
//! object is, which longer classes it holds, and which compacted objects replace it per class
//! and range of stream hashes. An upload, an eviction or a compaction changes one row, never a
//! stream.
//!
//! ```text
//! key  := class u8 | id u64 big-endian        class 0 for a sealed object, else its code
//! row  := state u8 | held u8 | n u16 | n × (class u8 | hash_first u64 | hash_last u64 | by u64)
//! ```
//!
//! `held` is 0xFF until the compactor has looked at the object, and then the mask of the longer
//! classes it holds streams of. All little-endian but the key.

use std::collections::{BTreeMap, BTreeSet};

use object_store::path::Path;

use crate::meta_format::MetaError;
use crate::object::{ObjectId, RetentionClass, SEALED_PREFIX, object_path};

pub const ROW_KEY_BYTES: usize = 9;
const HELD_UNKNOWN: u8 = 0xFF;
const REPLACEMENT_BYTES: usize = 25;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    /// `None` for a sealed object. A compacted id is unique within its class only.
    pub class: Option<RetentionClass>,
    pub id: ObjectId,
}

impl ObjectKey {
    pub fn sealed(id: ObjectId) -> ObjectKey {
        ObjectKey { class: None, id }
    }

    pub fn compacted(class: RetentionClass, id: ObjectId) -> ObjectKey {
        ObjectKey {
            class: Some(class),
            id,
        }
    }

    pub fn path(self) -> Path {
        match self.class {
            None => object_path(SEALED_PREFIX, self.id),
            Some(class) => object_path(class.prefix(), self.id),
        }
    }

    pub fn encode(self) -> [u8; ROW_KEY_BYTES] {
        let mut out = [0; ROW_KEY_BYTES];
        out[0] = self.class.map_or(0, RetentionClass::code);
        out[1..].copy_from_slice(&self.id.0.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<ObjectKey> {
        let bytes: [u8; ROW_KEY_BYTES] = bytes.try_into().ok()?;
        let class = match bytes[0] {
            0 => None,
            code => Some(RetentionClass::of_code(code)?),
        };
        let id = ObjectId(u64::from_be_bytes(bytes[1..].try_into().ok()?));
        Some(ObjectKey { class, id })
    }
}

/// Where an object is (section 7.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    /// Written locally and not uploaded yet.
    Local,
    /// Uploaded, with a copy kept locally.
    Cached,
    /// Uploaded only.
    Remote,
}

impl ObjectState {
    fn code(self) -> u8 {
        match self {
            ObjectState::Local => 0,
            ObjectState::Cached => 1,
            ObjectState::Remote => 2,
        }
    }

    fn of_code(code: u8) -> Option<ObjectState> {
        match code {
            0 => Some(ObjectState::Local),
            1 => Some(ObjectState::Cached),
            2 => Some(ObjectState::Remote),
            _ => None,
        }
    }

    pub fn is_uploaded(self) -> bool {
        self != ObjectState::Local
    }
}

/// A compacted object that holds what this one held for `class`'s streams with hashes in
/// `hash_first..=hash_last` (section 7.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replacement {
    pub class: RetentionClass,
    pub hash_first: u64,
    pub hash_last: u64,
    pub by: ObjectId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRow {
    pub key: ObjectKey,
    pub state: ObjectState,
    /// The longer classes it holds streams of, once the compactor has looked (section 7.4).
    pub held: Option<u8>,
    pub replacements: Vec<Replacement>,
}

impl ObjectRow {
    pub fn new(key: ObjectKey, state: ObjectState) -> ObjectRow {
        ObjectRow {
            key,
            state,
            held: None,
            replacements: Vec::new(),
        }
    }

    /// The compacted object holding what this one held for a stream of `class` and `hash`.
    pub fn replaced_by(&self, class: RetentionClass, hash: u64) -> Option<ObjectId> {
        self.replacements
            .iter()
            .find(|replacement| {
                replacement.class == class
                    && (replacement.hash_first..=replacement.hash_last).contains(&hash)
            })
            .map(|replacement| replacement.by)
    }

    /// Whether every longer class it holds has a compacted copy, so it can go (section 7.4).
    pub fn is_passed(&self) -> bool {
        self.held.is_some_and(|held| {
            RetentionClass::ALL.iter().all(|class| {
                held & class.bit() == 0
                    || self
                        .replacements
                        .iter()
                        .any(|replacement| replacement.class == *class)
            })
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.state.code());
        out.push(self.held.unwrap_or(HELD_UNKNOWN));
        out.extend_from_slice(&(self.replacements.len() as u16).to_le_bytes());
        for replacement in &self.replacements {
            out.push(replacement.class.code());
            out.extend_from_slice(&replacement.hash_first.to_le_bytes());
            out.extend_from_slice(&replacement.hash_last.to_le_bytes());
            out.extend_from_slice(&replacement.by.0.to_le_bytes());
        }
    }

    /// Decodes a row from the front of `bytes`, saying how many bytes it took.
    pub fn decode(key: ObjectKey, bytes: &[u8]) -> Result<(ObjectRow, usize), MetaError> {
        const BAD: MetaError = MetaError::new("an object table row is corrupt");
        let head = bytes.get(..4).ok_or(BAD)?;
        let state = ObjectState::of_code(head[0]).ok_or(BAD)?;
        let held = (head[1] != HELD_UNKNOWN).then_some(head[1]);
        let count = usize::from(u16::from_le_bytes([head[2], head[3]]));
        let end = 4 + count * REPLACEMENT_BYTES;
        let body = bytes.get(4..end).ok_or(BAD)?;
        let mut replacements = Vec::with_capacity(count);
        for entry in body.as_chunks::<REPLACEMENT_BYTES>().0 {
            let u64_at = |at: usize| u64::from_le_bytes(entry[at..at + 8].try_into().unwrap());
            replacements.push(Replacement {
                class: RetentionClass::of_code(entry[0]).ok_or(BAD)?,
                hash_first: u64_at(1),
                hash_last: u64_at(9),
                by: ObjectId(u64_at(17)),
            });
        }
        Ok((
            ObjectRow {
                key,
                state,
                held,
                replacements,
            },
            end,
        ))
    }
}

/// Rows as a checkpoint or the local store holds them: `n u32 | n × (key | row)`.
pub fn encode_rows(out: &mut Vec<u8>, rows: &[ObjectRow]) {
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        out.extend_from_slice(&row.key.encode());
        row.encode(out);
    }
}

/// The rows at the front of `bytes`, and how many bytes they took.
pub fn decode_rows(bytes: &[u8]) -> Result<(Vec<ObjectRow>, usize), MetaError> {
    const BAD: MetaError = MetaError::new("object table rows are corrupt");
    let count = bytes.get(..4).ok_or(BAD)?;
    let count = u32::from_le_bytes(count.try_into().unwrap()) as usize;
    let mut at = 4;
    let mut rows = Vec::with_capacity(count.min(bytes.len() / (ROW_KEY_BYTES + 4)));
    for _ in 0..count {
        let key = ObjectKey::decode(bytes.get(at..at + ROW_KEY_BYTES).ok_or(BAD)?).ok_or(BAD)?;
        let (row, taken) = ObjectRow::decode(key, &bytes[at + ROW_KEY_BYTES..])?;
        rows.push(row);
        at += ROW_KEY_BYTES + taken;
    }
    Ok((rows, at))
}

/// The node's rows, with which changed since the last checkpoint took them.
#[derive(Debug, Default)]
pub struct ObjectTable {
    rows: BTreeMap<ObjectKey, ObjectRow>,
    changed: BTreeSet<ObjectKey>,
}

impl ObjectTable {
    pub fn new() -> ObjectTable {
        ObjectTable::default()
    }

    /// Rows a startup read, which count as saved.
    pub fn load(&mut self, rows: impl IntoIterator<Item = ObjectRow>) {
        for row in rows {
            self.rows.insert(row.key, row);
        }
    }

    pub fn get(&self, key: ObjectKey) -> Option<&ObjectRow> {
        self.rows.get(&key)
    }

    pub fn rows(&self) -> impl Iterator<Item = &ObjectRow> {
        self.rows.values()
    }

    /// Changes `key`'s row, making it first if there is none.
    pub fn update(
        &mut self,
        key: ObjectKey,
        state: ObjectState,
        change: impl FnOnce(&mut ObjectRow),
    ) {
        let row = self
            .rows
            .entry(key)
            .or_insert_with(|| ObjectRow::new(key, state));
        change(row);
        self.changed.insert(key);
    }

    pub fn set_state(&mut self, key: ObjectKey, state: ObjectState) {
        self.update(key, state, |row| row.state = state);
    }

    /// The row is gone with its object.
    pub fn remove(&mut self, key: ObjectKey) {
        self.rows.remove(&key);
        self.changed.insert(key);
    }

    /// The rows changed since the last call, and the keys of those removed.
    pub fn take_changed(&mut self) -> (Vec<ObjectRow>, Vec<ObjectKey>) {
        let mut changed = Vec::new();
        let mut removed = Vec::new();
        for key in std::mem::take(&mut self.changed) {
            match self.rows.get(&key) {
                Some(row) => changed.push(row.clone()),
                None => removed.push(key),
            }
        }
        (changed, removed)
    }

    /// Hands rows back that a failed checkpoint took, to go in the next.
    pub fn mark_changed(&mut self, keys: impl IntoIterator<Item = ObjectKey>) {
        self.changed.extend(keys);
    }

    /// Where a list's `id` is read from for a stream of `class` and `hash`: a sealed object
    /// leads to the compacted one replacing it (section 7.6).
    pub fn resolve(&self, id: ObjectId, class: Option<RetentionClass>, hash: u64) -> ObjectKey {
        match class {
            Some(class) if id.is_compacted() => ObjectKey::compacted(class, id),
            Some(class) => {
                let by = self
                    .rows
                    .get(&ObjectKey::sealed(id))
                    .and_then(|row| row.replaced_by(class, hash));
                by.map_or(ObjectKey::sealed(id), |by| ObjectKey::compacted(class, by))
            }
            None => ObjectKey::sealed(id),
        }
    }
}
