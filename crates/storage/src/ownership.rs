//! The ownership record (SPEC section 8.1, D14): `ownership/<generation>`, made with a
//! conditional create, and the highest one is current. Each holds the last valid id of the
//! generation before it, so readers can tell what of an older generation's data to accept.
//!
//! ```text
//! record := magic u64 | generation u64 | last_valid_id u64 | crc32c u32 | 0 u32
//! ```

use std::collections::BTreeMap;
use std::fmt;

use format::crc32c;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

use crate::crash::crash_point;

/// `SSOWN001` as little-endian bytes on disk.
pub const OWNERSHIP_MAGIC: u64 = u64::from_le_bytes(*b"SSOWN001");
pub const OWNERSHIP_RECORD_BYTES: usize = 32;
const OWNERSHIP_PREFIX: &str = "ownership";

pub fn ownership_path(generation: u64) -> Path {
    Path::from(format!("{OWNERSHIP_PREFIX}/{generation:020}"))
}

pub fn encode_ownership(generation: u64, last_valid_id: u64) -> [u8; OWNERSHIP_RECORD_BYTES] {
    let mut out = [0u8; OWNERSHIP_RECORD_BYTES];
    out[0..8].copy_from_slice(&OWNERSHIP_MAGIC.to_le_bytes());
    out[8..16].copy_from_slice(&generation.to_le_bytes());
    out[16..24].copy_from_slice(&last_valid_id.to_le_bytes());
    let crc = crc32c(&out[..24]);
    out[24..28].copy_from_slice(&crc.to_le_bytes());
    out
}

/// `(generation, last_valid_id)`, or `None` for anything else.
pub fn decode_ownership(bytes: &[u8]) -> Option<(u64, u64)> {
    let bytes: &[u8; OWNERSHIP_RECORD_BYTES] = bytes.try_into().ok()?;
    let word = |at: usize| {
        let mut field = [0u8; 8];
        field.copy_from_slice(&bytes[at..at + 8]);
        u64::from_le_bytes(field)
    };
    let crc = u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
    if word(0) != OWNERSHIP_MAGIC || crc != crc32c(&bytes[..24]) || bytes[28..] != [0; 4] {
        return None;
    }
    Some((word(8), word(16)))
}

#[derive(Debug)]
pub enum OwnershipError {
    Store(object_store::Error),
    /// A record that isn't one. Guessing a generation could give a second owner.
    Corrupt(String),
    /// Another process took this generation first.
    Lost {
        generation: u64,
    },
    /// Every generation an object id can hold has been taken.
    Exhausted,
}

impl fmt::Display for OwnershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OwnershipError::Store(error) => write!(f, "ownership record: {error}"),
            OwnershipError::Corrupt(path) => write!(f, "ownership record {path} is corrupt"),
            OwnershipError::Lost { generation } => {
                write!(f, "another process took generation {generation} first")
            }
            OwnershipError::Exhausted => write!(
                f,
                "every generation up to {} has been taken, so no object can be named",
                crate::MAX_GENERATION
            ),
        }
    }
}

impl std::error::Error for OwnershipError {}

/// Every ownership record there is.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ownership {
    last_valid_ids: BTreeMap<u64, u64>,
}

impl Ownership {
    /// From `(generation, last_valid_id)` pairs.
    pub fn from_records(records: impl IntoIterator<Item = (u64, u64)>) -> Ownership {
        Ownership {
            last_valid_ids: records.into_iter().collect(),
        }
    }

    /// What this process knows after its own claim.
    pub fn record(&mut self, generation: u64, last_valid_id: u64) {
        self.last_valid_ids.insert(generation, last_valid_id);
    }

    /// `None` on a node that has never owned anything.
    pub fn current_generation(&self) -> Option<u64> {
        self.last_valid_ids.keys().next_back().copied()
    }

    pub fn current_last_valid_id(&self) -> u64 {
        self.last_valid_ids
            .values()
            .next_back()
            .copied()
            .unwrap_or(0)
    }

    /// How far readers accept `generation`'s data (section 8.1): the last valid id the next
    /// generation recorded, or everything if there is no next generation yet.
    pub fn limit_for(&self, generation: u64) -> Option<u64> {
        self.last_valid_ids
            .range(generation.saturating_add(1)..)
            .next()
            .map(|(_, last_valid_id)| *last_valid_id)
    }
}

pub async fn read_ownership(store: &dyn ObjectStore) -> Result<Ownership, OwnershipError> {
    let listed = store
        .list_with_delimiter(Some(&Path::from(OWNERSHIP_PREFIX)))
        .await
        .map_err(OwnershipError::Store)?;
    let mut last_valid_ids = BTreeMap::new();
    for object in listed.objects {
        let bytes = store
            .get(&object.location)
            .await
            .map_err(OwnershipError::Store)?
            .bytes()
            .await
            .map_err(OwnershipError::Store)?;
        let (generation, last_valid_id) = decode_ownership(&bytes)
            .ok_or_else(|| OwnershipError::Corrupt(object.location.to_string()))?;
        last_valid_ids.insert(generation, last_valid_id);
    }
    Ok(Ownership { last_valid_ids })
}

/// Takes the generation above what `known` shows, or fails if someone else got there first.
/// A process that fails here must stop. A conditional create is the only write it makes.
/// Returns the generation and the last valid id it recorded.
pub async fn claim_generation(
    store: &dyn ObjectStore,
    known: &Ownership,
    last_valid_id: u64,
) -> Result<(u64, u64), OwnershipError> {
    let generation = known.current_generation().unwrap_or(0) + 1;
    if generation > crate::MAX_GENERATION {
        return Err(OwnershipError::Exhausted);
    }
    let last_valid_id = last_valid_id.max(known.current_last_valid_id());
    crash_point("ownership.before_write");
    let options = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    let payload = PutPayload::from(encode_ownership(generation, last_valid_id).to_vec());
    match store
        .put_opts(&ownership_path(generation), payload, options)
        .await
    {
        Ok(_) => {
            crash_point("ownership.after_write");
            Ok((generation, last_valid_id))
        }
        Err(object_store::Error::AlreadyExists { .. }) => Err(OwnershipError::Lost { generation }),
        Err(error) => Err(OwnershipError::Store(error)),
    }
}
