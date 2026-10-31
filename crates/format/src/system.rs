//! System records: `author = 0`, `options` is the entry type, and data is
//! `actor u64 | doc` (SPEC section 2.2). The only data the server reads.

use crate::record::read_u64;
use crate::{FormatError, RecordHeader, RecordView, encode_record};

pub const ENTRY_STATE_SET: u64 = 1;
pub const ENTRY_STATE_MERGE: u64 = 2;
pub const ENTRY_STATE_INCR: u64 = 3;
/// A seal manifest, in the manifest stream (section 7.5, Q3).
pub const ENTRY_SEAL_MANIFEST: u64 = 4;

const ACTOR_BYTES: usize = 8;

/// Appends a system record with `count = 1` and `codec = 0`.
pub fn encode_system_record(
    out: &mut Vec<u8>,
    id: u64,
    entry_type: u64,
    actor: u64,
    doc: &[u8],
) -> Result<(), FormatError> {
    let len = doc
        .len()
        .checked_add(ACTOR_BYTES)
        .and_then(|len| u32::try_from(len).ok())
        .ok_or(FormatError::DataTooLong)?;
    let header = RecordHeader {
        id,
        options: entry_type,
        author: 0,
        len,
        count: 1,
        codec: 0,
    };
    let mut data = Vec::with_capacity(len as usize);
    data.extend_from_slice(&actor.to_le_bytes());
    data.extend_from_slice(doc);
    encode_record(out, &header, &data)
}

/// What a seal manifest's doc says: `len_bytes u64 | path`, the object and its size. The extents
/// are in the object's own footer, so a manifest stays small however many streams it holds.
pub fn encode_manifest_doc(len_bytes: u64, path: &str) -> Vec<u8> {
    let mut doc = Vec::with_capacity(8 + path.len());
    doc.extend_from_slice(&len_bytes.to_le_bytes());
    doc.extend_from_slice(path.as_bytes());
    doc
}

/// `(len_bytes, path)`.
pub fn decode_manifest_doc(doc: &[u8]) -> Result<(u64, &str), FormatError> {
    if doc.len() < 8 {
        return Err(FormatError::TruncatedSystemRecord);
    }
    let path = std::str::from_utf8(&doc[8..]).map_err(|_| FormatError::TruncatedSystemRecord)?;
    Ok((read_u64(doc, 0), path))
}

/// The parsed contents of a system record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemRecord<'a> {
    pub entry_type: u64,
    pub actor: u64,
    pub doc: &'a [u8],
}

impl<'a> SystemRecord<'a> {
    /// Refuses any record with a non-zero author, which is what keeps the server
    /// from reading anyone else's data.
    pub fn parse(view: &RecordView<'a>) -> Result<SystemRecord<'a>, FormatError> {
        if !view.header.is_system() {
            return Err(FormatError::NotSystemRecord);
        }
        if view.data.len() < ACTOR_BYTES {
            return Err(FormatError::TruncatedSystemRecord);
        }
        Ok(SystemRecord {
            entry_type: view.header.options,
            actor: read_u64(view.data, 0),
            doc: &view.data[ACTOR_BYTES..],
        })
    }
}
