//! The record format (SPEC section 2), publish validation and stamping
//! (section 3), filters and block summaries (section 4), and CRC helpers.
//!
//! No I/O. All fields are little-endian and read through byte slices, so input
//! needs no particular alignment and this crate needs no `unsafe`.
#![forbid(unsafe_code)]

mod crc;
mod error;
mod filter;
mod id;
mod key;
mod publish;
mod record;
mod summary;
mod system;

pub use crc::{crc32c, crc32c_append, crc32c_combine, crc32c_seeded};
pub use error::FormatError;
pub use filter::{Clause, Filter, MAX_CLAUSES, OP_ANY, OP_EQ};
pub use id::{
    COUNTER_BITS, IdAllocator, MAX_ID_LEAD_MS, MAX_ID_MS, RESTART_ID_CREDIT_MS, id_counter, id_ms,
    make_id,
};
pub use key::{IdentityKey, KeyHash};
pub use publish::{ValidatedForward, ValidatedPublish, validate_forward, validate_publish};
pub use record::{
    AUTHOR_OFFSET, CODEC_OFFSET, COUNT_OFFSET, DATA_OFFSET, HEADER_BYTES, ID_OFFSET, LEN_OFFSET,
    OPTIONS_OFFSET, RecordHeader, RecordView, Records, decode_record, encode_record,
    padded_data_bytes,
};
pub use summary::{BlockAuthor, BlockSummary, Decision};
pub use system::{
    ENTRY_SEAL_MANIFEST, ENTRY_STATE_INCR, ENTRY_STATE_MERGE, ENTRY_STATE_SET, SystemRecord,
    decode_manifest_doc, encode_manifest_doc, encode_system_record,
};

/// Set in the producer id of every forward sink, and refused on any other publish, so forwards
/// have a dedupe key space no publisher can reach (section 9.1).
pub const FORWARD_PRODUCER_BIT: u64 = 1 << 63;

/// The producer id a forward from `source` under `sink_id` publishes with. FNV-1a, because it
/// must stay the same across restarts and releases.
pub fn forward_producer_id(source: KeyHash, sink_id: u64) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in source.0.into_iter().chain(sink_id.to_le_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash | FORWARD_PRODUCER_BIT
}
