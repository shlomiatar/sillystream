//! Stream → virtual shard → core (SPEC section 6.1).

use format::KeyHash;

/// The shard a stream hashes to. The override table comes first, and lives on the cores.
pub fn shard_of(stream: KeyHash, virtual_shard_count: u32) -> u32 {
    (stream.top64() % u64::from(virtual_shard_count)) as u32
}
