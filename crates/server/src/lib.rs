//! Cores, the write path, state and delivery (SPEC sections 3, 5, 6, 7.2, 10). APIs and
//! recovery come in later milestones.
//!
//! Each core is one thread with one current-thread tokio runtime, and owns its streams
//! outright. Everything between cores, and between cores and the WAL writer or uploader,
//! is a message. This crate reads no record's data unless `author = 0`, and has no
//! compression codec.
#![forbid(unsafe_code)]

mod clock;
mod connection;
mod core;
mod dedupe;
mod delivery;
mod message;
mod node;
mod placement;
mod slab;
mod state;

pub use clock::{Clock, ManualClock, SystemClock};
pub use connection::{Connection, DataFrame, DataPart, Event, Subscription};
pub use dedupe::{
    DEDUPE_CAP_COUNT, DEDUPE_ENTRY_BYTES, DEDUPE_IDLE_MS, DedupeCheck, DedupeEntry, DedupeTable,
};
pub use delivery::Matched;
pub use message::{CoreStats, Reply, SnapshotReply, StateChange, StreamConfig};
pub use node::{
    Client, ClientError, LocalCacheConfig, Node, NodeConfig, NodeExit, Publish, SealOutcome,
    StateOpRequest,
};
pub use placement::{ShardCounters, ShardMove, plan_moves, shard_of};
pub use slab::{SlabAccount, SlabBytes, SlabHook, SlabPool, SlabRegion};
pub use state::{MAX_STATE_BYTES, StateError, StateOp, StreamState, apply_op, check_doc};
