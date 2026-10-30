//! The messages cores exchange. Nothing between cores is shared but these.

use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use format::IdAllocator;
use proto::ErrorCode;
use tokio::sync::{mpsc, oneshot};

use crate::dedupe::DedupeTable;
use crate::placement::ShardCounters;
use crate::slab::{SlabBytes, SlabRegion};
use crate::state::{StateOp, StreamState};

/// What a client hears from an owner core. Each maps to one frame of section 11.1,
/// except `Moved` and `RouteDone`, which `Client` handles itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Ack {
        handle: u32,
        producer_seq: u64,
        first_id: u64,
        count: u32,
    },
    Nack {
        handle: u32,
        producer_seq: u64,
        code: ErrorCode,
        msg: String,
    },
    StateResult {
        req: u32,
        ok: bool,
        ver: u64,
    },
    Error {
        reference: u32,
        code: ErrorCode,
        msg: String,
    },
    /// This stream's messages are being forwarded, so the route is out of date.
    Moved {
        stream: Arc<str>,
    },
    /// All of this client's earlier messages for the stream have been handled, on `core`.
    RouteDone {
        stream: Arc<str>,
        core: usize,
    },
}

pub(crate) type ReplyTx = mpsc::UnboundedSender<Reply>;

/// Per-stream config of section 13. Retention arrives with M8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamConfig {
    pub default_ack_level: u8,
    pub min_ack_level: u8,
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            default_ack_level: 2,
            min_ack_level: 0,
        }
    }
}

/// The state change a publish carries. It commits with the records or not at all.
#[derive(Debug, Clone)]
pub struct StateChange {
    pub expect_ver: u64,
    pub op: StateOp,
    pub doc: Bytes,
}

#[derive(Debug)]
pub(crate) struct PublishMsg {
    pub stream: Arc<str>,
    pub handle: u32,
    pub producer_id: u64,
    pub producer_seq: u64,
    /// `None` takes the stream's default.
    pub ack_level: Option<u8>,
    pub author: NonZeroU64,
    pub state: Option<StateChange>,
    pub records: SlabRegion,
    pub reply: ReplyTx,
}

#[derive(Debug)]
pub(crate) struct StateOpMsg {
    pub stream: Arc<str>,
    pub req: u32,
    pub author: NonZeroU64,
    pub change: StateChange,
    pub reply: ReplyTx,
}

/// Messages that make records, read only while the core has room for them (D7).
#[derive(Debug)]
pub(crate) enum WriteMsg {
    Publish(Box<PublishMsg>),
    StateOp(Box<StateOpMsg>),
    /// Passes through the route that this client's earlier messages took.
    Barrier {
        stream: Arc<str>,
        reply: ReplyTx,
    },
}

/// The reply to a snapshot: state and head as one consistent step (section 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotReply {
    pub head: u64,
    pub state_ver: u64,
    pub state: Bytes,
}

/// What a core reports about itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoreStats {
    pub core: usize,
    pub stream_count: usize,
    /// Stamped entries waiting for room in the WAL channel.
    pub outbound_count: usize,
    pub wal_stopped: bool,
    pub upload_failed_count: u64,
    /// Sections and dedicated objects waiting for room in the uploader's channel.
    pub uploads_waiting_count: usize,
    /// Unsealed bytes the core holds.
    pub unsealed_bytes: u64,
    pub duplicate_count: u64,
    pub forwarded_count: u64,
}

/// A stream on its way to another core. Its records stay with the old core until they
/// are sealed, along with any acks waiting on them.
#[derive(Debug)]
pub(crate) struct StreamHandoff {
    pub name: Arc<str>,
    pub ids: IdAllocator,
    pub state: StreamState,
    pub dedupe: DedupeTable,
    pub config: StreamConfig,
    /// `(last_id, core)`: the core that stamped ids up to `last_id`, above the entry before.
    pub owners: Vec<(u64, usize)>,
    /// `(core, ticket)`: stamped entries still to reach the WAL channel. Nothing the new
    /// owner stamps can go in ahead of them (D77).
    pub wal_waits: Vec<(usize, u64)>,
}

#[derive(Debug)]
pub(crate) struct ShardHandoff {
    pub shard: u32,
    pub streams: Vec<StreamHandoff>,
    pub counters: ShardCounters,
    /// Override entries that this shard's owner answers for, with where each one was.
    pub overrides: Vec<(Arc<str>, u32, u16)>,
    pub done: oneshot::Sender<()>,
}

/// Everything else. Always read, however backed up the write path is.
#[derive(Debug)]
pub(crate) enum ControlMsg {
    Ping {
        reply: oneshot::Sender<usize>,
    },
    Snapshot {
        stream: Arc<str>,
        reply: oneshot::Sender<Result<SnapshotReply, ErrorCode>>,
    },
    /// Stamped batches this core still holds, in id order.
    ReadMemory {
        stream: Arc<str>,
        reply: oneshot::Sender<Vec<SlabBytes>>,
    },
    Configure {
        stream: Arc<str>,
        config: StreamConfig,
        reply: oneshot::Sender<()>,
    },
    Seal {
        seal: u64,
    },
    MoveShard {
        shard: u32,
        to: usize,
        done: oneshot::Sender<()>,
    },
    /// Gives a stream its own shard, through the override table.
    SplitStream {
        stream: Arc<str>,
        new_shard: u32,
        to: usize,
        done: oneshot::Sender<()>,
    },
    Adopt(Box<ShardHandoff>),
    /// Samples the counters and reports `(shard, load)`.
    ReportLoad {
        reply: oneshot::Sender<Vec<(u32, f64)>>,
    },
    Stats {
        reply: oneshot::Sender<CoreStats>,
    },
    /// A retry for ids this core stamped before the stream moved: reply once those ids
    /// reach `level` (`Reply` is the ack to send).
    AckWhenReached {
        stream: Arc<str>,
        last_id: u64,
        level: u8,
        ack: Reply,
        reply: ReplyTx,
    },
    /// The WAL channel has taken `core`'s entries up to `ticket` (D77).
    WalSent {
        core: usize,
        ticket: u64,
    },
    Stop,
}
