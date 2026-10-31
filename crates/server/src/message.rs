//! The messages cores exchange. Nothing between cores is shared but these.

use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use format::IdAllocator;
use proto::ErrorCode;
use tokio::sync::{mpsc, oneshot};

use crate::connection::{CommandTicket, Event, ExtentRead, Outbox};
use crate::core::UploadTag;
use crate::dedupe::DedupeTable;
use crate::delivery::{ExtentRef, FanOut, StreamDelivery};
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

/// Per-stream config of section 13.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamConfig {
    pub default_ack_level: u8,
    pub min_ack_level: u8,
    /// Section 7.4's class, as a length of time. Only reads use it until M8 (D89).
    pub retention_ms: u64,
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            default_ack_level: 2,
            min_ack_level: 0,
            retention_ms: 24 * 60 * 60 * 1000,
        }
    }
}

/// What a config change sets. `None` keeps what the stream has (D21).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamConfigPatch {
    pub default_ack_level: Option<u8>,
    pub min_ack_level: Option<u8>,
    pub retention_ms: Option<u64>,
}

/// One sink as it is registered. Its runtime is M9, so nothing here delivers yet (BUILD_NOTES).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkSpec {
    pub kind: String,
    /// `webhook.*`: where it goes.
    pub url: Option<String>,
    /// `forward`: the stream it writes into.
    pub target: Option<String>,
    /// `D26`'s filter text, already checked.
    pub filter: Option<String>,
    pub on_expire: String,
    pub concurrency: u32,
    pub paused: bool,
}

/// `GET /v1/stream/<id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub head: u64,
    pub state_ver: u64,
    pub config: StreamConfig,
    /// `(sink id, spec, cursor)`, in id order.
    pub sinks: Vec<(u64, SinkSpec, Option<u64>)>,
}

/// A change to a stream's sinks (section 9's lifecycle, without its runtime).
#[derive(Debug, Clone)]
pub enum SinkOp {
    /// Creates at `start`, or updates a sink that exists and keeps its cursor and pause.
    Put {
        sink_id: u64,
        spec: SinkSpec,
        start: SinkStart,
    },
    Delete {
        sink_id: u64,
    },
    SetPaused {
        sink_id: u64,
        paused: bool,
    },
    Reset {
        sink_id: u64,
        start: SinkStart,
    },
}

/// Where a sink's cursor starts, or goes on reset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkStart {
    Now,
    Earliest,
    Id(u64),
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
    /// `(core, its forward count)` the first time each core forwarded this, so that a core
    /// can put its own forwards back in the order it first read them (D78).
    pub passed: Vec<(u16, u64)>,
}

#[derive(Debug)]
pub(crate) struct StateOpMsg {
    pub stream: Arc<str>,
    pub req: u32,
    pub author: NonZeroU64,
    pub change: StateChange,
    pub reply: ReplyTx,
    /// As on `PublishMsg`.
    pub passed: Vec<(u16, u64)>,
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
        /// As on `PublishMsg`.
        passed: Vec<(u16, u64)>,
    },
    /// From a core that has just adopted a shard, down the route it was forwarding on. What
    /// it forwarded is ahead of this at every hop, so its return means all of that has
    /// reached the owner (D78). Read even by a core that isn't reading writes.
    Probe {
        target: ProbeTarget,
        origin: usize,
    },
}

/// What a probe follows: the chain of a shard, or of one stream that has its own shard.
#[derive(Debug)]
pub(crate) enum ProbeTarget {
    Shard(u32),
    Stream(Arc<str>),
}

impl WriteMsg {
    /// The stream it is for. A probe isn't for one in this sense.
    pub(crate) fn stream(&self) -> Option<&str> {
        match self {
            WriteMsg::Publish(publish) => Some(&publish.stream),
            WriteMsg::StateOp(op) => Some(&op.stream),
            WriteMsg::Barrier { stream, .. } => Some(stream),
            WriteMsg::Probe { .. } => None,
        }
    }

    fn passed_mut(&mut self) -> Option<&mut Vec<(u16, u64)>> {
        match self {
            WriteMsg::Publish(publish) => Some(&mut publish.passed),
            WriteMsg::StateOp(op) => Some(&mut op.passed),
            WriteMsg::Barrier { passed, .. } => Some(passed),
            WriteMsg::Probe { .. } => None,
        }
    }

    /// Where this core first forwarded it, if it has.
    pub(crate) fn first_pass(&mut self, core: usize) -> Option<u64> {
        let passed = self.passed_mut()?;
        passed
            .iter()
            .find(|(by, _)| usize::from(*by) == core)
            .map(|(_, count)| *count)
    }

    pub(crate) fn mark_passed(&mut self, core: usize, count: u64) {
        if self.first_pass(core).is_none()
            && let Some(passed) = self.passed_mut()
        {
            passed.push((core as u16, count));
        }
    }
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
    /// One per record per distinct filter (section 5.2).
    pub filter_check_count: u64,
    pub behind_count: u64,
    /// Stalled connections whose unread frames were taken back under memory pressure (D99).
    pub stalled_release_count: u64,
    pub progress_count: u64,
    /// Blocks catch-ups on this home core read, and those summaries let them skip.
    pub catch_up_blocks_read_count: u64,
    pub catch_up_blocks_skipped_count: u64,
    /// Bytes catch-ups on this home core read from each tier.
    pub catch_up_memory_bytes: u64,
    pub catch_up_local_bytes: u64,
    pub catch_up_object_bytes: u64,
    /// Streams this core has let go of, loaded back, and of those how many came from a
    /// checkpoint because the local store didn't have them (section 7.6).
    pub evicted_count: u64,
    pub loaded_count: u64,
    pub rebuilt_count: u64,
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
    pub delivery: StreamDelivery,
}

#[derive(Debug)]
pub(crate) struct ShardHandoff {
    pub shard: u32,
    pub streams: Vec<StreamHandoff>,
    pub counters: ShardCounters,
    /// Override entries that this shard's owner answers for, with where each one was.
    pub overrides: Vec<(Arc<str>, u32, u16)>,
    /// Deleted streams in this shard, with the id a new one must stay above.
    pub tombstones: Vec<(Arc<str>, u64)>,
    pub done: oneshot::Sender<()>,
}

/// Work to start on a core, which is how a socket reaches its home core (D24).
pub(crate) struct RunOnCore(pub Box<dyn FnOnce() + Send>);

impl std::fmt::Debug for RunOnCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RunOnCore")
    }
}

/// Everything else. Always read, however backed up the write path is.
#[derive(Debug)]
pub(crate) enum ControlMsg {
    Run(RunOnCore),
    StreamInfo {
        stream: Arc<str>,
        reply: oneshot::Sender<Result<StreamInfo, ErrorCode>>,
    },
    ConfigurePatch {
        stream: Arc<str>,
        patch: StreamConfigPatch,
        reply: oneshot::Sender<StreamConfig>,
    },
    DeleteStream {
        stream: Arc<str>,
        reply: oneshot::Sender<bool>,
    },
    Sink {
        stream: Arc<str>,
        op: SinkOp,
        reply: oneshot::Sender<Result<(), ErrorCode>>,
    },
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
    Conn(ConnCmd),
    Attach(Box<AttachMsg>),
    /// An explicit id's retention check, asking a stream's earlier owners (D100).
    AttachWalk(Box<AttachMsg>),
    Detach {
        stream: Arc<str>,
        filter_id: u32,
        home: usize,
    },
    Cursor {
        stream: Arc<str>,
        name: Arc<str>,
        op: CursorOp,
        /// Where an error goes.
        report: Option<CursorReport>,
        /// A `COMMIT` from a socket, which ends with this message.
        ticket: Option<CommandTicket>,
    },
    /// An upload's extents for a stream whose owner is elsewhere. Goes back to `origin`
    /// when noted, which is what lets it drop its batches (D83).
    AddExtents {
        stream: Arc<str>,
        path: object_store::path::Path,
        extents: Vec<storage::StreamExtent>,
        origin: usize,
        tag: UploadTag,
    },
    ExtentsNoted {
        stream: Arc<str>,
        tag: UploadTag,
    },
    /// `core`'s entries are durable up to `ticket`, for streams that moved away while an
    /// entry waited (D84).
    DurableSeen {
        core: usize,
        ticket: u64,
    },
    /// A probe this core sent has reached the owner (D78).
    ProbeBack,
    /// The WAL channel has taken `core`'s entries up to `ticket` (D77).
    WalSent {
        core: usize,
        ticket: u64,
    },
    /// Replayed at startup (section 8.1), before anything else reaches this core.
    Restore(Box<crate::recovery::RestoredStream>),
    /// Whether everything sent before this seal's rotation is sealed and at its owner (D137).
    CheckpointClean {
        seal: u64,
        reply: oneshot::Sender<bool>,
    },
    /// What changed since the last gather.
    CheckpointGather {
        gather: u64,
        reply: oneshot::Sender<Vec<storage::StreamMeta>>,
    },
    /// That gather is in the local store and the object store, so what it took may leave RAM.
    CheckpointDone {
        gather: u64,
    },
    /// The metadata a load found, or `None` if there is no such stream (section 7.6).
    Loaded {
        stream: Arc<str>,
        ticket: u64,
        meta: Option<Box<storage::StreamMeta>>,
        rebuilt: bool,
    },
    Stop,
}

/// A connection on its home core, and the reference an error about a cursor should use.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CursorReport {
    pub home: usize,
    pub conn: usize,
    pub conn_uid: u64,
    pub reference: u32,
}

/// Work for catch-ups, which a core does below writes and a little per turn (D101).
#[derive(Debug)]
pub(crate) enum ReadMsg {
    Step(Box<ReadStepMsg>),
    Chain(Box<ReadChain>),
    /// A read task on this core has finished.
    Done {
        sub: SubRef,
        done: Box<ReadDone>,
    },
}

/// A subscription on its home core.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SubRef {
    pub home: usize,
    pub key: usize,
    pub uid: u64,
}

/// What a connection handle asks of its home core.
#[derive(Debug)]
pub(crate) enum ConnCmd {
    Open {
        out: Arc<Outbox>,
        reply: oneshot::Sender<usize>,
    },
    Subscribe {
        conn: usize,
        sub: u32,
        stream: Arc<str>,
        from: u64,
        linger_ms: u16,
        /// `None` if the clauses didn't make a filter.
        filter: Option<format::Filter>,
        name: Option<Arc<str>>,
        with_state: bool,
        state_only: bool,
        ticket: CommandTicket,
    },
    Unsubscribe {
        conn: usize,
        sub: u32,
        _ticket: CommandTicket,
    },
    Commit {
        conn: usize,
        sub: u32,
        id: u64,
        ticket: CommandTicket,
    },
    ResetCursor {
        conn: usize,
        stream: Arc<str>,
        name: Arc<str>,
        id: u64,
    },
    ReadCursor {
        stream: Arc<str>,
        name: Arc<str>,
        reply: oneshot::Sender<Option<u64>>,
    },
    /// The socket has taken this many bytes.
    Credit {
        conn: usize,
        bytes: u64,
    },
    /// The client has read a `BEHIND`, so it is reading again.
    BehindRead {
        conn: usize,
        sub: u32,
    },
    Closed {
        conn: usize,
    },
}

#[derive(Debug)]
pub(crate) enum CursorOp {
    Commit(u64),
    Reset(u64),
    Read(oneshot::Sender<Option<u64>>),
    /// `PUT .../cursor/<name>`: commit or reset, answered with where it ended up.
    Put {
        id: u64,
        reset: bool,
        reply: oneshot::Sender<Result<u64, ErrorCode>>,
    },
}

#[derive(Debug)]
pub(crate) struct AttachMsg {
    pub stream: Arc<str>,
    pub sub: SubRef,
    pub filter: format::Filter,
    pub from: u64,
    pub name: Option<Arc<str>>,
    pub with_state: bool,
    pub state_only: bool,
    /// Set while the check of an explicit id is asking earlier owners (D100).
    pub walk: Option<AttachWalk>,
    /// Ends with this message, on whichever core finishes with it.
    pub _ticket: CommandTicket,
}

/// Which of a stream's earlier owners still have to say what they hold above `cursor`.
#[derive(Debug)]
pub(crate) struct AttachWalk {
    pub owner: usize,
    pub cursor: u64,
    /// `(last_id, core)`, oldest first.
    pub hops: std::collections::VecDeque<(u64, usize)>,
    /// The lowest id found.
    pub found_id: Option<u64>,
}

/// "What comes after `after`?", from a catching-up subscriber to the stream's owner.
#[derive(Debug)]
pub(crate) struct ReadStepMsg {
    pub stream: Arc<str>,
    pub sub: SubRef,
    pub filter: format::Filter,
    pub after: u64,
    /// Bytes read since the last step, for the shard's counters.
    pub read_bytes: u64,
}

/// A read step's walk over the cores that stamped the ids in question, oldest first, which
/// ends at the owner (D83).
#[derive(Debug)]
pub(crate) struct ReadChain {
    pub stream: Arc<str>,
    pub sub: SubRef,
    pub after: u64,
    pub limit: u64,
    /// `(last_id, core)`.
    pub hops: std::collections::VecDeque<(u64, usize)>,
    pub batches: Vec<SlabBytes>,
}

/// An owner's answer to a subscriber. Not in `seq`: one is out at a time per subscriber.
#[derive(Debug)]
pub(crate) enum CatchReply {
    Start {
        head: u64,
        cursor: u64,
        state_ver: u64,
        state: Bytes,
    },
    Memory {
        batches: Vec<SlabBytes>,
        limit: u64,
    },
    Extent {
        extent: ExtentRef,
        limit: u64,
    },
    Fail {
        code: ErrorCode,
        msg: String,
    },
}

#[derive(Debug)]
pub(crate) enum ReadDone {
    Read(ExtentRead),
    /// The store failed. The next try is the subscriber's to start.
    Retry,
    Fail {
        code: ErrorCode,
        msg: String,
    },
}

/// In order for each stream and home core, through `seq`.
#[derive(Debug)]
pub(crate) enum StreamBody {
    Attached {
        sub: SubRef,
        filter_id: u32,
        head: u64,
        cursor: u64,
        state_ver: u64,
        state: Bytes,
    },
    Fan(Arc<FanOut>),
    /// The stream is gone: its subscriptions end, and the next message is number 0.
    Deleted,
    /// The stream left RAM, and the next message is number 0 (section 7.6).
    Evicted,
}

/// What goes from owners to home cores.
#[derive(Debug)]
pub(crate) enum DeliveryMsg {
    Stream {
        stream: Arc<str>,
        seq: u64,
        body: StreamBody,
    },
    Catch {
        sub: SubRef,
        reply: CatchReply,
    },
    Report {
        report: CursorReport,
        event: Event,
    },
}
