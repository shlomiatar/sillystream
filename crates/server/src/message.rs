//! The messages cores exchange. Nothing between cores is shared but these.

use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use format::{IdAllocator, KeyHash};
use proto::ErrorCode;
use tokio::sync::{mpsc, oneshot};

use crate::connection::{CommandTicket, Event, ObjectRead, Outbox};
use crate::core::UploadTag;
use crate::dedupe::DedupeTable;
use crate::delivery::{FanOut, ObjectRef, StreamDelivery};
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
        stream: KeyHash,
    },
    /// All of this client's earlier messages for the stream have been handled, on `core`.
    RouteDone {
        stream: KeyHash,
        core: usize,
    },
}

pub(crate) type ReplyTx = mpsc::UnboundedSender<Reply>;

/// Per-stream config of section 13.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamConfig {
    pub default_ack_level: u8,
    pub min_ack_level: u8,
    /// Section 7.4's class, as a length of time.
    pub retention_ms: u64,
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            default_ack_level: 2,
            min_ack_level: 1,
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

/// One sink as the stream holds it: what it was told, then how it is doing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SinkSpec {
    pub kind: String,
    /// `webhook.*`: where it goes.
    pub url: Option<String>,
    /// `forward`: the stream it writes into.
    pub target: Option<KeyHash>,
    /// `D26`'s filter text, already checked.
    pub filter: Option<String>,
    pub on_expire: String,
    pub paused: bool,
    /// `webhook.*`: the HMAC key. Never shown (D29).
    pub secret: Option<String>,
    /// What the sink may hold at once, up to the node's. 0 takes the node's.
    pub budget_bytes: u64,
    /// `webhook.deliver`: `zstd` or `gzip` to compress each POST's body, or none.
    pub compression: Option<String>,
    /// Why it paused itself. A person pausing leaves this empty, and a resume clears it.
    pub pause_reason: Option<String>,
    /// The latest gap a skip recorded (D30), and how many there have been.
    pub last_gap: Option<SinkGap>,
    pub gap_count: u64,
    /// Failures in a row, and the last one. Not saved.
    pub failure_count: u32,
    pub last_error: Option<String>,
    /// The unread id that is about to expire. Not saved.
    pub expiry_warning_id: Option<u64>,
    /// Goes up when the cursor is set from outside, so that a delivery started before then
    /// can't move it back on (D195). Not saved.
    pub epoch: u64,
    /// How many updates this sink has had, so the owner can tell a commit from a spell under an
    /// older spec (D207). Not saved: no spell outlives a restart.
    pub update_count: u64,
    /// The visible head when the latest update was made. Not saved.
    pub update_head_id: u64,
}

/// Ids a sink skipped because they expired unread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkGap {
    pub first_id: u64,
    pub last_id: u64,
}

/// `GET /v1/stream/<id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub head: u64,
    /// The last id given out, visible yet or not.
    pub stamped_id: u64,
    /// The lowest id this stream's objects and memory hold, or 0 with none. A sink at cursor 0
    /// warns from it (D239).
    pub earliest_id: u64,
    pub state_ver: u64,
    pub config: StreamConfig,
    /// `(sink id, spec, cursor)`, in id order.
    pub sinks: Vec<(u64, SinkSpec, Option<u64>)>,
}

/// A change to a stream's sinks: section 9's lifecycle, and what the sink's worker reports.
#[derive(Debug, Clone)]
pub enum SinkOp {
    /// Creates at `start`, or updates a sink that exists and keeps its cursor and pause.
    Put {
        sink_id: u64,
        spec: Box<SinkSpec>,
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
    /// From the sink's worker: the cursor moves forward to `id`, unless `epoch` is stale. A
    /// spell under an older spec than the sink's now moves it no further than D207 allows.
    Commit {
        sink_id: u64,
        id: u64,
        epoch: u64,
        update_count: u64,
    },
    /// From the sink's worker.
    Report {
        sink_id: u64,
        /// The spell's sink: a report from before a reset or a delete is ignored.
        epoch: u64,
        /// The spell's spec: what it says of its own health is ignored once a put replaced it.
        update_count: u64,
        report: SinkReport,
    },
}

impl SinkOp {
    /// Which sink, if this is the lifecycle rather than the worker's own traffic: a person is
    /// waiting for these, and they have to outlive a crash (section 9, D196).
    pub(crate) fn lifecycle_sink(&self) -> Option<u64> {
        match self {
            SinkOp::Put { sink_id, .. }
            | SinkOp::Delete { sink_id }
            | SinkOp::SetPaused { sink_id, .. }
            | SinkOp::Reset { sink_id, .. } => Some(*sink_id),
            SinkOp::Commit { .. } | SinkOp::Report { .. } => None,
        }
    }
}

/// What a sink's worker has to say about itself.
#[derive(Debug, Clone)]
pub enum SinkReport {
    Failure { count: u32, error: String },
    Healthy,
    Warning { first_id: Option<u64> },
    Paused { reason: String },
    Gap { first_id: u64, last_id: u64 },
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
    pub stream: KeyHash,
    pub handle: u32,
    pub producer_id: u64,
    pub producer_seq: u64,
    /// `None` takes the stream's default.
    pub ack_level: Option<u8>,
    pub author: NonZeroU64,
    pub state: Option<StateChange>,
    pub records: SlabRegion,
    pub reply: ReplyTx,
    /// From a forward sink: records keep their own author, and `author` isn't used (section 9.1).
    pub forward: bool,
    /// `(core, its forward count)` the first time each core forwarded this, so that a core
    /// can put its own forwards back in the order it first read them (D78).
    pub passed: Vec<(u16, u64)>,
}

#[derive(Debug)]
pub(crate) struct StateOpMsg {
    pub stream: KeyHash,
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
        stream: KeyHash,
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
    Stream(KeyHash),
}

impl WriteMsg {
    /// The stream it is for. A probe isn't for one in this sense.
    pub(crate) fn stream(&self) -> Option<KeyHash> {
        match self {
            WriteMsg::Publish(publish) => Some(publish.stream),
            WriteMsg::StateOp(op) => Some(op.stream),
            WriteMsg::Barrier { stream, .. } => Some(*stream),
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
    pub name: KeyHash,
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
    pub overrides: Vec<(KeyHash, u32, u16)>,
    /// Deleted streams in this shard, with the id a new one must stay above.
    pub tombstones: Vec<(KeyHash, u64)>,
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
    /// A load that retried and gave up (D243).
    LoadFailed {
        stream: KeyHash,
        ticket: u64,
    },
    StreamInfo {
        stream: KeyHash,
        /// Only this sink in `sinks`, so a sink's worker doesn't copy every spec on its stream.
        only_sink: Option<u64>,
        reply: oneshot::Sender<Result<StreamInfo, ErrorCode>>,
    },
    ConfigurePatch {
        stream: KeyHash,
        patch: StreamConfigPatch,
        reply: oneshot::Sender<StreamConfig>,
    },
    DeleteStream {
        stream: KeyHash,
        reply: oneshot::Sender<bool>,
    },
    Sink {
        stream: KeyHash,
        op: SinkOp,
        reply: oneshot::Sender<Result<(), ErrorCode>>,
    },
    Ping {
        reply: oneshot::Sender<usize>,
    },
    Snapshot {
        stream: KeyHash,
        reply: oneshot::Sender<Result<SnapshotReply, ErrorCode>>,
    },
    /// Stamped batches this core still holds, in id order.
    ReadMemory {
        stream: KeyHash,
        reply: oneshot::Sender<Vec<SlabBytes>>,
    },
    Configure {
        stream: KeyHash,
        config: StreamConfig,
        reply: oneshot::Sender<()>,
    },
    MoveShard {
        shard: u32,
        to: usize,
        done: oneshot::Sender<()>,
    },
    /// Gives a stream its own shard, through the override table.
    SplitStream {
        stream: KeyHash,
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
        stream: KeyHash,
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
        stream: KeyHash,
        filter_id: u32,
        home: usize,
    },
    Cursor {
        stream: KeyHash,
        name: KeyHash,
        op: CursorOp,
        /// Where an error goes.
        report: Option<CursorReport>,
        /// A `COMMIT` from a socket, which ends with this message.
        ticket: Option<CommandTicket>,
    },
    /// An upload's extents for a stream whose owner is elsewhere. Goes back to `origin`
    /// when noted, which is what lets it drop its batches (D83).
    AddExtents {
        stream: KeyHash,
        path: object_store::path::Path,
        extents: Vec<storage::StreamExtent>,
        origin: usize,
        tag: UploadTag,
    },
    ExtentsNoted {
        stream: KeyHash,
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
    /// Answers once everything this core has stamped is in the WAL's channel, so a seal sent
    /// after the answer holds it (D268).
    PassToWal {
        reply: oneshot::Sender<()>,
    },
    /// Whether everything sent before this seal's rotation is sealed and at its owner (D137).
    CheckpointClean {
        seal: u64,
        reply: oneshot::Sender<bool>,
    },
    /// What changed since the last gather.
    CheckpointGather {
        gather: u64,
        reply: oneshot::Sender<Vec<crate::delivery::Gathered>>,
    },
    /// That gather is in the local store and the object store, so what it took may leave RAM.
    CheckpointDone {
        gather: u64,
    },
    /// Writes a seal manifest to the manifest stream, and acks at level 2 (section 7.5).
    PublishManifest {
        doc: Vec<u8>,
        reply: ReplyTx,
    },
    /// A stream's retention, which says which compacted class it goes to (section 7.5). `None`
    /// if there is no such stream.
    RetentionOf {
        stream: KeyHash,
        reply: oneshot::Sender<Option<u64>>,
    },
    /// The metadata a load found, or `None` if there is no such stream (section 7.6).
    Loaded {
        stream: KeyHash,
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
        stream: KeyHash,
        from: u64,
        linger_ms: u16,
        /// `None` if the clauses didn't make a filter.
        filter: Option<format::Filter>,
        name: Option<KeyHash>,
        with_state: bool,
        state_only: bool,
        tag: u64,
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
        stream: KeyHash,
        name: KeyHash,
        id: u64,
    },
    ReadCursor {
        stream: KeyHash,
        name: KeyHash,
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
    pub stream: KeyHash,
    pub sub: SubRef,
    pub filter: format::Filter,
    pub from: u64,
    pub name: Option<KeyHash>,
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
    pub stream: KeyHash,
    pub sub: SubRef,
    pub filter: format::Filter,
    pub after: u64,
    /// Bytes read since the last step, for the shard's counters.
    pub read_bytes: u64,
    /// The object the subscriber read last, so the next is after it.
    pub past: Option<ObjectRef>,
    /// The object the subscriber found missing.
    pub gone: Option<ObjectRef>,
}

/// A read step's walk over the cores that stamped the ids in question, oldest first, which
/// ends at the owner (D83).
#[derive(Debug)]
pub(crate) struct ReadChain {
    pub stream: KeyHash,
    pub sub: SubRef,
    pub after: u64,
    pub limit: u64,
    /// The object the subscriber read last.
    pub past: Option<ObjectRef>,
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
    Object {
        object: ObjectRef,
        /// The object after it in the list, whose pages the reader fetches ahead (section 7.3).
        next: Option<ObjectRef>,
        limit: u64,
    },
    Fail {
        code: ErrorCode,
        msg: String,
    },
}

#[derive(Debug)]
pub(crate) enum ReadDone {
    Read(ObjectRead),
    /// The store failed. The next try is the subscriber's to start.
    Retry,
    /// The object isn't there. The owner knows if the records are somewhere else.
    Gone,
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
        stream: KeyHash,
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
