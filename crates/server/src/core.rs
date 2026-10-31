//! One stream-owning core (SPEC sections 6.1, 6.2): its event loop, write path, seal
//! and shard handoff.
//!
//! The core owns everything here and blocks on nothing: the WAL and the uploader are
//! reached with `try_send`, and whatever doesn't fit waits in a queue here while the
//! loop goes on serving control messages (D7).

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use format::{
    ENTRY_SEAL_MANIFEST, HEADER_BYTES, IdAllocator, ValidatedPublish, encode_system_record,
    validate_publish,
};
use proto::ErrorCode;
use storage::{
    MetaCursor, MetaDedupe, Section, SectionExtent, StreamMeta, UploadEvent, UploadKey,
    UploadSendError, UploaderSender, WalEntry, WalSendError, WalSender, stream_fits_dedicated_path,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::clock::Clock;
use crate::compaction::MANIFEST_STREAM;
use crate::connection::{CommandTicket, DeliverySettings, Home, Stores};
use crate::dedupe::{DedupeCheck, DedupeTable};
use crate::delivery::{
    FanOut, PendingVisible, RecordSpan, ReplaceResult, STATE_ONLY_FILTER_ID, SharedBytes,
    StreamDelivery, UnsavedStep, retention_start_id, walk_records,
};
use crate::message::{
    AttachMsg, AttachWalk, CatchReply, CompactSource, CompactView, ControlMsg, CoreStats, CursorOp,
    CursorReport, DeliveryMsg, ProbeTarget, PublishMsg, ReadChain, ReadMsg, ReadStepMsg, Reply,
    ReplyTx, ShardHandoff, SinkOp, SinkStart, SnapshotReply, StateChange, StateOpMsg, StreamBody,
    StreamConfig, StreamHandoff, StreamInfo, SubRef, WriteMsg,
};
use crate::metadata::MetaMsg;
use crate::placement::{ShardCounters, shard_of};
use crate::slab::{SlabAccount, SlabBytes, SlabPool};
use crate::state::{StreamState, apply_op, check_doc};

/// How often a core retries what didn't fit in a full channel. Crossbeam gives nothing
/// to await, and the writer's wake-ups only cover this core's own entries (D62).
const RETRY_FULL_MS: u64 = 1;
/// `actor u64` of a system record, then its doc (section 2.2).
const ACTOR_BYTES: usize = 8;
/// What a client hears once nothing more can be made durable (D79).
const WAL_STOPPED_MSG: &str = "the WAL writer has stopped, so nothing can be made durable";
/// How many delivery messages a core takes per turn of its loop, whatever else is waiting,
/// so that neither writes nor fan-out can starve the other (D85).
const DELIVERY_PER_TURN_COUNT: usize = 16;
/// How many more steps of catching up a core does on a turn that had nothing above reads to
/// do. On any other turn it does one, so reads can't starve either (D101).
const READS_PER_IDLE_TURN_COUNT: usize = 4;

#[derive(Clone)]
pub(crate) struct Links {
    pub control: Vec<mpsc::UnboundedSender<ControlMsg>>,
    /// Writes passed on by another core. Unbounded, so that two cores forwarding to each
    /// other can't wait on each other. What is in here was read from a bounded inbox.
    /// Always read: what can't be stamped yet waits on the core (D78).
    pub forwarded: Vec<mpsc::UnboundedSender<WriteMsg>>,
    /// Writes from clients. Bounded: a full inbox is backpressure on the publisher.
    pub direct: Vec<mpsc::Sender<WriteMsg>>,
    /// From owners to home cores. Unbounded, since an owner never waits for a subscriber:
    /// each home core drops what is over its own bounds.
    pub delivery: Vec<mpsc::UnboundedSender<DeliveryMsg>>,
    /// Catch-up's reads, which have their own channel so that nothing waits on them (D101).
    pub reads: Vec<mpsc::UnboundedSender<ReadMsg>>,
}

/// What a core tells the node's control thread.
#[derive(Debug)]
pub(crate) enum CoreToNode {
    /// This core's share of the segment cap is used up (Q18).
    SealRequested,
    /// A delete, to answer once a checkpoint holds its tombstone (D20).
    DurableRequested(oneshot::Sender<bool>),
}

pub(crate) struct CoreConfig {
    pub id: usize,
    pub virtual_shard_count: u32,
    pub override_shard_count: u32,
    pub core_count: usize,
    pub segment_cap_share_bytes: u64,
    pub stream_buffer_bytes: u64,
    pub last_valid_id: u64,
    /// How many unwatched streams this core keeps in RAM. `None` never evicts.
    pub active_stream_limit: Option<usize>,
    /// Writes waiting on loads, above which no more are read: this is what a full inbox is
    /// to a publisher (D7).
    pub write_park_cap_count: usize,
}

pub(crate) struct CoreInbox {
    pub control: mpsc::UnboundedReceiver<ControlMsg>,
    pub forwarded: mpsc::UnboundedReceiver<WriteMsg>,
    pub direct: mpsc::Receiver<WriteMsg>,
    pub delivery: mpsc::UnboundedReceiver<DeliveryMsg>,
    pub reads: mpsc::UnboundedReceiver<ReadMsg>,
    pub durable: watch::Receiver<u64>,
    pub uploads: mpsc::UnboundedReceiver<UploadEvent>,
}

/// An entry another core stamped for `stream`, not yet in the WAL channel. What this core
/// stamps after it took the stream over goes in behind it (D77).
struct WalWait {
    stream: Arc<str>,
    core: usize,
    ticket: u64,
    /// This core's last ticket when the stream arrived. Earlier ones don't wait.
    stamped_through: u64,
}

/// Writes for a shard this core just adopted, waiting for what it had forwarded for that
/// shard to arrive first (D78). Control runs one move at a time and `done` goes out only
/// when this ends, so a node has at most one, and only the new owner has it.
struct Hold {
    shard: u32,
    /// Streams whose route changed here because the handoff told this core of their own
    /// shard, whichever shard they were on.
    streams: Vec<Arc<str>>,
    probes_out_count: usize,
    /// This core's own forwards coming back, with when it first forwarded each. A stale
    /// read can send one round twice, so arrival order isn't enough. Everything else was
    /// read later than these.
    returned: Vec<(u64, WriteMsg)>,
    later: VecDeque<WriteMsg>,
    done: oneshot::Sender<()>,
}

struct Waiter {
    reply: Reply,
    tx: ReplyTx,
}

impl Waiter {
    fn fire(self) {
        // A client that has gone away no longer needs its ack.
        let _ = self.tx.send(self.reply);
    }

    /// The write was stamped and may or may not be in the file, so this isn't a `NACK`,
    /// which would promise it committed nothing (D79).
    fn fail_wal_stopped(self) {
        let reference = match self.reply {
            Reply::Ack { handle, .. } => handle,
            Reply::StateResult { req, .. } => req,
            _ => 0,
        };
        let _ = self.tx.send(Reply::Error {
            reference,
            code: ErrorCode::RateLimited,
            msg: WAL_STOPPED_MSG.to_owned(),
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum UploadTag {
    Seal(u64),
    Dedicated(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchUpload {
    Unsealed,
    Pending(UploadTag),
    /// In the store, so level 3 can ack. Stays readable here until the owner knows.
    Done(UploadTag),
    /// The owner has the extent, so nothing needs this copy (D83).
    Noted,
}

/// One WAL entry's bytes: a publish, with one reference to its slab.
struct Batch {
    bytes: SlabBytes,
    first_id: u64,
    last_id: u64,
    ticket: u64,
    /// Which stay on this core the batch was stamped in. A seal takes one stay at a time,
    /// so that a stream moving away and back can't overlap another core's extent.
    residency: u32,
    upload: BatchUpload,
    /// Level 3 acks.
    waiters: Vec<Waiter>,
}

/// The batches this core has stamped for one stream and not yet seen uploaded. It stays
/// here after the stream moves away, since this core's section is what covers them.
#[derive(Default)]
struct StreamLog {
    batches: VecDeque<Batch>,
    first_unsealed: usize,
    unsealed_bytes: u64,
    residency: u32,
    dirty: bool,
}

impl StreamLog {
    fn find(&self, id: u64) -> Option<usize> {
        let at = self.batches.partition_point(|batch| batch.last_id < id);
        self.batches
            .get(at)
            .filter(|batch| batch.first_id <= id)
            .map(|_| at)
    }

    /// Takes what the WAL channel has accepted, from one stay on this core.
    fn take_unsealed(&mut self, tag: UploadTag, sent_ticket: u64) -> (Vec<SlabBytes>, u64) {
        let mut records = Vec::new();
        let mut taken_bytes = 0;
        let Some(residency) = self.batches.get(self.first_unsealed).map(|b| b.residency) else {
            return (records, 0);
        };
        while let Some(batch) = self.batches.get_mut(self.first_unsealed) {
            if batch.ticket > sent_ticket || batch.residency != residency {
                break;
            }
            batch.upload = BatchUpload::Pending(tag);
            records.push(batch.bytes.clone());
            taken_bytes += batch.bytes.len() as u64;
            self.first_unsealed += 1;
        }
        self.unsealed_bytes -= taken_bytes;
        (records, taken_bytes)
    }

    /// Batches above `after`, up to `through`, and about `budget_bytes` of them. In id order.
    fn read_after(&self, after: u64, through: u64, budget_bytes: u64) -> Vec<SlabBytes> {
        let start = self.batches.partition_point(|batch| batch.last_id <= after);
        let mut out = Vec::new();
        let mut taken_bytes = 0;
        for batch in self.batches.iter().skip(start) {
            if batch.last_id > through || taken_bytes >= budget_bytes {
                break;
            }
            taken_bytes += batch.bytes.len() as u64;
            out.push(batch.bytes.clone());
        }
        out
    }

    fn has_unsealed(&self) -> bool {
        self.first_unsealed < self.batches.len()
    }

    /// Level 3 acks go out.
    fn complete(&mut self, tag: UploadTag) {
        for batch in self.batches.iter_mut().take(self.first_unsealed) {
            if batch.upload == BatchUpload::Pending(tag) {
                batch.upload = BatchUpload::Done(tag);
                for waiter in batch.waiters.drain(..) {
                    waiter.fire();
                }
            }
        }
    }

    /// The owner has the extents, so a read can't miss these. The core's references go.
    fn note(&mut self, tag: UploadTag) {
        for batch in self.batches.iter_mut().take(self.first_unsealed) {
            if batch.upload == BatchUpload::Done(tag) {
                batch.upload = BatchUpload::Noted;
            }
        }
        while self
            .batches
            .front()
            .is_some_and(|batch| batch.upload == BatchUpload::Noted)
        {
            self.batches.pop_front();
            self.first_unsealed -= 1;
        }
    }
}

struct Stream {
    name: Arc<str>,
    shard: u32,
    ids: IdAllocator,
    state: StreamState,
    dedupe: DedupeTable,
    config: StreamConfig,
    owners: Vec<(u64, usize)>,
    delivery: StreamDelivery,
}

#[derive(Default)]
struct Shard {
    streams: HashSet<Arc<str>>,
    counters: ShardCounters,
}

enum UploadOut {
    Section {
        seal: u64,
        section: Section<SlabBytes>,
    },
    Dedicated {
        stream: Arc<str>,
        number: u64,
        records: Vec<SlabBytes>,
    },
}

enum Route {
    Here(u32),
    Forward(usize),
}

/// A message for a stream being loaded, in the order it came (section 7.6).
enum Parked {
    Write(WriteMsg),
    Control(ControlMsg),
    Read(ReadMsg),
}

struct Loading {
    ticket: u64,
    parked: Vec<Parked>,
}

fn effective_level(asked: Option<u8>, config: &StreamConfig) -> u8 {
    asked
        .unwrap_or(config.default_ack_level)
        .max(config.min_ack_level)
        .min(3)
}

fn send_reply(tx: &ReplyTx, reply: Reply) {
    let _ = tx.send(reply);
}

/// A stream id has to fit a WAL entry (D42).
fn stream_name_is_valid(name: &str) -> bool {
    !name.is_empty() && name.len() <= usize::from(u16::MAX)
}

/// What the WAL entry for this publish would hold, with its system record.
fn entry_fits(records_bytes: usize, change: Option<&StateChange>) -> bool {
    let system_bytes = change.map_or(0, |change| {
        HEADER_BYTES + (ACTOR_BYTES + change.doc.len()).next_multiple_of(8)
    });
    u32::try_from(records_bytes + system_bytes).is_ok()
}

/// What an owner refuses of a publish with error 8 before anything about the stream counts
/// (section 3). It reads no record's data. Gives the records, ready to stamp, and how many ids
/// they and the state change take.
pub(crate) fn check_publish<'a>(
    stream: &str,
    records: &'a mut [u8],
    change: Option<&StateChange>,
) -> Result<(ValidatedPublish<'a>, u32), String> {
    if !stream_name_is_valid(stream) {
        return Err("stream id is empty or too long".to_owned());
    }
    let validated = validate_publish(records).map_err(|error| error.to_string())?;
    let item_count = validated.item_count() + u64::from(change.is_some());
    if item_count == 0 {
        return Err("publish has no records or state".to_owned());
    }
    let Ok(ack_count) = u32::try_from(item_count) else {
        return Err("publish has too many items".to_owned());
    };
    if !entry_fits(validated.as_bytes().len(), change) {
        return Err("publish is too large".to_owned());
    }
    if let Some(change) = change {
        check_doc(&change.doc).map_err(|error| error.to_string())?;
    }
    Ok((validated, ack_count))
}

pub(crate) struct Core {
    config: CoreConfig,
    clock: Arc<dyn Clock>,
    links: Links,
    inbox: CoreInbox,
    wal: WalSender<SlabBytes>,
    uploader: UploaderSender<SlabBytes>,
    to_node: mpsc::UnboundedSender<CoreToNode>,
    pool: SlabPool,

    /// Where each shard is, as far as this core knows. Following it always ends at the
    /// owner, because each core that gives a shard away remembers where it went, and an
    /// entry is never overwritten by what another core says (D66).
    location: Vec<u16>,
    streams: HashMap<Arc<str>, Stream>,
    shards: HashMap<u32, Shard>,
    /// Stream → its own shard. Kept for good, so that the chain above stays whole.
    overrides: HashMap<Arc<str>, u32>,
    hold: Option<Hold>,
    /// Writes read from the forwarded inbox, for streams on this core, that can't be stamped
    /// until the outbound queue empties (D78).
    deferred: VecDeque<WriteMsg>,

    /// Deleted streams this core answers for, with their last id (D20).
    tombstones: HashMap<Arc<str>, u64>,
    logs: HashMap<Arc<str>, StreamLog>,
    dirty: Vec<Arc<str>>,
    next_ticket: u64,
    /// The highest ticket the WAL channel has taken.
    sent_ticket: u64,
    outbound: VecDeque<WalEntry<SlabBytes>>,
    wal_stopped: bool,
    wal_waits: Vec<WalWait>,
    /// How far the WAL channel has taken each other core's entries, as far as it has said.
    wal_sent_by_core: Vec<u64>,
    /// The ticket other cores are waiting to hear about, or 0.
    wal_sent_owed: u64,
    durable_waiters: VecDeque<(u64, Waiter)>,

    uploads_out: VecDeque<UploadOut>,
    upload_stopped: bool,
    in_flight: HashMap<UploadTag, Vec<Arc<str>>>,
    unsealed_bytes: u64,
    seal_requested: bool,
    dedicated_count: u64,

    upload_failed_count: u64,
    duplicate_count: u64,
    forwarded_count: u64,

    home: Home,
    settings: DeliverySettings,
    /// Streams waiting on the WAL for a batch, in ticket order.
    wal_visible: VecDeque<(u64, Arc<str>)>,
    /// Streams holding batches another core stamped (D84).
    foreign_visible: Vec<Arc<str>>,
    /// How far other cores' entries are durable, as far as they have said.
    durable_by_core: Vec<u64>,
    /// The ticket other cores are waiting to hear is durable, or 0.
    durable_owed: u64,
    filter_check_count: u64,
    spans: Vec<RecordSpan>,

    loader: Option<mpsc::UnboundedSender<MetaMsg>>,
    loading: HashMap<Arc<str>, Loading>,
    load_count: u64,
    /// While a load's parked messages run, that a stream it found no trace of is known new.
    load_passed: Option<Arc<str>>,
    parked_write_count: usize,
    /// Tombstones no checkpoint has yet, with their last ids.
    dirty_tombstones: Vec<(Arc<str>, u64)>,
    /// `sent_ticket` when each seal not yet checkpointed reached this core.
    seal_sent: HashMap<u64, u64>,
    use_count: u64,
    evicted_count: u64,
    loaded_count: u64,
    rebuilt_count: u64,
}

pub(crate) struct CoreParts {
    pub config: CoreConfig,
    pub clock: Arc<dyn Clock>,
    pub links: Links,
    pub inbox: CoreInbox,
    pub wal: WalSender<SlabBytes>,
    pub uploader: UploaderSender<SlabBytes>,
    pub to_node: mpsc::UnboundedSender<CoreToNode>,
    pub pool: SlabPool,
    pub settings: DeliverySettings,
    pub stores: Stores,
    pub accounts: Vec<SlabAccount>,
    /// `None` on a node without a metadata store.
    pub loader: Option<mpsc::UnboundedSender<MetaMsg>>,
}

impl Core {
    pub(crate) fn new(parts: CoreParts) -> Core {
        let config = parts.config;
        let core_count = config.core_count;
        let total = config.virtual_shard_count + config.override_shard_count;
        let location = (0..total)
            .map(|shard| {
                if shard < config.virtual_shard_count {
                    (shard as usize % config.core_count) as u16
                } else {
                    u16::MAX
                }
            })
            .collect();
        let home = Home::new(
            config.id,
            parts.links.clone(),
            parts.settings,
            parts.stores,
            parts.uploader.clone(),
            parts.accounts,
        );
        Core {
            config,
            clock: parts.clock,
            links: parts.links,
            inbox: parts.inbox,
            wal: parts.wal,
            uploader: parts.uploader,
            to_node: parts.to_node,
            pool: parts.pool,
            location,
            streams: HashMap::new(),
            shards: HashMap::new(),
            overrides: HashMap::new(),
            hold: None,
            deferred: VecDeque::new(),
            tombstones: HashMap::new(),
            logs: HashMap::new(),
            dirty: Vec::new(),
            next_ticket: 0,
            sent_ticket: 0,
            outbound: VecDeque::new(),
            wal_stopped: false,
            wal_waits: Vec::new(),
            wal_sent_by_core: vec![0; core_count],
            wal_sent_owed: 0,
            durable_waiters: VecDeque::new(),
            uploads_out: VecDeque::new(),
            upload_stopped: false,
            in_flight: HashMap::new(),
            unsealed_bytes: 0,
            seal_requested: false,
            dedicated_count: 0,
            upload_failed_count: 0,
            duplicate_count: 0,
            forwarded_count: 0,
            home,
            settings: parts.settings,
            wal_visible: VecDeque::new(),
            foreign_visible: Vec::new(),
            durable_by_core: vec![0; core_count],
            durable_owed: 0,
            filter_check_count: 0,
            spans: Vec::new(),
            loader: parts.loader,
            loading: HashMap::new(),
            load_count: 0,
            load_passed: None,
            parked_write_count: 0,
            dirty_tombstones: Vec::new(),
            seal_sent: HashMap::new(),
            use_count: 0,
            evicted_count: 0,
            loaded_count: 0,
            rebuilt_count: 0,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut durable_open = true;
        let mut uploads_open = true;
        let mut stopping = false;
        while !stopping {
            // A stamped entry that doesn't fit stops new writes being read, and nothing else.
            let can_write =
                !self.is_stalled() && self.parked_write_count < self.config.write_park_cap_count;
            let need_retry = (!self.outbound.is_empty() && !self.wal_stopped)
                || (!self.uploads_out.is_empty() && !self.upload_stopped);
            let home_deadline = self.home.next_deadline();
            let mut reads_this_turn = 1;
            tokio::select! {
                biased;
                message = self.inbox.control.recv() => match message {
                    Some(ControlMsg::Stop) | None => stopping = true,
                    Some(message) => self.on_control(message),
                },
                changed = self.inbox.durable.changed(), if durable_open => match changed {
                    Ok(()) => self.on_durable(),
                    Err(_) => {
                        durable_open = false;
                        self.on_wal_stopped();
                    }
                },
                event = self.inbox.uploads.recv(), if uploads_open => match event {
                    Some(event) => self.on_upload_event(event),
                    None => uploads_open = false,
                },
                _ = tokio::time::sleep(Duration::from_millis(RETRY_FULL_MS)), if need_retry => {}
                _ = sleep_until_some(home_deadline), if home_deadline.is_some() => {}
                message = self.inbox.forwarded.recv() => {
                    if let Some(message) = message {
                        self.read_write(message);
                    }
                },
                message = self.inbox.direct.recv(), if can_write => {
                    if let Some(message) = message {
                        self.read_write(message);
                    }
                },
                message = self.inbox.delivery.recv() => {
                    if let Some(message) = message {
                        self.on_delivery(message);
                    }
                },
                message = self.inbox.reads.recv() => {
                    if let Some(message) = message {
                        self.on_read(message);
                    }
                    reads_this_turn = READS_PER_IDLE_TURN_COUNT;
                },
            }
            for _ in 0..DELIVERY_PER_TURN_COUNT {
                let Ok(message) = self.inbox.delivery.try_recv() else {
                    break;
                };
                self.on_delivery(message);
            }
            for _ in 0..reads_this_turn {
                let Ok(message) = self.inbox.reads.try_recv() else {
                    break;
                };
                self.on_read(message);
            }
            if self
                .home
                .next_deadline()
                .is_some_and(|at| at <= tokio::time::Instant::now())
            {
                self.home.on_timer();
            }
            self.home.flush();
            self.flush_outbound();
            self.flush_uploads();
            self.run_deferred();
        }
    }

    fn on_delivery(&mut self, message: DeliveryMsg) {
        match message {
            DeliveryMsg::Stream { stream, seq, body } => self.home.on_stream(stream, seq, body),
            DeliveryMsg::Catch { sub, reply } => self.home.on_catch(sub, reply),
            DeliveryMsg::Report { report, event } => self.home.on_report(report, event),
        }
    }

    fn on_read(&mut self, message: ReadMsg) {
        if let ReadMsg::Step(step) = &message
            && matches!(self.route(&step.stream), Route::Here(_))
        {
            let name = Arc::clone(&step.stream);
            if self.needs_load(&name) {
                self.park(&name, Parked::Read(message));
                return;
            }
            self.touch(&name);
        }
        match message {
            ReadMsg::Step(message) => self.on_read_step(*message),
            ReadMsg::Chain(chain) => self.on_read_chain(chain),
            ReadMsg::Done { sub, done } => self.home.on_read_done(sub, *done),
        }
    }

    /// A stamped entry doesn't fit yet. With the WAL gone, none ever will, and writes are
    /// read in order to be refused (D79).
    fn is_stalled(&self) -> bool {
        !self.outbound.is_empty() && !self.wal_stopped
    }

    /// A write just read from an inbox. Order across a client's writes depends on this
    /// path: see D78.
    fn read_write(&mut self, mut message: WriteMsg) {
        let Some(name) = message.stream() else {
            self.on_write(message);
            return;
        };
        if self.is_held(name) {
            let first_pass = message.first_pass(self.config.id);
            let hold = self.hold.as_mut().expect("is_held found it");
            match first_pass {
                Some(count) => hold.returned.push((count, message)),
                None => hold.later.push_back(message),
            }
            return;
        }
        self.accept(message);
    }

    fn is_held(&self, name: &str) -> bool {
        let Some(hold) = &self.hold else {
            return false;
        };
        let shard = self
            .overrides
            .get(name)
            .copied()
            .unwrap_or_else(|| shard_of(name, self.config.virtual_shard_count));
        shard == hold.shard || hold.streams.iter().any(|stream| &**stream == name)
    }

    /// Passing a write on needs no room here, so that a stalled core still does it.
    fn accept(&mut self, message: WriteMsg) {
        let waits_here = !self.deferred.is_empty() || self.is_stalled();
        if waits_here
            && let Some(name) = message.stream()
            && matches!(self.route(name), Route::Here(_))
        {
            self.deferred.push_back(message);
        } else {
            self.on_write(message);
        }
    }

    fn run_deferred(&mut self) {
        while !self.is_stalled()
            && let Some(message) = self.deferred.pop_front()
        {
            self.on_write(message);
        }
    }

    /// After giving a stream away: what was waiting for it goes after it, in order. So
    /// `deferred` only ever holds writes for streams on this core.
    fn pass_on_deferred(&mut self) {
        for message in std::mem::take(&mut self.deferred) {
            let here = message
                .stream()
                .is_some_and(|name| matches!(self.route(name), Route::Here(_)));
            if here {
                self.deferred.push_back(message);
            } else {
                self.on_write(message);
            }
        }
    }

    fn on_probe(&mut self, target: ProbeTarget, origin: usize) {
        let at = match &target {
            ProbeTarget::Shard(shard) => match self.location[*shard as usize] {
                u16::MAX => self.config.id,
                at => at as usize,
            },
            ProbeTarget::Stream(name) => match self.route(name) {
                Route::Here(_) => self.config.id,
                Route::Forward(to) => to,
            },
        };
        if at != self.config.id {
            let _ = self.links.forwarded[at].send(WriteMsg::Probe { target, origin });
        } else if origin == self.config.id {
            self.on_probe_back();
        } else {
            let _ = self.links.control[origin].send(ControlMsg::ProbeBack);
        }
    }

    fn on_probe_back(&mut self) {
        let Some(hold) = &mut self.hold else {
            return;
        };
        hold.probes_out_count -= 1;
        if hold.probes_out_count > 0 {
            return;
        }
        let mut hold = self.hold.take().expect("just looked at it");
        hold.returned.sort_by_key(|(count, _)| *count);
        let returned = hold.returned.into_iter().map(|(_, message)| message);
        for message in returned.chain(hold.later) {
            self.accept(message);
        }
        let _ = hold.done.send(());
    }

    fn route(&self, name: &str) -> Route {
        if let Some(stream) = self.streams.get(name) {
            return Route::Here(stream.shard);
        }
        let shard = self
            .overrides
            .get(name)
            .copied()
            .unwrap_or_else(|| shard_of(name, self.config.virtual_shard_count));
        let at = self.location[shard as usize] as usize;
        if at == self.config.id {
            Route::Here(shard)
        } else {
            Route::Forward(at)
        }
    }

    fn on_write(&mut self, message: WriteMsg) {
        match message {
            WriteMsg::Publish(publish) => match self.route(&publish.stream) {
                Route::Here(_) if self.wal_stopped => send_reply(
                    &publish.reply,
                    Reply::Nack {
                        handle: publish.handle,
                        producer_seq: publish.producer_seq,
                        code: ErrorCode::RateLimited,
                        msg: WAL_STOPPED_MSG.to_owned(),
                    },
                ),
                Route::Here(_) if self.needs_load(&publish.stream) => {
                    let name = Arc::clone(&publish.stream);
                    self.park(&name, Parked::Write(WriteMsg::Publish(publish)));
                }
                Route::Here(shard) => self.on_publish(*publish, shard),
                Route::Forward(to) => {
                    send_reply(
                        &publish.reply,
                        Reply::Moved {
                            stream: Arc::clone(&publish.stream),
                        },
                    );
                    self.forward_write(to, WriteMsg::Publish(publish));
                }
            },
            WriteMsg::StateOp(op) => match self.route(&op.stream) {
                Route::Here(_) if self.wal_stopped => send_reply(
                    &op.reply,
                    Reply::Error {
                        reference: op.req,
                        code: ErrorCode::RateLimited,
                        msg: WAL_STOPPED_MSG.to_owned(),
                    },
                ),
                Route::Here(_) if self.needs_load(&op.stream) => {
                    let name = Arc::clone(&op.stream);
                    self.park(&name, Parked::Write(WriteMsg::StateOp(op)));
                }
                Route::Here(shard) => self.on_state_op(*op, shard),
                Route::Forward(to) => {
                    send_reply(
                        &op.reply,
                        Reply::Moved {
                            stream: Arc::clone(&op.stream),
                        },
                    );
                    self.forward_write(to, WriteMsg::StateOp(op));
                }
            },
            WriteMsg::Barrier {
                stream,
                reply,
                passed,
            } => match self.route(&stream) {
                // Behind whatever of this client's is waiting for the load.
                Route::Here(_) if self.loading.contains_key(&stream) => {
                    let name = Arc::clone(&stream);
                    let barrier = WriteMsg::Barrier {
                        stream,
                        reply,
                        passed,
                    };
                    self.park(&name, Parked::Write(barrier));
                }
                Route::Here(_) => send_reply(
                    &reply,
                    Reply::RouteDone {
                        stream,
                        core: self.config.id,
                    },
                ),
                Route::Forward(to) => {
                    let barrier = WriteMsg::Barrier {
                        stream,
                        reply,
                        passed,
                    };
                    self.forward_write(to, barrier);
                }
            },
            WriteMsg::Probe { target, origin } => self.on_probe(target, origin),
        }
    }

    fn forward_write(&mut self, to: usize, mut message: WriteMsg) {
        self.forwarded_count += 1;
        message.mark_passed(self.config.id, self.forwarded_count);
        let _ = self.links.forwarded[to].send(message);
    }

    fn stream_entry(&mut self, name: &Arc<str>, shard: u32) -> &mut Stream {
        if !self.streams.contains_key(&**name) {
            self.shards
                .entry(shard)
                .or_default()
                .streams
                .insert(Arc::clone(name));
            let floor_id = self.tombstones.remove(&**name).unwrap_or(0);
            let mut delivery = StreamDelivery::new(self.config.core_count);
            delivery.floor_id = floor_id;
            delivery.track_meta = self.loader.is_some();
            delivery.meta_dirty = delivery.track_meta;
            self.streams.insert(
                Arc::clone(name),
                Stream {
                    name: Arc::clone(name),
                    shard,
                    ids: IdAllocator::new(floor_id, self.config.last_valid_id),
                    state: StreamState::new(),
                    dedupe: DedupeTable::new(),
                    config: StreamConfig::default(),
                    owners: Vec::new(),
                    delivery,
                },
            );
        }
        self.streams
            .get_mut(&**name)
            .expect("the stream was just made if it was missing")
    }

    /// The write path of section 6.2: validate, dedupe, check the state change, stamp in
    /// place, then hand a reference on. Anything rejected has touched nothing.
    fn on_publish(&mut self, msg: PublishMsg, shard: u32) {
        let mut records = msg.records;
        let nack = |code: ErrorCode, text: &dyn std::fmt::Display| {
            send_reply(
                &msg.reply,
                Reply::Nack {
                    handle: msg.handle,
                    producer_seq: msg.producer_seq,
                    code,
                    msg: text.to_string(),
                },
            );
        };
        if &*msg.stream == MANIFEST_STREAM {
            nack(ErrorCode::Unauthorized, &"this stream is the server's");
            return;
        }
        let (mut validated, ack_count) =
            match check_publish(&msg.stream, &mut records, msg.state.as_ref()) {
                Ok(checked) => checked,
                Err(error) => {
                    nack(ErrorCode::Malformed, &error);
                    return;
                }
            };
        let has_state = msg.state.is_some();
        let item_count = u64::from(ack_count);
        let now_ms = self.clock.now_ms();
        if msg.producer_id != 0
            && let Some(stream) = self.streams.get_mut(&*msg.stream)
        {
            let level = effective_level(msg.ack_level, &stream.config);
            let (ack, wait_last_id) =
                match stream
                    .dedupe
                    .check(msg.producer_id, msg.producer_seq, now_ms)
                {
                    DedupeCheck::New => (None, 0),
                    DedupeCheck::Latest { first_id, count } => (
                        Some(Reply::Ack {
                            handle: msg.handle,
                            producer_seq: msg.producer_seq,
                            first_id,
                            count,
                        }),
                        first_id + u64::from(count) - 1,
                    ),
                    DedupeCheck::Older { latest_last_id } => (
                        Some(Reply::Ack {
                            handle: msg.handle,
                            producer_seq: msg.producer_seq,
                            first_id: 0,
                            count: 0,
                        }),
                        latest_last_id,
                    ),
                };
            if let Some(ack) = ack {
                self.duplicate_count += 1;
                self.ack_when_reached(&msg.stream, wait_last_id, level, ack, msg.reply);
                return;
            }
        }
        let new_state = match &msg.state {
            Some(change) => match self.check_state_change(&msg.stream, change) {
                Ok(doc) => Some(doc),
                Err((code, text)) => {
                    nack(code, &text);
                    return;
                }
            },
            None => None,
        };

        let name = Arc::clone(&msg.stream);
        let stream = self.stream_entry(&name, shard);
        let name = Arc::clone(&stream.name);
        let first_id = match stream.ids.allocate(now_ms, item_count) {
            Ok(first_id) => first_id,
            Err(error) => {
                nack(ErrorCode::Malformed, &error);
                return;
            }
        };
        let last_id = first_id + item_count - 1;
        let level = effective_level(msg.ack_level, &stream.config);
        if msg.producer_id != 0 {
            stream.dedupe.record(
                msg.producer_id,
                msg.producer_seq,
                first_id,
                ack_count,
                now_ms,
            );
        }
        validated.stamp(first_id, msg.author);
        let record_count = u64::from(validated.record_count()) + u64::from(has_state);

        let mut state_after = None;
        let bytes = match (&msg.state, new_state) {
            (Some(change), Some(doc)) => {
                let bytes = self.bytes_with_system_record(&records, last_id, msg.author, change);
                state_after = Some((last_id, doc.clone()));
                self.streams
                    .get_mut(&*name)
                    .expect("the stream was made above")
                    .state
                    .install(doc, last_id);
                bytes
            }
            _ => records.freeze(),
        };
        let ack = Reply::Ack {
            handle: msg.handle,
            producer_seq: msg.producer_seq,
            first_id,
            count: ack_count,
        };
        self.commit(
            &name,
            CommitIds {
                producer_id: msg.producer_id,
                producer_seq: msg.producer_seq,
                first_id,
                last_id,
                record_count,
                state_after,
            },
            bytes,
            level,
            Waiter {
                reply: ack,
                tx: msg.reply,
            },
        );
    }

    fn on_state_op(&mut self, msg: StateOpMsg, shard: u32) {
        let error = |code, text: String| {
            send_reply(
                &msg.reply,
                Reply::Error {
                    reference: msg.req,
                    code,
                    msg: text,
                },
            );
        };
        if &*msg.stream == MANIFEST_STREAM {
            error(
                ErrorCode::Unauthorized,
                "this stream is the server's".into(),
            );
            return;
        }
        if !stream_name_is_valid(&msg.stream) {
            error(
                ErrorCode::Malformed,
                "stream id is empty or too long".into(),
            );
            return;
        }
        if !entry_fits(0, Some(&msg.change)) {
            error(ErrorCode::Malformed, "state doc is too large".into());
            return;
        }
        if let Err(state_error) = check_doc(&msg.change.doc) {
            error(ErrorCode::Malformed, state_error.to_string());
            return;
        }
        let doc = match self.check_state_change(&msg.stream, &msg.change) {
            Ok(doc) => doc,
            Err((ErrorCode::VersionConflict, _)) => {
                let ver = self.streams.get(&*msg.stream).map_or(0, |s| s.state.ver());
                send_reply(
                    &msg.reply,
                    Reply::StateResult {
                        req: msg.req,
                        ok: false,
                        ver,
                    },
                );
                return;
            }
            Err((code, text)) => {
                error(code, text);
                return;
            }
        };
        let now_ms = self.clock.now_ms();
        let stream = self.stream_entry(&msg.stream, shard);
        let name = Arc::clone(&stream.name);
        let ver = match stream.ids.allocate(now_ms, 1) {
            Ok(ver) => ver,
            Err(id_error) => {
                error(ErrorCode::Malformed, id_error.to_string());
                return;
            }
        };
        // STATE_OP can't ask for less, and section 10 acks after the WAL (D70).
        let level = effective_level(None, &stream.config).max(2);
        let state_after = Some((ver, doc.clone()));
        stream.state.install(doc, ver);
        let bytes = self.bytes_with_system_record(&[], ver, msg.author, &msg.change);
        self.commit(
            &name,
            CommitIds {
                producer_id: 0,
                producer_seq: 0,
                first_id: ver,
                last_id: ver,
                record_count: 1,
                state_after,
            },
            bytes,
            level,
            Waiter {
                reply: Reply::StateResult {
                    req: msg.req,
                    ok: true,
                    ver,
                },
                tx: msg.reply,
            },
        );
    }

    fn check_state_change(
        &self,
        name: &str,
        change: &StateChange,
    ) -> Result<Bytes, (ErrorCode, String)> {
        let stream = self.streams.get(name);
        let ver = stream.map_or(0, |stream| stream.state.ver());
        if change.expect_ver != 0 && change.expect_ver != ver {
            return Err((
                ErrorCode::VersionConflict,
                format!("state is at version {ver}"),
            ));
        }
        let current: &[u8] = stream.map_or(&[], |stream| stream.state.doc());
        apply_op(current, change.op, &change.doc)
            .map_err(|error| (ErrorCode::Malformed, error.to_string()))
    }

    /// One contiguous region for the WAL's one entry: the stamped records, then the
    /// system record. This is the only copy on the write path (D63).
    fn bytes_with_system_record(
        &mut self,
        records: &[u8],
        id: u64,
        actor: NonZeroU64,
        change: &StateChange,
    ) -> SlabBytes {
        let mut system = Vec::with_capacity(HEADER_BYTES + ACTOR_BYTES + change.doc.len() + 8);
        encode_system_record(
            &mut system,
            id,
            change.op.entry_type(),
            actor.get(),
            &change.doc,
        )
        .expect("entry_fits checked the length");
        let mut region = self.pool.alloc(records.len() + system.len());
        region[..records.len()].copy_from_slice(records);
        region[records.len()..].copy_from_slice(&system);
        region.freeze()
    }

    fn commit(
        &mut self,
        name: &Arc<str>,
        ids: CommitIds,
        bytes: SlabBytes,
        level: u8,
        waiter: Waiter,
    ) {
        self.next_ticket += 1;
        let ticket = self.next_ticket;
        let len_bytes = bytes.len() as u64;
        let entry = WalEntry::new(
            Arc::clone(name),
            ids.producer_id,
            ids.producer_seq,
            bytes.clone(),
            self.config.id,
            ticket,
        )
        .expect("the stream id and the length were checked, and there is at least one record");

        let shard_id = self.streams.get(&**name).map(|stream| stream.shard);
        if let Some(shard) = shard_id.and_then(|shard| self.shards.get_mut(&shard)) {
            shard.counters.ingest_bytes += len_bytes;
            shard.counters.ingest_records += ids.record_count;
        }
        let needs_wal = level >= 2;
        let mut track_meta = false;
        if let Some(stream) = self.streams.get_mut(&**name) {
            track_meta = stream.delivery.track_meta;
            stream.delivery.pending.push_back(PendingVisible {
                core: self.config.id,
                ticket,
                needs_wal,
                last_id: ids.last_id,
                bytes: bytes.clone(),
                state: ids.state_after,
                dedupe: (ids.producer_id != 0).then_some((
                    ids.producer_id,
                    ids.producer_seq,
                    ids.first_id,
                    (ids.last_id - ids.first_id + 1) as u32,
                )),
            });
            stream.delivery.meta_dirty = stream.delivery.track_meta;
            self.use_count += 1;
            stream.delivery.last_used = self.use_count;
        }
        let log = self.logs.entry(Arc::clone(name)).or_default();
        let mut batch = Batch {
            bytes,
            first_id: ids.first_id,
            last_id: ids.last_id,
            ticket,
            residency: log.residency,
            upload: BatchUpload::Unsealed,
            waiters: Vec::new(),
        };
        match level {
            0 => {}
            1 => waiter.fire(),
            2 => self.durable_waiters.push_back((ticket, waiter)),
            _ => batch.waiters.push(waiter),
        }
        log.batches.push_back(batch);
        log.unsealed_bytes += len_bytes;
        let over_stream_buffer = log.unsealed_bytes >= self.config.stream_buffer_bytes;
        if !log.dirty {
            log.dirty = true;
            self.dirty.push(Arc::clone(name));
        }
        self.unsealed_bytes += len_bytes;

        self.outbound.push_back(entry);
        self.flush_outbound();

        // A checkpoint's view waits for the WAL whatever the level (D154).
        if needs_wal || track_meta {
            self.wal_visible.push_back((ticket, Arc::clone(name)));
        }
        self.drain_visible(name);

        if over_stream_buffer && stream_fits_dedicated_path(name) {
            self.send_dedicated(name);
        }
        if !self.seal_requested && self.unsealed_bytes >= self.config.segment_cap_share_bytes {
            self.seal_requested = true;
            let _ = self.to_node.send(CoreToNode::SealRequested);
        }
    }

    /// In order, so a full channel keeps a stream's entries in id order, and behind what
    /// earlier owners of its stream have yet to send (D77).
    fn flush_outbound(&mut self) {
        if self.wal_stopped {
            return;
        }
        while let Some(entry) = self.outbound.pop_front() {
            let ticket = entry.ticket_for_core();
            if !self.wal_waits.is_empty() {
                self.clear_wal_waits();
                if self
                    .wal_waits
                    .iter()
                    .any(|wait| wait.stamped_through < ticket)
                {
                    self.outbound.push_front(entry);
                    return;
                }
            }
            match self.wal.try_append(entry) {
                Ok(()) => {
                    self.sent_ticket = ticket;
                    if self.wal_sent_owed != 0 && ticket >= self.wal_sent_owed {
                        self.wal_sent_owed = 0;
                        self.tell_wal_sent(ticket);
                    }
                }
                Err(WalSendError::Full(entry)) => {
                    self.outbound.push_front(entry);
                    return;
                }
                Err(WalSendError::Stopped(entry)) => {
                    self.outbound.push_front(entry);
                    self.on_wal_stopped();
                    return;
                }
            }
        }
    }

    /// Nothing more can become durable. No other core should wait on what this one holds,
    /// and no client should wait for an ack that can't come (D79). What an upload already
    /// has may still reach level 3.
    fn on_wal_stopped(&mut self) {
        if self.wal_stopped {
            return;
        }
        self.wal_stopped = true;
        self.tell_wal_sent(u64::MAX);
        self.on_durable();
        for (_, waiter) in self.durable_waiters.drain(..) {
            waiter.fail_wal_stopped();
        }
        for log in self.logs.values_mut() {
            for batch in log.batches.iter_mut().skip(log.first_unsealed) {
                for waiter in batch.waiters.drain(..) {
                    waiter.fail_wal_stopped();
                }
            }
        }
    }

    fn tell_wal_sent(&self, ticket: u64) {
        for (core, control) in self.links.control.iter().enumerate() {
            if core != self.config.id {
                let _ = control.send(ControlMsg::WalSent {
                    core: self.config.id,
                    ticket,
                });
            }
        }
    }

    fn clear_wal_waits(&mut self) {
        let (me, own_sent) = (self.config.id, self.sent_ticket);
        let sent = &self.wal_sent_by_core;
        self.wal_waits.retain(|wait| {
            let through = if wait.core == me {
                own_sent
            } else {
                sent[wait.core]
            };
            through < wait.ticket
        });
    }

    /// A stream's waits as it leaves: those it arrived with, and its own entry if one is
    /// still queued here. This core keeps its copies, since entries it stamped wait on them.
    fn wal_waits_for_handoff(&mut self, name: &str) -> Vec<(usize, u64)> {
        let mut waits: Vec<(usize, u64)> = self
            .wal_waits
            .iter()
            .filter(|wait| &*wait.stream == name)
            .map(|wait| (wait.core, wait.ticket))
            .collect();
        let parked = self
            .outbound
            .iter()
            .rev()
            .find(|entry| entry.stream() == name)
            .map(|entry| entry.ticket_for_core());
        if let Some(ticket) = parked
            && !self.wal_stopped
        {
            waits.push((self.config.id, ticket));
            self.wal_sent_owed = self.wal_sent_owed.max(ticket);
        }
        waits
    }

    fn flush_uploads(&mut self) {
        while let Some(out) = self.uploads_out.pop_front() {
            let result = match out {
                UploadOut::Section { seal, section } => self
                    .uploader
                    .try_send_section(seal, self.config.id, section)
                    .map_err(|error| match error {
                        UploadSendError::Full(section) => {
                            (true, UploadOut::Section { seal, section })
                        }
                        UploadSendError::Stopped(section) => {
                            (false, UploadOut::Section { seal, section })
                        }
                    }),
                UploadOut::Dedicated {
                    stream,
                    number,
                    records,
                } => self
                    .uploader
                    .try_send_dedicated(Arc::clone(&stream), number, records)
                    .map_err(|error| match error {
                        UploadSendError::Full(records) => (
                            true,
                            UploadOut::Dedicated {
                                stream: Arc::clone(&stream),
                                number,
                                records,
                            },
                        ),
                        UploadSendError::Stopped(records) => (
                            false,
                            UploadOut::Dedicated {
                                stream: Arc::clone(&stream),
                                number,
                                records,
                            },
                        ),
                    }),
            };
            if let Err((full, out)) = result {
                self.upload_stopped = !full;
                self.uploads_out.push_front(out);
                return;
            }
        }
    }

    fn on_durable(&mut self) {
        let durable_ticket = *self.inbox.durable.borrow_and_update();
        while self
            .durable_waiters
            .front()
            .is_some_and(|(ticket, _)| *ticket <= durable_ticket)
        {
            if let Some((_, waiter)) = self.durable_waiters.pop_front() {
                waiter.fire();
            }
        }
        while self
            .wal_visible
            .front()
            .is_some_and(|(ticket, _)| *ticket <= durable_ticket)
        {
            if let Some((_, name)) = self.wal_visible.pop_front() {
                self.drain_visible(&name);
            }
        }
        self.drain_foreign_visible();
        self.tell_durable_if_owed();
    }

    fn tell_durable_if_owed(&mut self) {
        let durable_ticket = *self.inbox.durable.borrow();
        if self.durable_owed == 0 || durable_ticket < self.durable_owed {
            return;
        }
        self.durable_owed = 0;
        for (core, control) in self.links.control.iter().enumerate() {
            if core != self.config.id {
                let _ = control.send(ControlMsg::DurableSeen {
                    core: self.config.id,
                    ticket: durable_ticket,
                });
            }
        }
    }

    fn drain_foreign_visible(&mut self) {
        if self.foreign_visible.is_empty() {
            return;
        }
        for name in std::mem::take(&mut self.foreign_visible) {
            self.drain_visible(&name);
            let waiting = self.streams.get(&*name).is_some_and(|stream| {
                !stream.delivery.pending.is_empty() || !stream.delivery.unsaved.is_empty()
            });
            if waiting {
                self.foreign_visible.push(name);
            }
        }
    }

    /// Delivery advances in id order: what is ready goes, up to the first that isn't. A
    /// level 0 or 1 batch waits only behind an earlier one that needs the WAL (section 5.2).
    fn drain_visible(&mut self, name: &str) {
        let me = self.config.id;
        let Some(stream) = self.streams.get_mut(name) else {
            return;
        };
        let durable_ticket = *self.inbox.durable.borrow();
        let seen = |core: usize| {
            if core == me {
                durable_ticket
            } else {
                self.durable_by_core[core]
            }
        };
        while let Some(front) = stream.delivery.pending.front() {
            if front.needs_wal && front.ticket > seen(front.core) {
                break;
            }
            let Some(entry) = stream.delivery.pending.pop_front() else {
                break;
            };
            stream.delivery.visible_through = entry.last_id;
            if let Some(state) = &entry.state {
                stream.delivery.visible_state = state.clone();
            }
            if stream.delivery.track_meta {
                stream.delivery.unsaved.push_back(UnsavedStep {
                    core: entry.core,
                    ticket: entry.ticket,
                    last_id: entry.last_id,
                    state: entry.state.clone(),
                    dedupe: entry.dedupe,
                });
            }
            let counters = self.shards.get_mut(&stream.shard).map(|s| &mut s.counters);
            fan_out(
                stream,
                entry,
                FanOutParts {
                    me,
                    links: &self.links,
                    home: &mut self.home,
                    spans: &mut self.spans,
                    filter_check_count: &mut self.filter_check_count,
                    counters,
                },
            );
        }
        while let Some(front) = stream.delivery.unsaved.front() {
            if front.ticket > seen(front.core) {
                break;
            }
            let Some(step) = stream.delivery.unsaved.pop_front() else {
                break;
            };
            let delivery = &mut stream.delivery;
            delivery.meta_dirty = true;
            delivery.saved_through = step.last_id;
            if let Some(state) = step.state {
                delivery.saved_state = state;
            }
            if let Some((producer_id, producer_seq, first_id, count)) = step.dedupe {
                delivery.saved_dedupe.record(
                    producer_id,
                    producer_seq,
                    first_id,
                    count,
                    self.clock.now_ms(),
                );
            }
        }
    }

    fn send_catch(&mut self, sub: SubRef, reply: CatchReply) {
        if sub.home == self.config.id {
            self.home.on_catch(sub, reply);
        } else {
            let _ = self.links.delivery[sub.home].send(DeliveryMsg::Catch { sub, reply });
        }
    }

    /// Where a subscriber starts (section 5.1), then live or catching up.
    fn on_attach(&mut self, mut message: AttachMsg) {
        if let Route::Forward(to) = self.route(&message.stream) {
            // Whatever a walk found, a move since means another core to ask.
            message.walk = None;
            let _ = self.links.control[to].send(ControlMsg::Attach(Box::new(message)));
            return;
        }
        let now_ms = self.clock.now_ms();
        let sub = message.sub;
        let Some(stream) = self.streams.get_mut(&*message.stream) else {
            self.send_catch(
                sub,
                CatchReply::Fail {
                    code: ErrorCode::NotFound,
                    msg: "no such stream".to_owned(),
                },
            );
            return;
        };
        // What is stamped and not yet visible may never have happened (D98).
        let head = stream.delivery.visible_head(stream.ids.last_id());
        let retention_start = retention_start_id(now_ms, stream.config.retention_ms);
        let stored = message
            .name
            .as_ref()
            .and_then(|name| stream.delivery.cursors.get(name).copied());
        let from = stored.unwrap_or(message.from);
        let cursor = match from {
            // The start of retention is what's left, whatever class the stream has now.
            0 => retention_start
                .saturating_sub(1)
                .max(stream.delivery.expired_through),
            u64::MAX => head,
            id => id,
        }
        .max(stream.delivery.floor_id);
        if from != 0 && from != u64::MAX {
            if cursor < stream.delivery.expired_through {
                self.send_catch(
                    sub,
                    CatchReply::Fail {
                        code: ErrorCode::RetentionExpired,
                        msg: "records after that id have expired".to_owned(),
                    },
                );
                return;
            }
            // Only a record below the start can be missed, and only earlier owners may
            // hold one in memory, so most subscribers ask nobody (D100).
            if message.walk.is_none() && cursor.saturating_add(1) < retention_start {
                let me = self.config.id;
                let hops: VecDeque<(u64, usize)> = stream
                    .owners
                    .iter()
                    .filter(|(end, core)| *end > cursor && *core != me)
                    .copied()
                    .collect();
                if !hops.is_empty() {
                    message.walk = Some(AttachWalk {
                        owner: me,
                        cursor,
                        hops,
                        found_id: None,
                    });
                    self.on_attach_walk(Box::new(message));
                    return;
                }
            }
            let own_id = self
                .logs
                .get(&*message.stream)
                .and_then(|log| next_id_in_log(log, cursor, u64::MAX));
            let lowest_id = [
                stream.delivery.sealed_id_above(cursor),
                own_id,
                message.walk.as_ref().and_then(|walk| walk.found_id),
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(head);
            if cursor < head && lowest_id < retention_start {
                self.send_catch(
                    sub,
                    CatchReply::Fail {
                        code: ErrorCode::RetentionExpired,
                        msg: "that id is before the start of retention".to_owned(),
                    },
                );
                return;
            }
        }
        if let Some(name) = &message.name {
            stream
                .delivery
                .cursors
                .entry(Arc::clone(name))
                .or_insert(cursor);
            stream.delivery.meta_dirty = stream.delivery.track_meta;
        }
        let (state_ver, visible_doc) = &stream.delivery.visible_state;
        let state_ver = *state_ver;
        let state = if message.with_state || message.state_only {
            visible_doc.clone()
        } else {
            Bytes::new()
        };
        if message.state_only {
            stream.delivery.table.attach_state_only(sub.home);
            self.send_attached(
                &message.stream,
                sub,
                STATE_ONLY_FILTER_ID,
                head,
                cursor,
                state_ver,
                state,
            );
        } else if cursor >= stream.delivery.visible_through {
            self.attach_live(
                &message.stream,
                sub,
                message.filter,
                head,
                cursor,
                state_ver,
                state,
            );
        } else {
            self.send_catch(
                sub,
                CatchReply::Start {
                    head,
                    cursor,
                    state_ver,
                    state,
                },
            );
        }
    }

    /// One earlier owner says what it holds above the cursor, oldest stretch first, and the
    /// answer goes back to the owner, who decides.
    fn on_attach_walk(&mut self, mut message: Box<AttachMsg>) {
        let me = self.config.id;
        let walk = message.walk.as_mut().expect("only a walk gets here");
        while let Some((end, core)) = walk.hops.pop_front() {
            if core != me {
                walk.hops.push_front((end, core));
                let _ = self.links.control[core].send(ControlMsg::AttachWalk(message));
                return;
            }
            let found = self
                .logs
                .get(&*message.stream)
                .and_then(|log| next_id_in_log(log, walk.cursor, end));
            if found.is_some() {
                walk.found_id = found;
                walk.hops.clear();
            }
        }
        let owner = walk.owner;
        let _ = self.links.control[owner].send(ControlMsg::Attach(message));
    }

    #[allow(clippy::too_many_arguments)]
    fn attach_live(
        &mut self,
        name: &Arc<str>,
        sub: SubRef,
        filter: format::Filter,
        head: u64,
        cursor: u64,
        state_ver: u64,
        state: Bytes,
    ) {
        let Some(stream) = self.streams.get_mut(&**name) else {
            return;
        };
        let filter_id = stream.delivery.table.attach(filter, sub.home);
        if let Some(shard) = self.shards.get_mut(&stream.shard) {
            shard.counters.subscriber_count += 1;
        }
        self.send_attached(name, sub, filter_id, head, cursor, state_ver, state);
    }

    #[allow(clippy::too_many_arguments)]
    fn send_attached(
        &mut self,
        name: &Arc<str>,
        sub: SubRef,
        filter_id: u32,
        head: u64,
        cursor: u64,
        state_ver: u64,
        state: Bytes,
    ) {
        let Some(stream) = self.streams.get_mut(&**name) else {
            return;
        };
        let seq = next_seq(&mut stream.delivery, sub.home);
        let body = StreamBody::Attached {
            sub,
            filter_id,
            head,
            cursor,
            state_ver,
            state,
        };
        if sub.home == self.config.id {
            self.home.on_stream(Arc::clone(name), seq, body);
        } else {
            let _ = self.links.delivery[sub.home].send(DeliveryMsg::Stream {
                stream: Arc::clone(name),
                seq,
                body,
            });
        }
    }

    fn on_detach(&mut self, name: Arc<str>, filter_id: u32, home: usize) {
        if let Route::Forward(to) = self.route(&name) {
            let _ = self.links.control[to].send(ControlMsg::Detach {
                stream: name,
                filter_id,
                home,
            });
            return;
        }
        if let Some(stream) = self.streams.get_mut(&*name) {
            stream.delivery.table.detach(filter_id, home);
            if filter_id != STATE_ONLY_FILTER_ID
                && let Some(shard) = self.shards.get_mut(&stream.shard)
            {
                shard.counters.subscriber_count = shard.counters.subscriber_count.saturating_sub(1);
            }
        }
    }

    /// A catching-up subscriber asks what comes next. Reaching what is visible attaches it,
    /// in one step here, so nothing falls between the read and live (section 5.1).
    fn on_read_step(&mut self, message: ReadStepMsg) {
        if let Route::Forward(to) = self.route(&message.stream) {
            let _ = self.links.reads[to].send(ReadMsg::Step(Box::new(message)));
            return;
        }
        let me = self.config.id;
        let Some(stream) = self.streams.get_mut(&*message.stream) else {
            self.send_catch(
                message.sub,
                CatchReply::Fail {
                    code: ErrorCode::NotFound,
                    msg: "no such stream".to_owned(),
                },
            );
            return;
        };
        if let Some(shard) = self.shards.get_mut(&stream.shard) {
            shard.counters.catch_up_bytes += message.read_bytes;
        }
        // Still listed, with the object gone: these records are lost to this reader. A list
        // that has moved on means compaction has them, and the read carries on from there,
        // unless what moved it on was their expiry (D178).
        if let Some(gone) = &message.gone
            && stream.delivery.drop_path(gone) > 0
        {
            self.send_catch(
                message.sub,
                CatchReply::Fail {
                    code: ErrorCode::RetentionExpired,
                    msg: "the object holding these records is gone".to_owned(),
                },
            );
            return;
        }
        if message.after < stream.delivery.expired_through {
            self.send_catch(
                message.sub,
                CatchReply::Fail {
                    code: ErrorCode::RetentionExpired,
                    msg: "records this read hadn't reached have expired".to_owned(),
                },
            );
            return;
        }
        let limit = stream.delivery.visible_through;
        if message.after >= limit {
            let head = stream.delivery.visible_head(stream.ids.last_id());
            let state_ver = stream.delivery.visible_state.0;
            self.attach_live(
                &message.stream,
                message.sub,
                message.filter,
                head,
                message.after,
                state_ver,
                Bytes::new(),
            );
            return;
        }
        let mut hops: VecDeque<(u64, usize)> = stream
            .owners
            .iter()
            .filter(|(end, _)| *end > message.after)
            .copied()
            .collect();
        hops.push_back((limit, me));
        self.on_read_chain(Box::new(ReadChain {
            stream: message.stream,
            sub: message.sub,
            after: message.after,
            limit,
            hops,
            batches: Vec::new(),
        }));
    }

    /// The oldest core holding anything wanted gives its batches, and then the owner, last,
    /// compares them with its extents. Asking in that order means a batch dropped anywhere
    /// has its extent at the owner by then (D83).
    fn on_read_chain(&mut self, mut chain: Box<ReadChain>) {
        while let Some((end, core)) = chain.hops.pop_front() {
            if core != self.config.id {
                chain.hops.push_front((end, core));
                let _ = self.links.reads[core].send(ReadMsg::Chain(chain));
                return;
            }
            if let Some(log) = self.logs.get(&*chain.stream) {
                chain.batches =
                    log.read_after(chain.after, end.min(chain.limit), self.settings.read_bytes);
                if !chain.batches.is_empty() {
                    chain.hops.clear();
                    break;
                }
            }
        }
        if let Route::Forward(to) = self.route(&chain.stream) {
            let _ = self.links.reads[to].send(ReadMsg::Chain(chain));
            return;
        }
        let name = Arc::clone(&chain.stream);
        if self.needs_load(&name) {
            self.park(&name, Parked::Read(ReadMsg::Chain(chain)));
            return;
        }
        let extent = self
            .streams
            .get(&*chain.stream)
            .and_then(|stream| stream.delivery.first_extent_after(chain.after))
            .cloned();
        let memory_first = chain.batches.first().map(first_id);
        let ReadChain {
            sub,
            limit,
            batches,
            ..
        } = *chain;
        let reply = match (extent, memory_first) {
            (Some(extent), memory_first)
                if memory_first.is_none_or(|first| extent.extent.id_first < first) =>
            {
                // An extent of a former generation ends at its last accepted record (D138).
                let limit = limit.min(extent.extent.id_last);
                CatchReply::Extent { extent, limit }
            }
            _ => CatchReply::Memory { batches, limit },
        };
        self.send_catch(sub, reply);
    }

    fn on_cursor(
        &mut self,
        name: Arc<str>,
        cursor: Arc<str>,
        op: CursorOp,
        report: Option<CursorReport>,
        ticket: Option<CommandTicket>,
    ) {
        if let Route::Forward(to) = self.route(&name) {
            let _ = self.links.control[to].send(ControlMsg::Cursor {
                stream: name,
                name: cursor,
                op,
                report,
                ticket,
            });
            return;
        }
        let fail = |code, msg: &str| {
            if let Some(report) = report {
                let _ = self.links.delivery[report.home].send(DeliveryMsg::Report {
                    report,
                    event: crate::connection::Event::Error {
                        reference: report.reference,
                        code,
                        msg: msg.to_owned(),
                    },
                });
            }
        };
        let Some(stream) = self.streams.get_mut(&*name) else {
            match op {
                CursorOp::Read(reply) => {
                    let _ = reply.send(None);
                }
                CursorOp::Put { reply, .. } => {
                    let _ = reply.send(Err(ErrorCode::NotFound));
                }
                _ => fail(ErrorCode::NotFound, "no such stream"),
            }
            return;
        };
        let head = stream.ids.last_id();
        stream.delivery.meta_dirty = stream.delivery.track_meta;
        match op {
            CursorOp::Commit(id) if id > head => {
                fail(ErrorCode::Malformed, "COMMIT above the stream's head");
            }
            CursorOp::Commit(id) => {
                // Forward only: a lower one is ignored (section 5.1).
                let at = stream.delivery.cursors.entry(cursor).or_insert(id);
                *at = (*at).max(id);
            }
            CursorOp::Reset(id) => {
                stream.delivery.cursors.insert(cursor, id);
            }
            CursorOp::Read(reply) => {
                let _ = reply.send(stream.delivery.cursors.get(&cursor).copied());
            }
            CursorOp::Put { id, reset, reply } => {
                if !reset && id > head {
                    let _ = reply.send(Err(ErrorCode::Malformed));
                    return;
                }
                let at = stream.delivery.cursors.entry(cursor).or_insert(id);
                *at = if reset { id } else { (*at).max(id) };
                let _ = reply.send(Ok(*at));
            }
        }
    }

    /// An upload's extents reach the stream's owner before this core lets go of its copies.
    fn note_extents(
        &mut self,
        tag: UploadTag,
        names: &[Arc<str>],
        path: &object_store::path::Path,
        extents: &[storage::StreamExtent],
    ) {
        let mut by_stream: HashMap<&str, Vec<storage::StreamExtent>> = HashMap::new();
        for extent in extents {
            by_stream
                .entry(&*extent.stream)
                .or_default()
                .push(extent.clone());
        }
        for name in names {
            let own = by_stream.remove(&**name).unwrap_or_default();
            if let Some(stream) = self.streams.get_mut(&**name) {
                for extent in &own {
                    stream.delivery.add_extent(path, extent);
                }
                self.note_batches(name, tag);
            } else if let Route::Forward(to) = self.route(name) {
                let _ = self.links.control[to].send(ControlMsg::AddExtents {
                    stream: Arc::clone(name),
                    path: path.clone(),
                    extents: own,
                    origin: self.config.id,
                    tag,
                });
            } else if self.loader.is_some() {
                // It may only be out of RAM, and the extent must reach its metadata.
                self.on_control(ControlMsg::AddExtents {
                    stream: Arc::clone(name),
                    path: path.clone(),
                    extents: own,
                    origin: self.config.id,
                    tag,
                });
            } else {
                self.note_batches(name, tag);
            }
        }
    }

    fn note_batches(&mut self, name: &str, tag: UploadTag) {
        if let Some(log) = self.logs.get_mut(name) {
            log.note(tag);
            if log.batches.is_empty() && !self.streams.contains_key(name) {
                self.logs.remove(name);
            }
        }
    }

    /// A duplicate is acked once what it duplicates has reached the level asked for. The
    /// original may still be on its way, and may have been stamped by an earlier owner.
    fn ack_when_reached(
        &mut self,
        name: &Arc<str>,
        last_id: u64,
        level: u8,
        ack: Reply,
        tx: ReplyTx,
    ) {
        match level {
            0 => return,
            1 => {
                send_reply(&tx, ack);
                return;
            }
            _ => {}
        }
        let owner = self
            .streams
            .get(&**name)
            .and_then(|stream| stream.owners.iter().find(|(end, _)| *end >= last_id))
            .map_or(self.config.id, |(_, core)| *core);
        if owner == self.config.id {
            self.ack_local(name, last_id, level, ack, tx);
        } else {
            let _ = self.links.control[owner].send(ControlMsg::AckWhenReached {
                stream: Arc::clone(name),
                last_id,
                level,
                ack,
                reply: tx,
            });
        }
    }

    fn ack_local(&mut self, name: &str, last_id: u64, level: u8, ack: Reply, tx: ReplyTx) {
        let waiter = Waiter { reply: ack, tx };
        let wal_stopped = self.wal_stopped;
        let batch = self.logs.get_mut(name).and_then(|log| {
            let at = log.find(last_id)?;
            log.batches.get_mut(at)
        });
        // No longer held means uploaded.
        let Some(batch) = batch else {
            waiter.fire();
            return;
        };
        if level == 2 {
            let durable_ticket = *self.inbox.durable.borrow();
            if batch.ticket <= durable_ticket {
                waiter.fire();
            } else if wal_stopped {
                waiter.fail_wal_stopped();
            } else {
                let ticket = batch.ticket;
                let at = self.durable_waiters.partition_point(|(t, _)| *t <= ticket);
                self.durable_waiters.insert(at, (ticket, waiter));
            }
        } else if matches!(batch.upload, BatchUpload::Done(_) | BatchUpload::Noted) {
            waiter.fire();
        } else if wal_stopped && batch.upload == BatchUpload::Unsealed {
            waiter.fail_wal_stopped();
        } else {
            batch.waiters.push(waiter);
        }
    }

    fn on_seal(&mut self, seal: u64) {
        self.seal_requested = false;
        if self.loader.is_some() {
            self.seal_sent.insert(seal, self.sent_ticket);
        }
        let tag = UploadTag::Seal(seal);
        let mut names = std::mem::take(&mut self.dirty);
        names.sort_unstable();
        names.dedup();
        let mut extents = Vec::new();
        let mut touched = Vec::new();
        for name in names {
            let Some(log) = self.logs.get_mut(&*name) else {
                continue;
            };
            let (records, taken_bytes) = log.take_unsealed(tag, self.sent_ticket);
            self.unsealed_bytes -= taken_bytes;
            let again = log.has_unsealed();
            log.dirty = again;
            if !records.is_empty() {
                touched.push(Arc::clone(&name));
                extents.push(SectionExtent {
                    stream: Arc::clone(&name),
                    records,
                });
            }
            if again {
                self.dirty.push(name);
            }
        }
        self.in_flight.insert(tag, touched);
        self.uploads_out.push_back(UploadOut::Section {
            seal,
            section: Section { extents },
        });
        self.flush_uploads();
    }

    /// A stream that filled its own buffer gets its own object (section 7.3, D58).
    fn send_dedicated(&mut self, name: &Arc<str>) {
        let number = ((self.config.id as u64) << 48) | self.dedicated_count;
        let tag = UploadTag::Dedicated(number);
        let Some(log) = self.logs.get_mut(&**name) else {
            return;
        };
        let (records, taken_bytes) = log.take_unsealed(tag, self.sent_ticket);
        if records.is_empty() {
            return;
        }
        self.dedicated_count += 1;
        self.unsealed_bytes -= taken_bytes;
        self.in_flight.insert(tag, vec![Arc::clone(name)]);
        self.uploads_out.push_back(UploadOut::Dedicated {
            stream: Arc::clone(name),
            number,
            records,
        });
        self.flush_uploads();
    }

    fn on_upload_event(&mut self, event: UploadEvent) {
        let (key, uploaded) = match event {
            UploadEvent::Uploaded {
                key, path, extents, ..
            } => (key, Some((path, extents))),
            UploadEvent::Failed { key, .. } => (key, None),
            UploadEvent::Empty { key } => {
                if let UploadKey::Seal(seal) = key {
                    self.in_flight.remove(&UploadTag::Seal(seal));
                }
                return;
            }
            UploadEvent::Retrying { .. } => return,
        };
        let tag = match key {
            UploadKey::Seal(seal) => UploadTag::Seal(seal),
            UploadKey::Dedicated { number, .. } => UploadTag::Dedicated(number),
        };
        let Some((path, extents)) = uploaded else {
            // The uploader keeps the references and level 3 stays unacked, since the
            // records aren't in the store (D55).
            if self.in_flight.contains_key(&tag) {
                self.upload_failed_count += 1;
            }
            return;
        };
        let Some(names) = self.in_flight.remove(&tag) else {
            return;
        };
        for name in &names {
            if let Some(log) = self.logs.get_mut(&**name) {
                log.complete(tag);
            }
        }
        self.note_extents(tag, &names, &path, &extents);
    }

    fn on_control(&mut self, message: ControlMsg) {
        if self.loader.is_some()
            && let Some(name) = control_stream(&message)
            && matches!(self.route(&name), Route::Here(_))
        {
            if self.needs_load(&name) {
                self.park(&name, Parked::Control(message));
                return;
            }
            self.touch(&name);
        }
        match message {
            ControlMsg::Run(work) => (work.0)(),
            ControlMsg::StreamInfo { stream, reply } => match self.route(&stream) {
                Route::Forward(to) => {
                    let _ = self.links.control[to].send(ControlMsg::StreamInfo { stream, reply });
                }
                Route::Here(_) => {
                    let result = self
                        .streams
                        .get(&*stream)
                        .map(|stream| StreamInfo {
                            head: stream.delivery.visible_head(stream.ids.last_id()),
                            state_ver: stream.delivery.visible_state.0,
                            config: stream.config,
                            sinks: stream
                                .delivery
                                .sinks
                                .iter()
                                .map(|(id, spec)| {
                                    let cursor = stream
                                        .delivery
                                        .cursors
                                        .get(sink_cursor_name(*id).as_str())
                                        .copied();
                                    (*id, spec.clone(), cursor)
                                })
                                .collect(),
                        })
                        .ok_or(ErrorCode::NotFound);
                    let _ = reply.send(result);
                }
            },
            ControlMsg::ConfigurePatch {
                stream,
                patch,
                reply,
            } => match self.route(&stream) {
                Route::Forward(to) => {
                    let _ = self.links.control[to].send(ControlMsg::ConfigurePatch {
                        stream,
                        patch,
                        reply,
                    });
                }
                Route::Here(_) if &*stream == MANIFEST_STREAM => {
                    let config = self.streams.get(&*stream).map(|known| known.config);
                    let _ = reply.send(config.unwrap_or_default());
                }
                Route::Here(shard) => {
                    let config = &mut self.stream_entry(&stream, shard).config;
                    if let Some(level) = patch.default_ack_level {
                        config.default_ack_level = level;
                    }
                    if let Some(level) = patch.min_ack_level {
                        config.min_ack_level = level;
                    }
                    if let Some(retention_ms) = patch.retention_ms {
                        config.retention_ms = retention_ms;
                    }
                    let config = *config;
                    self.mark_dirty(&stream);
                    let _ = reply.send(config);
                }
            },
            ControlMsg::DeleteStream { stream, reply } => match self.route(&stream) {
                Route::Forward(to) => {
                    let _ = self.links.control[to].send(ControlMsg::DeleteStream { stream, reply });
                }
                Route::Here(_) if &*stream == MANIFEST_STREAM => {
                    let _ = reply.send(false);
                }
                Route::Here(shard) => {
                    let deleted = self.on_delete_stream(&stream, shard);
                    if deleted && self.loader.is_some() {
                        let _ = self.to_node.send(CoreToNode::DurableRequested(reply));
                    } else {
                        let _ = reply.send(deleted);
                    }
                }
            },
            ControlMsg::Sink { stream, op, reply } => match self.route(&stream) {
                Route::Forward(to) => {
                    let _ = self.links.control[to].send(ControlMsg::Sink { stream, op, reply });
                }
                Route::Here(_) => {
                    let result = self.on_sink(&stream, op);
                    self.mark_dirty(&stream);
                    let _ = reply.send(result);
                }
            },
            ControlMsg::Ping { reply } => {
                let _ = reply.send(self.config.id);
            }
            ControlMsg::Snapshot { stream, reply } => match self.route(&stream) {
                Route::Forward(to) => {
                    let _ = self.links.control[to].send(ControlMsg::Snapshot { stream, reply });
                }
                Route::Here(_) => {
                    let result = self
                        .streams
                        .get(&*stream)
                        .map(|stream| SnapshotReply {
                            head: stream.delivery.visible_head(stream.ids.last_id()),
                            state_ver: stream.delivery.visible_state.0,
                            state: stream.delivery.visible_state.1.clone(),
                        })
                        .ok_or(ErrorCode::NotFound);
                    let _ = reply.send(result);
                }
            },
            ControlMsg::ReadMemory { stream, reply } => {
                let batches = self
                    .logs
                    .get(&*stream)
                    .map(|log| log.batches.iter().map(|b| b.bytes.clone()).collect())
                    .unwrap_or_default();
                let _ = reply.send(batches);
            }
            ControlMsg::Configure {
                stream,
                config,
                reply,
            } => match self.route(&stream) {
                Route::Forward(to) => {
                    let _ = self.links.control[to].send(ControlMsg::Configure {
                        stream,
                        config,
                        reply,
                    });
                }
                Route::Here(shard) => {
                    self.stream_entry(&stream, shard).config = config;
                    self.mark_dirty(&stream);
                    let _ = reply.send(());
                }
            },
            ControlMsg::Seal { seal } => self.on_seal(seal),
            ControlMsg::MoveShard { shard, to, done } => self.on_move_shard(shard, to, done),
            ControlMsg::SplitStream {
                stream,
                new_shard,
                to,
                done,
            } => self.on_split_stream(stream, new_shard, to, done),
            ControlMsg::Adopt(handoff) => self.on_adopt(*handoff),
            ControlMsg::ReportLoad { reply } => {
                let mut loads = Vec::with_capacity(self.shards.len());
                for (id, shard) in &mut self.shards {
                    shard.counters.sample();
                    loads.push((*id, shard.counters.load()));
                }
                let _ = reply.send(loads);
            }
            ControlMsg::Stats { reply } => {
                let _ = reply.send(CoreStats {
                    core: self.config.id,
                    stream_count: self.streams.len(),
                    outbound_count: self.outbound.len(),
                    wal_stopped: self.wal_stopped,
                    upload_failed_count: self.upload_failed_count,
                    uploads_waiting_count: self.uploads_out.len(),
                    unsealed_bytes: self.unsealed_bytes,
                    duplicate_count: self.duplicate_count,
                    forwarded_count: self.forwarded_count,
                    filter_check_count: self.filter_check_count,
                    behind_count: self.home.stats.behind_count,
                    stalled_release_count: self.home.stats.stalled_release_count,
                    progress_count: self.home.stats.progress_count,
                    catch_up_blocks_read_count: self.home.stats.blocks_read_count,
                    catch_up_blocks_skipped_count: self.home.stats.blocks_skipped_count,
                    catch_up_memory_bytes: self.home.stats.memory_bytes,
                    catch_up_local_bytes: self.home.stats.local_bytes,
                    catch_up_object_bytes: self.home.stats.object_bytes,
                    evicted_count: self.evicted_count,
                    loaded_count: self.loaded_count,
                    rebuilt_count: self.rebuilt_count,
                });
            }
            ControlMsg::AckWhenReached {
                stream,
                last_id,
                level,
                ack,
                reply,
            } => self.ack_local(&stream, last_id, level, ack, reply),
            ControlMsg::Conn(command) => self.home.on_command(command),
            ControlMsg::Attach(message) => self.on_attach(*message),
            ControlMsg::AttachWalk(message) => self.on_attach_walk(message),
            ControlMsg::Detach {
                stream,
                filter_id,
                home,
            } => self.on_detach(stream, filter_id, home),
            ControlMsg::Cursor {
                stream,
                name,
                op,
                report,
                ticket,
            } => self.on_cursor(stream, name, op, report, ticket),
            ControlMsg::AddExtents {
                stream,
                path,
                extents,
                origin,
                tag,
            } => {
                if let Route::Forward(to) = self.route(&stream) {
                    let _ = self.links.control[to].send(ControlMsg::AddExtents {
                        stream,
                        path,
                        extents,
                        origin,
                        tag,
                    });
                    return;
                }
                if let Some(known) = self.streams.get_mut(&*stream) {
                    for extent in &extents {
                        known.delivery.add_extent(&path, extent);
                    }
                }
                if origin == self.config.id {
                    self.note_batches(&stream, tag);
                } else {
                    let _ =
                        self.links.control[origin].send(ControlMsg::ExtentsNoted { stream, tag });
                }
            }
            ControlMsg::ExtentsNoted { stream, tag } => self.note_batches(&stream, tag),
            ControlMsg::PublishManifest { doc, reply } => self.on_publish_manifest(doc, reply),
            ControlMsg::CompactView {
                stream,
                path,
                footer,
                reply,
            } => {
                if let Route::Forward(to) = self.route(&stream) {
                    let _ = self.links.control[to].send(ControlMsg::CompactView {
                        stream,
                        path,
                        footer,
                        reply,
                    });
                    return;
                }
                let _ = reply.send(self.compact_view(&stream, &path, &footer));
            }
            ControlMsg::ReplaceExtents {
                stream,
                path,
                sources,
                extent,
                reply,
            } => {
                if let Route::Forward(to) = self.route(&stream) {
                    let _ = self.links.control[to].send(ControlMsg::ReplaceExtents {
                        stream,
                        path,
                        sources,
                        extent,
                        reply,
                    });
                    return;
                }
                let merge_below = self.merge_below(&stream);
                let result = self
                    .streams
                    .get_mut(&*stream)
                    .map_or(ReplaceResult::Gone, |known| {
                        known
                            .delivery
                            .replace_extents(&path, &sources, &extent, merge_below)
                    });
                let _ = reply.send(result);
            }
            ControlMsg::DropExtents {
                stream,
                path,
                reply,
            } => {
                if let Route::Forward(to) = self.route(&stream) {
                    let _ = self.links.control[to].send(ControlMsg::DropExtents {
                        stream,
                        path,
                        reply,
                    });
                    return;
                }
                let dropped = self
                    .streams
                    .get_mut(&*stream)
                    .is_some_and(|known| known.delivery.drop_path(&path) > 0);
                let _ = reply.send(dropped);
            }
            ControlMsg::DurableSeen { core, ticket } => {
                self.durable_by_core[core] = self.durable_by_core[core].max(ticket);
                self.drain_foreign_visible();
            }
            ControlMsg::ProbeBack => self.on_probe_back(),
            ControlMsg::WalSent { core, ticket } => {
                self.wal_sent_by_core[core] = self.wal_sent_by_core[core].max(ticket);
            }
            ControlMsg::Restore(restored) => self.on_restore(*restored),
            ControlMsg::CheckpointClean { seal, reply } => {
                let _ = reply.send(self.is_clean(seal));
            }
            ControlMsg::CheckpointGather { gather, reply } => {
                let _ = reply.send(self.gather(gather));
            }
            ControlMsg::CheckpointDone { gather } => self.evict_idle(gather),
            ControlMsg::Loaded {
                stream,
                ticket,
                meta,
                rebuilt,
            } => self.on_loaded(stream, ticket, meta, rebuilt),
            ControlMsg::Stop => {}
        }
    }

    fn mark_dirty(&mut self, name: &str) {
        if let Some(stream) = self.streams.get_mut(name) {
            stream.delivery.meta_dirty = stream.delivery.track_meta;
        }
    }

    fn touch(&mut self, name: &str) {
        if let Some(stream) = self.streams.get_mut(name) {
            self.use_count += 1;
            stream.delivery.last_used = self.use_count;
        }
    }

    /// Whether `name` could be out of RAM, on a core that owns it. Asks for it if nobody has.
    /// Only the first touch of an idle stream pays for this, off the core (section 7.6).
    fn needs_load(&mut self, name: &Arc<str>) -> bool {
        let Some(loader) = &self.loader else {
            return false;
        };
        if self.streams.contains_key(&**name)
            || self.tombstones.contains_key(&**name)
            || self.load_passed.as_deref() == Some(&**name)
        {
            return false;
        }
        if !self.loading.contains_key(&**name) {
            self.load_count += 1;
            let ticket = self.load_count;
            let _ = loader.send(MetaMsg::Load {
                core: self.config.id,
                stream: Arc::clone(name),
                ticket,
            });
            self.loading.insert(
                Arc::clone(name),
                Loading {
                    ticket,
                    parked: Vec::new(),
                },
            );
        }
        true
    }

    fn park(&mut self, name: &Arc<str>, message: Parked) {
        if let Some(loading) = self.loading.get_mut(&**name) {
            if matches!(message, Parked::Write(_)) {
                self.parked_write_count += 1;
            }
            loading.parked.push(message);
        }
    }

    fn on_loaded(
        &mut self,
        name: Arc<str>,
        ticket: u64,
        meta: Option<Box<StreamMeta>>,
        rebuilt: bool,
    ) {
        if self
            .loading
            .get(&*name)
            .is_none_or(|loading| loading.ticket != ticket)
        {
            // It moved away while this was out. Its new owner asks for itself.
            return;
        }
        let loading = self.loading.remove(&*name).expect("just looked at it");
        let shard = match self.route(&name) {
            Route::Here(shard) => Some(shard),
            Route::Forward(_) => None,
        };
        if let (Some(meta), Some(shard), false) = (meta, shard, self.streams.contains_key(&*name)) {
            self.loaded_count += 1;
            self.rebuilt_count += u64::from(rebuilt);
            if meta.deleted {
                self.tombstones.insert(Arc::clone(&name), meta.last_id);
            } else {
                self.install_meta(&name, *meta, shard);
            }
        }
        self.run_parked(&name, loading.parked);
    }

    /// In the order they came. Writes go ahead of what is deferred, which came later.
    fn run_parked(&mut self, name: &Arc<str>, parked: Vec<Parked>) {
        self.load_passed = Some(Arc::clone(name));
        let mut writes = Vec::new();
        for message in parked {
            match message {
                Parked::Write(write) => {
                    self.parked_write_count -= 1;
                    writes.push(write);
                }
                Parked::Control(control) => self.on_control(control),
                Parked::Read(read) => self.on_read(read),
            }
        }
        for write in writes.into_iter().rev() {
            self.deferred.push_front(write);
        }
        self.run_deferred();
        self.load_passed = None;
    }

    /// A shard just left: what waited for its streams to load follows it, in order.
    fn pass_on_parked(&mut self) {
        let leaving: Vec<Arc<str>> = self
            .loading
            .keys()
            .filter(|name| matches!(self.route(name), Route::Forward(_)))
            .cloned()
            .collect();
        for name in leaving {
            if let Some(loading) = self.loading.remove(&*name) {
                self.run_parked(&name, loading.parked);
            }
        }
    }

    /// A seal manifest is a system record, so nothing a client sends can look like one
    /// (sections 2.2, 7.5).
    fn on_publish_manifest(&mut self, doc: Vec<u8>, reply: ReplyTx) {
        let name: Arc<str> = Arc::from(MANIFEST_STREAM);
        let shard = match self.route(&name) {
            Route::Forward(to) => {
                let _ = self.links.control[to].send(ControlMsg::PublishManifest { doc, reply });
                return;
            }
            Route::Here(shard) => shard,
        };
        if self.wal_stopped {
            return;
        }
        let now_ms = self.clock.now_ms();
        let stream = self.stream_entry(&name, shard);
        // Its objects stay until every worker has passed them, so time doesn't limit a reader.
        stream.config.retention_ms = u64::MAX;
        let Ok(id) = stream.ids.allocate(now_ms, 1) else {
            return;
        };
        let mut system = Vec::with_capacity(HEADER_BYTES + ACTOR_BYTES + doc.len() + 8);
        encode_system_record(&mut system, id, ENTRY_SEAL_MANIFEST, 0, &doc)
            .expect("a path is far under the length limit");
        let mut region = self.pool.alloc(system.len());
        region.copy_from_slice(&system);
        self.commit(
            &name,
            CommitIds {
                producer_id: 0,
                producer_seq: 0,
                first_id: id,
                last_id: id,
                record_count: 1,
                state_after: None,
            },
            region.freeze(),
            2,
            Waiter {
                reply: Reply::Ack {
                    handle: 0,
                    producer_seq: 0,
                    first_id: id,
                    count: 1,
                },
                tx: reply,
            },
        );
    }

    /// Records at or above this may have no extent yet, here or on an earlier owner.
    fn merge_below(&self, name: &str) -> u64 {
        let elsewhere = self
            .streams
            .get(name)
            .is_some_and(|stream| !stream.delivery.held_elsewhere.is_empty());
        if elsewhere {
            return 0;
        }
        self.logs
            .get(name)
            .and_then(|log| log.batches.front())
            .map_or(u64::MAX, |batch| batch.first_id)
    }

    fn compact_view(
        &self,
        name: &str,
        path: &object_store::path::Path,
        footer: &[(u64, u64)],
    ) -> Option<CompactView> {
        let stream = self.streams.get(name)?;
        let delivery = &stream.delivery;
        let unnoted = footer.iter().any(|(id_first, id_last)| {
            *id_last > delivery.floor_id
                && delivery
                    .first_extent_after(id_first.saturating_sub(1))
                    .is_none_or(|known| known.extent.id_first > *id_last)
        });
        let sources = delivery
            .extents
            .iter()
            .enumerate()
            .filter(|(_, known)| known.path == *path)
            .map(|(at, known)| CompactSource {
                extent: known.extent.clone(),
                next_id_first: delivery
                    .extents
                    .get(at + 1)
                    .map(|next| next.extent.id_first),
            })
            .collect();
        Some(CompactView {
            retention_ms: stream.config.retention_ms,
            unnoted,
            sources,
            merge_below: self.merge_below(name),
        })
    }

    fn install_meta(&mut self, name: &Arc<str>, meta: StreamMeta, shard: u32) {
        let mut delivery = StreamDelivery::new(self.config.core_count);
        delivery.track_meta = true;
        delivery.floor_id = meta.floor_id;
        delivery.visible_through = meta.last_id;
        delivery.visible_state = (meta.state_ver, meta.state.clone());
        delivery.cursors = meta
            .cursors
            .iter()
            .map(|cursor| (Arc::from(cursor.name.as_str()), cursor.id))
            .collect();
        delivery.extents = meta
            .extents
            .iter()
            .map(|extent| crate::recovery::extent_ref(name, extent))
            .collect();
        delivery.sinks = meta
            .sinks
            .iter()
            .map(|sink| (sink.sink_id, crate::recovery::sink_spec(sink)))
            .collect();
        let dedupe = DedupeTable::from_entries(meta.dedupe.iter().map(|entry| {
            (
                entry.producer_id,
                entry.producer_seq,
                entry.first_id,
                entry.count,
            )
        }));
        delivery.saved_through = meta.last_id;
        delivery.saved_state = (meta.state_ver, meta.state.clone());
        delivery.saved_dedupe = dedupe.clone();
        self.use_count += 1;
        delivery.last_used = self.use_count;
        let mut state = StreamState::new();
        state.install(meta.state, meta.state_ver);
        self.shards
            .entry(shard)
            .or_default()
            .streams
            .insert(Arc::clone(name));
        self.tombstones.remove(&**name);
        self.streams.insert(
            Arc::clone(name),
            Stream {
                name: Arc::clone(name),
                shard,
                ids: IdAllocator::new(meta.last_id.max(meta.floor_id), self.config.last_valid_id),
                state,
                dedupe,
                config: StreamConfig {
                    default_ack_level: meta.default_ack_level,
                    min_ack_level: meta.min_ack_level,
                    retention_ms: meta.retention_ms,
                },
                owners: Vec::new(),
                delivery,
            },
        );
    }

    /// A stream as the startup put it back. What its WAL entries hold and no extent covers
    /// goes into the next seal, under this generation (D16).
    fn on_restore(&mut self, restored: crate::recovery::RestoredStream) {
        let name: Arc<str> = Arc::from(restored.meta.name.as_str());
        let shard = match self.route(&name) {
            Route::Here(shard) => shard,
            Route::Forward(_) => return,
        };
        self.install_meta(&name, restored.meta, shard);
        let stream = self.streams.get_mut(&*name).expect("just installed");
        stream.delivery.meta_dirty = true;
        stream.delivery.new_extents = restored
            .new_extents
            .iter()
            .map(|extent| crate::recovery::extent_ref(&name, extent))
            .collect();
        if restored.batches.is_empty() {
            return;
        }
        let log = self.logs.entry(Arc::clone(&name)).or_default();
        let mut previous_last_id = None;
        for batch in restored.batches {
            // One extent can't go around another's records: take these in separate seals.
            if previous_last_id.is_some_and(|last_id| {
                stream.delivery.extents.iter().any(|known| {
                    last_id < known.extent.id_first && known.extent.id_first < batch.first_id
                })
            }) {
                log.residency += 1;
            }
            previous_last_id = Some(batch.last_id);
            let bytes = self.pool.copy_in(&batch.bytes).freeze();
            log.unsealed_bytes += bytes.len() as u64;
            self.unsealed_bytes += bytes.len() as u64;
            log.batches.push_back(Batch {
                bytes,
                first_id: batch.first_id,
                last_id: batch.last_id,
                // Already durable, in a closed file: nothing waits on its ticket.
                ticket: 0,
                residency: log.residency,
                upload: BatchUpload::Unsealed,
                waiters: Vec::new(),
            });
        }
        log.dirty = true;
        self.dirty.push(name);
    }

    /// Whether everything sent to the WAL before this seal's rotation is uploaded, at its
    /// owner, and visible. Only then may the files holding it go (D137).
    fn is_clean(&mut self, seal: u64) -> bool {
        // The uploader tells cores before it tells control, so this seal's event is here.
        while let Ok(event) = self.inbox.uploads.try_recv() {
            self.on_upload_event(event);
        }
        self.on_durable();
        let Some(through) = self.seal_sent.remove(&seal) else {
            return false;
        };
        if !self.foreign_visible.is_empty()
            || self
                .wal_visible
                .front()
                .is_some_and(|(ticket, _)| *ticket <= through)
        {
            return false;
        }
        !self.logs.values().any(|log| {
            log.batches
                .front()
                .is_some_and(|batch| batch.ticket <= through)
        })
    }

    /// What changed, as of each stream's last visible record. A change above that comes back
    /// from the WAL, or never happened (D136).
    fn gather(&mut self, gather: u64) -> Vec<StreamMeta> {
        let mut metas = Vec::new();
        for stream in self.streams.values_mut() {
            let delivery = &mut stream.delivery;
            if !delivery.meta_dirty {
                continue;
            }
            delivery.meta_dirty = false;
            delivery.saved_gather = gather;
            metas.push(StreamMeta {
                name: stream.name.to_string(),
                deleted: false,
                last_id: stream.ids.last_id(),
                floor_id: delivery.floor_id,
                applied_id: delivery.saved_through,
                state_ver: delivery.saved_state.0,
                state: delivery.saved_state.1.clone(),
                default_ack_level: stream.config.default_ack_level,
                min_ack_level: stream.config.min_ack_level,
                retention_ms: stream.config.retention_ms,
                cursors: delivery
                    .cursors
                    .iter()
                    .map(|(name, id)| MetaCursor {
                        name: name.to_string(),
                        id: *id,
                    })
                    .collect(),
                dedupe: delivery
                    .saved_dedupe
                    .entries()
                    .iter()
                    .map(|entry| MetaDedupe {
                        producer_id: entry.producer_id(),
                        producer_seq: entry.producer_seq(),
                        first_id: entry.first_id(),
                        count: entry.count(),
                    })
                    .collect(),
                sinks: delivery
                    .sinks
                    .iter()
                    .map(|(id, spec)| crate::recovery::meta_sink(*id, spec))
                    .collect(),
                extents: delivery
                    .new_extents
                    .drain(..)
                    .map(|known| crate::recovery::meta_extent(&known))
                    .collect(),
                dropped_extents: std::mem::take(&mut delivery.dropped_extents),
            });
        }
        for (name, last_id) in self.dirty_tombstones.drain(..) {
            metas.push(StreamMeta {
                name: name.to_string(),
                deleted: true,
                last_id,
                floor_id: last_id,
                applied_id: last_id,
                ..StreamMeta::default()
            });
        }
        metas
    }

    /// Cold streams leave RAM, oldest first, once the store has all of what they hold. A live
    /// subscriber, or anything still on its way, keeps one here (section 7.6).
    fn evict_idle(&mut self, done_gather: u64) {
        let Some(limit) = self.config.active_stream_limit else {
            return;
        };
        let over = self.streams.len().saturating_sub(limit);
        if over == 0 {
            return;
        }
        let mut cold: Vec<(u64, Arc<str>)> = self
            .streams
            .values()
            .filter(|stream| {
                let delivery = &stream.delivery;
                !delivery.meta_dirty
                    && delivery.saved_gather <= done_gather
                    && delivery.pending.is_empty()
                    && delivery.unsaved.is_empty()
                    && delivery.held_elsewhere.is_empty()
                    && delivery.table.is_unwatched()
                    && self
                        .logs
                        .get(&stream.name)
                        .is_none_or(|log| log.batches.is_empty())
                    && !self.wal_waits.iter().any(|wait| wait.stream == stream.name)
            })
            .map(|stream| (stream.delivery.last_used, Arc::clone(&stream.name)))
            .collect();
        cold.sort_unstable();
        for (_, name) in cold.into_iter().take(over) {
            let Some(mut stream) = self.streams.remove(&*name) else {
                continue;
            };
            self.logs.remove(&*name);
            if let Some(shard) = self.shards.get_mut(&stream.shard) {
                shard.streams.remove(&*name);
            }
            for home in 0..self.config.core_count {
                if stream.delivery.seq_by_home[home] == 0 {
                    continue;
                }
                let seq = next_seq(&mut stream.delivery, home);
                if home == self.config.id {
                    self.home
                        .on_stream(Arc::clone(&name), seq, StreamBody::Evicted);
                } else {
                    let _ = self.links.delivery[home].send(DeliveryMsg::Stream {
                        stream: Arc::clone(&name),
                        seq,
                        body: StreamBody::Evicted,
                    });
                }
            }
            self.evicted_count += 1;
        }
    }

    fn package_stream(&mut self, name: &str) -> Option<StreamHandoff> {
        let mut stream = self.streams.remove(name)?;
        let last_id = stream.ids.last_id();
        if last_id > 0 {
            stream.owners.push((last_id, self.config.id));
        }
        if let Some(batch) = self.logs.get(name).and_then(|log| log.batches.back()) {
            stream.delivery.held_elsewhere.push(batch.last_id);
        }
        // The new owner hears when what it takes over from here becomes durable (D84).
        let durable_ticket = *self.inbox.durable.borrow();
        let track_meta = stream.delivery.track_meta;
        for pending in &stream.delivery.pending {
            if (pending.needs_wal || track_meta) && pending.core == self.config.id {
                self.durable_owed = self.durable_owed.max(pending.ticket);
            }
        }
        for step in &stream.delivery.unsaved {
            if step.core == self.config.id {
                self.durable_owed = self.durable_owed.max(step.ticket);
            }
        }
        if self.durable_owed != 0 && durable_ticket >= self.durable_owed {
            self.tell_durable_if_owed();
        }
        let wal_waits = self.wal_waits_for_handoff(name);
        Some(StreamHandoff {
            name: stream.name,
            ids: stream.ids,
            state: stream.state,
            dedupe: stream.dedupe,
            config: stream.config,
            owners: stream.owners,
            wal_waits,
            delivery: stream.delivery,
        })
    }

    /// Its records stop being reachable and age out with their prefixes (D20), and a stream
    /// made under this name later has higher ids and can't read them.
    fn on_delete_stream(&mut self, name: &Arc<str>, shard: u32) -> bool {
        let Some(mut stream) = self.streams.remove(&**name) else {
            return false;
        };
        if let Some(shard) = self.shards.get_mut(&shard) {
            shard.streams.remove(&**name);
        }
        // Everything at or below the last valid id is older than this delete, though a former
        // generation's late object can hold ids of it this stream never had (section 8.1).
        let floor_id = stream.ids.last_id().max(self.config.last_valid_id);
        self.tombstones.insert(Arc::clone(name), floor_id);
        if self.loader.is_some() {
            self.dirty_tombstones.push((Arc::clone(name), floor_id));
        }
        for home in 0..self.config.core_count {
            let seq = next_seq(&mut stream.delivery, home);
            if home == self.config.id {
                self.home
                    .on_stream(Arc::clone(name), seq, StreamBody::Deleted);
            } else {
                let _ = self.links.delivery[home].send(DeliveryMsg::Stream {
                    stream: Arc::clone(name),
                    seq,
                    body: StreamBody::Deleted,
                });
            }
        }
        true
    }

    fn on_sink(&mut self, name: &str, op: SinkOp) -> Result<(), ErrorCode> {
        let stream = self.streams.get_mut(name).ok_or(ErrorCode::NotFound)?;
        let head = stream.delivery.visible_head(stream.ids.last_id());
        let cursor_at = |start: SinkStart| match start {
            SinkStart::Now => head,
            SinkStart::Earliest => 0,
            SinkStart::Id(id) => id,
        };
        let delivery = &mut stream.delivery;
        match op {
            SinkOp::Put {
                sink_id,
                mut spec,
                start,
            } => {
                if let Some(existing) = delivery.sinks.get(&sink_id) {
                    spec.paused = existing.paused;
                } else {
                    delivery
                        .cursors
                        .insert(Arc::from(sink_cursor_name(sink_id)), cursor_at(start));
                }
                delivery.sinks.insert(sink_id, spec);
            }
            SinkOp::Delete { sink_id } => {
                delivery.sinks.remove(&sink_id).ok_or(ErrorCode::NotFound)?;
                delivery.cursors.remove(sink_cursor_name(sink_id).as_str());
            }
            SinkOp::SetPaused { sink_id, paused } => {
                delivery
                    .sinks
                    .get_mut(&sink_id)
                    .ok_or(ErrorCode::NotFound)?
                    .paused = paused;
            }
            SinkOp::Reset { sink_id, start } => {
                if !delivery.sinks.contains_key(&sink_id) {
                    return Err(ErrorCode::NotFound);
                }
                delivery
                    .cursors
                    .insert(Arc::from(sink_cursor_name(sink_id)), cursor_at(start));
            }
        }
        Ok(())
    }

    /// The move's handoff: everything goes in one message, the shard is someone else's
    /// from here, and later writes follow it. Batches and acks waiting on them stay.
    fn on_move_shard(&mut self, shard: u32, to: usize, done: oneshot::Sender<()>) {
        if to == self.config.id {
            let _ = done.send(());
            return;
        }
        let state = self.shards.remove(&shard).unwrap_or_default();
        self.location[shard as usize] = to as u16;
        let streams = state
            .streams
            .iter()
            .filter_map(|name| self.package_stream(name))
            .collect();
        // What this shard's new owner has to answer for: entries for streams that hash
        // here, and for streams living in it.
        let overrides = self
            .overrides
            .iter()
            .filter(|(name, own_shard)| {
                **own_shard == shard || shard_of(name, self.config.virtual_shard_count) == shard
            })
            .map(|(name, own_shard)| {
                (
                    Arc::clone(name),
                    *own_shard,
                    self.location[*own_shard as usize],
                )
            })
            .collect();
        let tombstones: Vec<(Arc<str>, u64)> = self
            .tombstones
            .iter()
            .filter(|(name, _)| {
                self.overrides
                    .get(&***name)
                    .copied()
                    .unwrap_or_else(|| shard_of(name, self.config.virtual_shard_count))
                    == shard
            })
            .map(|(name, last_id)| (Arc::clone(name), *last_id))
            .collect();
        for (name, _) in &tombstones {
            self.tombstones.remove(&**name);
        }
        let _ = self.links.control[to].send(ControlMsg::Adopt(Box::new(ShardHandoff {
            shard,
            streams,
            counters: state.counters,
            overrides,
            tombstones,
            done,
        })));
        self.pass_on_deferred();
        self.pass_on_parked();
    }

    fn on_split_stream(
        &mut self,
        name: Arc<str>,
        new_shard: u32,
        to: usize,
        done: oneshot::Sender<()>,
    ) {
        if let Route::Forward(next) = self.route(&name) {
            let _ = self.links.control[next].send(ControlMsg::SplitStream {
                stream: name,
                new_shard,
                to,
                done,
            });
            return;
        }
        let old_shard = self.streams.get(&*name).map(|stream| stream.shard);
        if let Some(shard) = old_shard.and_then(|shard| self.shards.get_mut(&shard)) {
            shard.streams.remove(&*name);
        }
        self.overrides.insert(Arc::clone(&name), new_shard);
        self.location[new_shard as usize] = to as u16;
        if to == self.config.id {
            let shard = self.shards.entry(new_shard).or_default();
            if old_shard.is_some() {
                shard.streams.insert(Arc::clone(&name));
            }
            if let Some(stream) = self.streams.get_mut(&*name) {
                stream.shard = new_shard;
            }
            let _ = done.send(());
            return;
        }
        let streams = self.package_stream(&name).into_iter().collect();
        let tombstones = self.tombstones.remove_entry(&*name).into_iter().collect();
        let _ = self.links.control[to].send(ControlMsg::Adopt(Box::new(ShardHandoff {
            shard: new_shard,
            streams,
            counters: ShardCounters::default(),
            overrides: vec![(name, new_shard, to as u16)],
            tombstones,
            done,
        })));
        self.pass_on_deferred();
        self.pass_on_parked();
    }

    fn on_adopt(&mut self, handoff: ShardHandoff) {
        let me = self.config.id as u16;
        let shard_id = handoff.shard;
        // Where this core has been forwarding, read before anything here changes it.
        let mut probes = Vec::new();
        let mut rerouted = Vec::new();
        let old_hop = self.location[shard_id as usize];
        if old_hop != u16::MAX && old_hop != me {
            probes.push((old_hop as usize, ProbeTarget::Shard(shard_id)));
        }
        for (name, _, _) in &handoff.overrides {
            if !self.overrides.contains_key(name)
                && let Route::Forward(hop) = self.route(name)
            {
                rerouted.push(Arc::clone(name));
                probes.push((hop, ProbeTarget::Stream(Arc::clone(name))));
            }
        }
        self.location[shard_id as usize] = me;
        for (name, own_shard, at) in handoff.overrides {
            self.overrides.insert(name, own_shard);
            // The sender's view may be old, and this core may since have had the shard. What
            // a core learned by holding a shard is newer than anything it can be told (D66).
            let known = &mut self.location[own_shard as usize];
            if *known == u16::MAX {
                *known = at;
            }
        }
        self.tombstones.extend(handoff.tombstones);
        let shard = self.shards.entry(shard_id).or_default();
        shard.counters = handoff.counters;
        for stream in handoff.streams {
            shard.streams.insert(Arc::clone(&stream.name));
            if let Some(log) = self.logs.get_mut(&*stream.name) {
                log.residency += 1;
            }
            // Anything already sent is dropped by the next `clear_wal_waits`.
            for (core, ticket) in stream.wal_waits {
                self.wal_waits.push(WalWait {
                    stream: Arc::clone(&stream.name),
                    core,
                    ticket,
                    stamped_through: self.next_ticket,
                });
            }
            if !stream.delivery.pending.is_empty() || !stream.delivery.unsaved.is_empty() {
                self.foreign_visible.push(Arc::clone(&stream.name));
            }
            self.streams.insert(
                Arc::clone(&stream.name),
                Stream {
                    name: stream.name,
                    shard: shard_id,
                    ids: stream.ids,
                    state: stream.state,
                    dedupe: stream.dedupe,
                    config: stream.config,
                    owners: stream.owners,
                    delivery: stream.delivery,
                },
            );
        }
        self.drain_foreign_visible();
        let mut probes_out_count = 0;
        for (hop, target) in probes {
            let origin = self.config.id;
            if self.links.forwarded[hop]
                .send(WriteMsg::Probe { target, origin })
                .is_ok()
            {
                probes_out_count += 1;
            }
        }
        if probes_out_count == 0 {
            let _ = handoff.done.send(());
            return;
        }
        self.hold = Some(Hold {
            shard: shard_id,
            streams: rerouted,
            probes_out_count,
            returned: Vec::new(),
            later: VecDeque::new(),
            done: handoff.done,
        });
    }
}

/// The stream a message needs in RAM before it can be handled, if any.
fn control_stream(message: &ControlMsg) -> Option<Arc<str>> {
    match message {
        ControlMsg::StreamInfo { stream, .. }
        | ControlMsg::ConfigurePatch { stream, .. }
        | ControlMsg::DeleteStream { stream, .. }
        | ControlMsg::Sink { stream, .. }
        | ControlMsg::Snapshot { stream, .. }
        | ControlMsg::Configure { stream, .. }
        | ControlMsg::Cursor { stream, .. }
        | ControlMsg::CompactView { stream, .. }
        | ControlMsg::ReplaceExtents { stream, .. }
        | ControlMsg::DropExtents { stream, .. }
        | ControlMsg::AddExtents { stream, .. } => Some(Arc::clone(stream)),
        ControlMsg::PublishManifest { .. } => Some(Arc::from(MANIFEST_STREAM)),
        ControlMsg::Attach(message) => Some(Arc::clone(&message.stream)),
        _ => None,
    }
}

struct CommitIds {
    producer_id: u64,
    producer_seq: u64,
    first_id: u64,
    last_id: u64,
    record_count: u64,
    state_after: Option<(u64, Bytes)>,
}

async fn sleep_until_some(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// The lowest id this core holds above `after`, in batches ending by `through`. Ids within a
/// batch run without gaps.
fn next_id_in_log(log: &StreamLog, after: u64, through: u64) -> Option<u64> {
    log.read_after(after, through, 1)
        .first()
        .map(|batch| first_id(batch).max(after + 1))
}

/// The named cursor of section 9.
fn sink_cursor_name(sink_id: u64) -> String {
    format!("sink:{sink_id}")
}

fn first_id(batch: &SlabBytes) -> u64 {
    format::RecordHeader::read(batch).map_or(0, |header| header.id)
}

fn next_seq(delivery: &mut StreamDelivery, home: usize) -> u64 {
    let seq = delivery.seq_by_home[home];
    delivery.seq_by_home[home] += 1;
    seq
}

struct FanOutParts<'a> {
    me: usize,
    links: &'a Links,
    home: &'a mut Home,
    spans: &'a mut Vec<RecordSpan>,
    filter_check_count: &'a mut u64,
    counters: Option<&'a mut ShardCounters>,
}

/// A batch has become visible: each distinct filter is checked once, here, and every home
/// core with subscribers gets the answers to share out (section 5.2, 6.1).
fn fan_out(stream: &mut Stream, entry: PendingVisible, parts: FanOutParts<'_>) {
    let has_state = entry.state.is_some();
    let table = &stream.delivery.table;
    let core_count = stream.delivery.seq_by_home.len();
    if !(0..core_count).any(|home| table.wants(home, has_state)) {
        return;
    }
    let bytes = SharedBytes::Slab(entry.bytes);
    walk_records(&bytes, parts.spans);
    let (matches, check_count, fan_out_bytes) = table.check(&bytes, parts.spans);
    *parts.filter_check_count += check_count;
    if let Some(counters) = parts.counters {
        counters.fan_out_bytes += fan_out_bytes;
    }
    let fan = Arc::new(FanOut {
        through_id: entry.last_id,
        matches,
        state: entry.state,
    });
    for home in 0..core_count {
        if !stream.delivery.table.wants(home, has_state) {
            continue;
        }
        let seq = next_seq(&mut stream.delivery, home);
        let body = StreamBody::Fan(Arc::clone(&fan));
        if home == parts.me {
            parts.home.on_stream(Arc::clone(&stream.name), seq, body);
        } else {
            let _ = parts.links.delivery[home].send(DeliveryMsg::Stream {
                stream: Arc::clone(&stream.name),
                seq,
                body,
            });
        }
    }
}
