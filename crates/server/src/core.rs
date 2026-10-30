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
use format::{HEADER_BYTES, IdAllocator, encode_system_record, validate_publish};
use proto::ErrorCode;
use storage::{
    Section, SectionExtent, UploadEvent, UploadKey, UploadSendError, UploaderSender, WalEntry,
    WalSendError, WalSender, stream_fits_dedicated_path,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::clock::Clock;
use crate::dedupe::{DedupeCheck, DedupeTable};
use crate::message::{
    ControlMsg, CoreStats, PublishMsg, Reply, ReplyTx, ShardHandoff, SnapshotReply, StateChange,
    StateOpMsg, StreamConfig, StreamHandoff, WriteMsg,
};
use crate::placement::{ShardCounters, shard_of};
use crate::slab::{SlabBytes, SlabPool};
use crate::state::{StreamState, apply_op, check_doc};

/// How often a core retries what didn't fit in a full channel. Crossbeam gives nothing
/// to await, and the writer's wake-ups only cover this core's own entries (D62).
const RETRY_FULL_MS: u64 = 1;
/// `actor u64` of a system record, then its doc (section 2.2).
const ACTOR_BYTES: usize = 8;

#[derive(Clone)]
pub(crate) struct Links {
    pub control: Vec<mpsc::UnboundedSender<ControlMsg>>,
    /// Writes passed on by another core. Unbounded, so that two cores forwarding to each
    /// other can't wait on each other. What is in here was read from a bounded inbox.
    pub forwarded: Vec<mpsc::UnboundedSender<WriteMsg>>,
    /// Writes from clients. Bounded: a full inbox is backpressure on the publisher.
    pub direct: Vec<mpsc::Sender<WriteMsg>>,
}

/// What a core tells the node's control thread.
#[derive(Debug)]
pub(crate) enum CoreToNode {
    /// This core's share of the segment cap is used up (Q18).
    SealRequested,
}

pub(crate) struct CoreConfig {
    pub id: usize,
    pub virtual_shard_count: u32,
    pub override_shard_count: u32,
    pub core_count: usize,
    pub segment_cap_share_bytes: u64,
    pub stream_buffer_bytes: u64,
    pub last_valid_id: u64,
}

pub(crate) struct CoreInbox {
    pub control: mpsc::UnboundedReceiver<ControlMsg>,
    pub forwarded: mpsc::UnboundedReceiver<WriteMsg>,
    pub direct: mpsc::Receiver<WriteMsg>,
    pub durable: watch::Receiver<u64>,
    pub uploads: mpsc::UnboundedReceiver<UploadEvent>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum UploadTag {
    Seal(u64),
    Dedicated(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchUpload {
    Unsealed,
    Pending(UploadTag),
    Done,
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

    fn has_unsealed(&self) -> bool {
        self.first_unsealed < self.batches.len()
    }

    /// Level 3 acks go out, and the core's references to uploaded batches go away.
    fn complete(&mut self, tag: UploadTag) {
        for batch in self.batches.iter_mut().take(self.first_unsealed) {
            if batch.upload == BatchUpload::Pending(tag) {
                batch.upload = BatchUpload::Done;
                for waiter in batch.waiters.drain(..) {
                    waiter.fire();
                }
            }
        }
        while self
            .batches
            .front()
            .is_some_and(|batch| batch.upload == BatchUpload::Done)
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

    logs: HashMap<Arc<str>, StreamLog>,
    dirty: Vec<Arc<str>>,
    next_ticket: u64,
    /// The highest ticket the WAL channel has taken.
    sent_ticket: u64,
    outbound: VecDeque<WalEntry<SlabBytes>>,
    wal_stopped: bool,
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
}

impl Core {
    pub(crate) fn new(parts: CoreParts) -> Core {
        let config = parts.config;
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
            logs: HashMap::new(),
            dirty: Vec::new(),
            next_ticket: 0,
            sent_ticket: 0,
            outbound: VecDeque::new(),
            wal_stopped: false,
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
        }
    }

    pub(crate) async fn run(mut self) {
        let mut durable_open = true;
        let mut uploads_open = true;
        let mut stopping = false;
        while !stopping {
            // A stamped entry that doesn't fit stops new writes being read, and nothing else.
            let can_write = self.outbound.is_empty();
            let need_retry = (!self.outbound.is_empty() && !self.wal_stopped)
                || (!self.uploads_out.is_empty() && !self.upload_stopped);
            tokio::select! {
                biased;
                message = self.inbox.control.recv() => match message {
                    Some(ControlMsg::Stop) | None => stopping = true,
                    Some(message) => self.on_control(message),
                },
                changed = self.inbox.durable.changed(), if durable_open => match changed {
                    Ok(()) => self.on_durable(),
                    Err(_) => durable_open = false,
                },
                event = self.inbox.uploads.recv(), if uploads_open => match event {
                    Some(event) => self.on_upload_event(event),
                    None => uploads_open = false,
                },
                _ = tokio::time::sleep(Duration::from_millis(RETRY_FULL_MS)), if need_retry => {}
                message = self.inbox.forwarded.recv(), if can_write => {
                    if let Some(message) = message {
                        stopping = self.drain_control();
                        self.on_write(message);
                    }
                },
                message = self.inbox.direct.recv(), if can_write => {
                    if let Some(message) = message {
                        stopping = self.drain_control();
                        self.on_write(message);
                    }
                },
            }
            self.flush_outbound();
            self.flush_uploads();
        }
    }

    /// A write that got here may have been sent after an `Adopt` it depends on, and a
    /// message sent earlier by that core is already in this inbox. So read it first.
    fn drain_control(&mut self) -> bool {
        while let Ok(message) = self.inbox.control.try_recv() {
            if matches!(message, ControlMsg::Stop) {
                return true;
            }
            self.on_control(message);
        }
        false
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
            WriteMsg::Barrier { stream, reply } => match self.route(&stream) {
                Route::Here(_) => send_reply(
                    &reply,
                    Reply::RouteDone {
                        stream,
                        core: self.config.id,
                    },
                ),
                Route::Forward(to) => {
                    self.forward_write(to, WriteMsg::Barrier { stream, reply });
                }
            },
        }
    }

    fn forward_write(&mut self, to: usize, message: WriteMsg) {
        self.forwarded_count += 1;
        let _ = self.links.forwarded[to].send(message);
    }

    fn stream_entry(&mut self, name: &Arc<str>, shard: u32) -> &mut Stream {
        if !self.streams.contains_key(&**name) {
            self.shards
                .entry(shard)
                .or_default()
                .streams
                .insert(Arc::clone(name));
            self.streams.insert(
                Arc::clone(name),
                Stream {
                    name: Arc::clone(name),
                    shard,
                    ids: IdAllocator::new(0, self.config.last_valid_id),
                    state: StreamState::new(),
                    dedupe: DedupeTable::new(),
                    config: StreamConfig::default(),
                    owners: Vec::new(),
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
        if !stream_name_is_valid(&msg.stream) {
            nack(ErrorCode::Malformed, &"stream id is empty or too long");
            return;
        }
        let mut validated = match validate_publish(&mut records) {
            Ok(validated) => validated,
            Err(error) => {
                nack(ErrorCode::Malformed, &error);
                return;
            }
        };
        let has_state = msg.state.is_some();
        let item_count = validated.item_count() + u64::from(has_state);
        if item_count == 0 {
            nack(ErrorCode::Malformed, &"publish has no records or state");
            return;
        }
        let Ok(ack_count) = u32::try_from(item_count) else {
            nack(ErrorCode::Malformed, &"publish has too many items");
            return;
        };
        if !entry_fits(validated.as_bytes().len(), msg.state.as_ref()) {
            nack(ErrorCode::Malformed, &"publish is too large");
            return;
        }
        if let Some(change) = &msg.state
            && let Err(error) = check_doc(&change.doc)
        {
            nack(ErrorCode::Malformed, &error);
            return;
        }
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

        let bytes = match (&msg.state, new_state) {
            (Some(change), Some(doc)) => {
                let bytes = self.bytes_with_system_record(&records, last_id, msg.author, change);
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
        // A result has to go back, so a stream whose default is level 0 acts as level 1.
        let level = effective_level(None, &stream.config).max(1);
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

        if over_stream_buffer && stream_fits_dedicated_path(name) {
            self.send_dedicated(name);
        }
        if !self.seal_requested && self.unsealed_bytes >= self.config.segment_cap_share_bytes {
            self.seal_requested = true;
            let _ = self.to_node.send(CoreToNode::SealRequested);
        }
    }

    /// In order, so a full channel keeps a stream's entries in id order.
    fn flush_outbound(&mut self) {
        while let Some(entry) = self.outbound.pop_front() {
            let ticket = entry.ticket_for_core();
            match self.wal.try_append(entry) {
                Ok(()) => self.sent_ticket = ticket,
                Err(WalSendError::Full(entry)) => {
                    self.outbound.push_front(entry);
                    return;
                }
                Err(WalSendError::Stopped(entry)) => {
                    // Nothing more can become durable, so writes stay unread (D7).
                    self.wal_stopped = true;
                    self.outbound.push_front(entry);
                    return;
                }
            }
        }
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
            } else {
                let ticket = batch.ticket;
                let at = self.durable_waiters.partition_point(|(t, _)| *t <= ticket);
                self.durable_waiters.insert(at, (ticket, waiter));
            }
        } else if batch.upload == BatchUpload::Done {
            waiter.fire();
        } else {
            batch.waiters.push(waiter);
        }
    }

    fn on_seal(&mut self, seal: u64) {
        self.seal_requested = false;
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
        let (key, failed) = match event {
            UploadEvent::Uploaded { key, .. } => (key, false),
            UploadEvent::Failed { key, .. } => (key, true),
            UploadEvent::Empty { .. } | UploadEvent::Retrying { .. } => return,
        };
        let tag = match key {
            UploadKey::Seal(seal) => UploadTag::Seal(seal),
            UploadKey::Dedicated { number, .. } => UploadTag::Dedicated(number),
        };
        if failed {
            // The uploader keeps the references and level 3 stays unacked, since the
            // records aren't in the store (D55).
            if self.in_flight.contains_key(&tag) {
                self.upload_failed_count += 1;
            }
            return;
        }
        let Some(names) = self.in_flight.remove(&tag) else {
            return;
        };
        for name in names {
            let Some(log) = self.logs.get_mut(&*name) else {
                continue;
            };
            log.complete(tag);
            if log.batches.is_empty() && !self.streams.contains_key(&*name) {
                self.logs.remove(&*name);
            }
        }
    }

    fn on_control(&mut self, message: ControlMsg) {
        match message {
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
                            head: stream.ids.last_id(),
                            state_ver: stream.state.ver(),
                            state: stream.state.doc().clone(),
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
                });
            }
            ControlMsg::AckWhenReached {
                stream,
                last_id,
                level,
                ack,
                reply,
            } => self.ack_local(&stream, last_id, level, ack, reply),
            ControlMsg::Stop => {}
        }
    }

    fn package_stream(&mut self, name: &str) -> Option<StreamHandoff> {
        let mut stream = self.streams.remove(name)?;
        let last_id = stream.ids.last_id();
        if last_id > 0 {
            stream.owners.push((last_id, self.config.id));
        }
        Some(StreamHandoff {
            name: stream.name,
            ids: stream.ids,
            state: stream.state,
            dedupe: stream.dedupe,
            config: stream.config,
            owners: stream.owners,
        })
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
        let _ = self.links.control[to].send(ControlMsg::Adopt(Box::new(ShardHandoff {
            shard,
            streams,
            counters: state.counters,
            overrides,
            done,
        })));
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
        let _ = self.links.control[to].send(ControlMsg::Adopt(Box::new(ShardHandoff {
            shard: new_shard,
            streams,
            counters: ShardCounters::default(),
            overrides: vec![(name, new_shard, to as u16)],
            done,
        })));
    }

    fn on_adopt(&mut self, handoff: ShardHandoff) {
        let me = self.config.id as u16;
        let shard_id = handoff.shard;
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
        let shard = self.shards.entry(shard_id).or_default();
        shard.counters = handoff.counters;
        for stream in handoff.streams {
            shard.streams.insert(Arc::clone(&stream.name));
            if let Some(log) = self.logs.get_mut(&*stream.name) {
                log.residency += 1;
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
                },
            );
        }
        let _ = handoff.done.send(());
    }
}

struct CommitIds {
    producer_id: u64,
    producer_seq: u64,
    first_id: u64,
    last_id: u64,
    record_count: u64,
}
