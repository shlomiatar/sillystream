//! The connection's side of delivery (SPEC section 5.2), on its home core (D24): each
//! subscription's bounded queue, linger, `PROGRESS`, `BEHIND`, fair sharing and catch-up.
//!
//! `Connection` is what M6 puts a socket behind. `recv` stands in for the socket, and bytes
//! it hasn't taken count against a window, which is how a slow client is seen here.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use format::{Clause, Decision, Filter, KeyHash};
use object_store::ObjectStore;
use proto::ErrorCode;
use storage::{BlockTable, Ownership, SegmentError, SegmentReader, StreamExtent};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;

use crate::core::Links;
use crate::delivery::{
    FanOut, Matched, ObjectRef, RecordSpan, STATE_ONLY_FILTER_ID, SharedBytes, walk_records,
};
use crate::message::{
    AttachMsg, CatchReply, ConnCmd, ControlMsg, CursorOp, CursorReport, ReadDone, ReadMsg,
    ReadStepMsg, StreamBody, SubRef,
};
use crate::pages::PageCache;
use crate::slab::SlabAccount;

/// `type u8 | flags u8 | sub u32 | len u32` of a `DATA` frame.
const DATA_FRAME_HEAD_BYTES: u64 = 10;
/// How long a failed store read waits before the next try (D87).
const READ_RETRY_MS: u64 = 100;
/// A newly held allocation costs at most this share of a connection's window (D96).
const HELD_WINDOW_SHARE: u64 = 8;
/// `type u8 | sub u32 | id u64`: what an unread `PROGRESS` takes from the window (D102).
const PROGRESS_FRAME_BYTES: u64 = 13;
/// How often a home core looks at memory pressure, and so how long a client has to take one
/// frame before it counts as stalled (D99).
const PRESSURE_CHECK_MS: u64 = 50;

/// One matching stretch of a `DATA` frame: shared bytes, not a copy.
#[derive(Debug, Clone)]
pub struct DataPart {
    matched: Arc<Matched>,
    start: usize,
    end: usize,
}

impl DataPart {
    pub fn as_bytes(&self) -> &[u8] {
        &self.matched.bytes[self.start..self.end]
    }
}

/// The `records` of a `DATA` frame: whole records, in id order.
#[derive(Debug, Clone, Default)]
pub struct DataFrame {
    parts: Vec<DataPart>,
    len_bytes: u64,
    /// What this frame took from the connection's window, and returns once read (D96).
    window_cost_bytes: u64,
    /// The subscription it was for, and the id given before it, which is where that
    /// subscription carries on if this frame is taken back (D99).
    sub_serial: u64,
    before_id: u64,
}

impl DataFrame {
    pub fn parts(&self) -> &[DataPart] {
        &self.parts
    }

    pub fn len_bytes(&self) -> u64 {
        self.len_bytes
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len_bytes as usize);
        for part in &self.parts {
            out.extend_from_slice(part.as_bytes());
        }
        out
    }
}

/// What a connection hears. Each is one server frame of section 11.1.
#[derive(Debug, Clone)]
pub enum Event {
    Subscribed {
        sub: u32,
        head: u64,
        state_ver: u64,
        state: Bytes,
        /// It started at or above what is visible, so no catch-up, and so no `DATA` or
        /// `PROGRESS` up to `head`, will follow (D231).
        caught_up: bool,
    },
    Data {
        sub: u32,
        records: DataFrame,
    },
    State {
        sub: u32,
        ver: u64,
        doc: Bytes,
    },
    Behind {
        sub: u32,
        resume_from: u64,
    },
    Progress {
        sub: u32,
        id: u64,
    },
    Error {
        reference: u32,
        code: ErrorCode,
        msg: String,
        /// The `tag` of the subscription this ended, if it ended one. The id alone can't say:
        /// the client may have used it again by the time this is read (D229).
        ended: Option<u64>,
    },
}

/// What a `SUBSCRIBE` frame carries.
#[derive(Debug, Clone, Copy)]
pub struct Subscription<'a> {
    pub sub: u32,
    pub stream: KeyHash,
    pub from: u64,
    pub linger_ms: u16,
    pub author: Option<u64>,
    pub clauses: &'a [Clause],
    pub name: Option<KeyHash>,
    pub with_state: bool,
    pub state_only: bool,
    pub include_system: bool,
    /// Comes back on the error that ends this subscription.
    pub tag: u64,
}

impl<'a> Subscription<'a> {
    /// Everything but system records, from `from`, with no linger.
    pub fn new(sub: u32, stream: KeyHash, from: u64) -> Subscription<'a> {
        Subscription {
            sub,
            stream,
            from,
            linger_ms: 0,
            author: None,
            clauses: &[],
            name: None,
            with_state: false,
            state_only: false,
            include_system: false,
            tag: 0,
        }
    }
}

#[derive(Debug, Default)]
struct OutFrames {
    queue: VecDeque<Event>,
    taken_count: u64,
    closed: bool,
}

/// A connection's frames not yet taken, as a socket's writer would hold them. Its home core
/// can take frames back, which a channel can't do, so that a client not reading doesn't keep
/// memory (D99). The lock is between one core and one connection's reader, never two cores.
#[derive(Debug, Default)]
pub(crate) struct Outbox {
    frames: Mutex<OutFrames>,
    wake: Notify,
    /// Commands from the client that no core has finished with (D131).
    commands_out_count: AtomicU64,
}

/// One client command on its way. It travels with the command across however many cores that
/// takes, and is dropped wherever the command ends, so a connection can see how much of the
/// node's unbounded inboxes it is using without any reply to count (D131).
#[derive(Debug)]
pub(crate) struct CommandTicket(Arc<Outbox>);

impl CommandTicket {
    fn new(out: &Arc<Outbox>) -> CommandTicket {
        out.commands_out_count.fetch_add(1, Ordering::Relaxed);
        CommandTicket(Arc::clone(out))
    }
}

impl Drop for CommandTicket {
    fn drop(&mut self) {
        self.0.commands_out_count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Outbox {
    fn lock(&self) -> MutexGuard<'_, OutFrames> {
        self.frames.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, event: Event) {
        let mut frames = self.lock();
        let was_empty = frames.queue.is_empty();
        frames.queue.push_back(event);
        drop(frames);
        if was_empty {
            self.wake.notify_one();
        }
    }

    fn close(&self) {
        self.lock().closed = true;
        self.wake.notify_one();
    }

    /// The next frame, then `None` once the core has gone and nothing is left.
    async fn next(&self) -> Option<Event> {
        loop {
            {
                let mut frames = self.lock();
                if let Some(event) = frames.queue.pop_front() {
                    frames.taken_count += 1;
                    return Some(event);
                }
                if frames.closed {
                    return None;
                }
            }
            self.wake.notified().await;
        }
    }

    fn try_next(&self) -> Option<Event> {
        let mut frames = self.lock();
        let event = frames.queue.pop_front()?;
        frames.taken_count += 1;
        Some(event)
    }

    /// `(frames taken so far, whether any wait)`.
    fn reading_state(&self) -> (u64, bool) {
        let frames = self.lock();
        (frames.taken_count, !frames.queue.is_empty())
    }

    /// Removes the unread `DATA`, and the `PROGRESS` behind it, which would claim past what
    /// was removed. A `BEHIND` behind it moves back to match. What is left goes on being
    /// delivered in order, and what the client took before this is untouched.
    fn take_back(&self) -> TakenBack {
        let mut taken = TakenBack::default();
        let mut frames = self.lock();
        frames.queue.retain_mut(|event| match event {
            Event::Data { sub, records } => {
                taken.window_cost_bytes += records.window_cost_bytes;
                taken
                    .first_by_sub
                    .entry(*sub)
                    .or_insert((records.sub_serial, records.before_id));
                false
            }
            Event::Progress { sub, .. } if taken.first_by_sub.contains_key(sub) => {
                taken.window_cost_bytes += PROGRESS_FRAME_BYTES;
                false
            }
            Event::Behind { sub, resume_from } => {
                if let Some((_, before_id)) = taken.first_by_sub.get(sub) {
                    *resume_from = *before_id;
                }
                true
            }
            _ => true,
        });
        taken
    }
}

#[derive(Debug, Default)]
struct TakenBack {
    window_cost_bytes: u64,
    /// `sub → (serial, id given before its first frame taken)`.
    first_by_sub: HashMap<u32, (u64, u64)>,
}

/// One connection's handle on its home core.
pub struct Connection {
    control: mpsc::UnboundedSender<ControlMsg>,
    conn: usize,
    out: Arc<Outbox>,
    credit_owed_bytes: u64,
    credit_step_bytes: u64,
}

impl Connection {
    pub(crate) async fn open(
        control: mpsc::UnboundedSender<ControlMsg>,
        window_bytes: u64,
    ) -> Option<Connection> {
        let out = Arc::new(Outbox::default());
        let (reply, answer) = oneshot::channel();
        control
            .send(ControlMsg::Conn(ConnCmd::Open {
                out: Arc::clone(&out),
                reply,
            }))
            .ok()?;
        let conn = answer.await.ok()?;
        Some(Connection {
            control,
            conn,
            out,
            credit_owed_bytes: 0,
            credit_step_bytes: (window_bytes / 4).max(1),
        })
    }

    fn send(&self, command: ConnCmd) {
        let _ = self.control.send(ControlMsg::Conn(command));
    }

    /// `SUBSCRIBE`. The answer is `Event::Subscribed` or `Event::Error`.
    pub fn subscribe(&self, subscription: Subscription<'_>) {
        let filter = Filter::new(subscription.clauses).map(|mut filter| {
            filter.author = subscription.author;
            filter.include_system = subscription.include_system;
            filter
        });
        self.send(ConnCmd::Subscribe {
            conn: self.conn,
            sub: subscription.sub,
            stream: subscription.stream,
            from: subscription.from,
            linger_ms: subscription.linger_ms,
            filter: filter.ok(),
            name: subscription.name,
            with_state: subscription.with_state,
            state_only: subscription.state_only,
            tag: subscription.tag,
            ticket: CommandTicket::new(&self.out),
        });
    }

    pub fn unsubscribe(&self, sub: u32) {
        self.send(ConnCmd::Unsubscribe {
            conn: self.conn,
            sub,
            _ticket: CommandTicket::new(&self.out),
        });
    }

    /// `COMMIT`: moves the subscription's named cursor forward.
    pub fn commit(&self, sub: u32, id: u64) {
        self.send(ConnCmd::Commit {
            conn: self.conn,
            sub,
            id,
            ticket: CommandTicket::new(&self.out),
        });
    }

    /// Commands sent that the node hasn't finished with.
    pub(crate) fn commands_out_count(&self) -> u64 {
        self.out.commands_out_count.load(Ordering::Relaxed)
    }

    /// The explicit reset of section 5.1, which alone can move a cursor back.
    pub fn reset_cursor(&self, stream: KeyHash, name: KeyHash, id: u64) {
        self.send(ConnCmd::ResetCursor {
            conn: self.conn,
            stream,
            name,
            id,
        });
    }

    /// Where a named cursor is. In order with this connection's commits and resets.
    pub async fn read_cursor(&self, stream: KeyHash, name: KeyHash) -> Option<u64> {
        let (reply, answer) = oneshot::channel();
        self.send(ConnCmd::ReadCursor {
            stream,
            name,
            reply,
        });
        answer.await.ok().flatten()
    }

    /// Returns what a frame took from the window, which `DATA`, `STATE` and `PROGRESS` do.
    fn took(&mut self, event: &Event) {
        self.credit_owed_bytes += match event {
            Event::Data { records, .. } => records.window_cost_bytes,
            Event::State { doc, .. } => doc.len() as u64,
            Event::Progress { .. } => PROGRESS_FRAME_BYTES,
            Event::Behind { sub, .. } => {
                self.send(ConnCmd::BehindRead {
                    conn: self.conn,
                    sub: *sub,
                });
                return;
            }
            _ => return,
        };
        if self.credit_owed_bytes >= self.credit_step_bytes {
            self.send(ConnCmd::Credit {
                conn: self.conn,
                bytes: self.credit_owed_bytes,
            });
            self.credit_owed_bytes = 0;
        }
    }

    /// The next frame if one is here already.
    pub(crate) fn try_recv(&mut self) -> Option<Event> {
        let event = self.out.try_next()?;
        self.took(&event);
        Some(event)
    }

    /// The next frame, as a socket taking it. `None` once the node has gone.
    pub async fn recv(&mut self) -> Option<Event> {
        let event = self.out.next().await?;
        self.took(&event);
        Some(event)
    }

    /// The next frame within `wait_ms`.
    pub async fn recv_within(&mut self, wait_ms: u64) -> Option<Event> {
        let waited = tokio::time::timeout(Duration::from_millis(wait_ms), self.out.next()).await;
        let event = waited.ok().flatten()?;
        self.took(&event);
        Some(event)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.send(ConnCmd::Closed { conn: self.conn });
    }
}

/// Delivery settings of a node (D6d).
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeliverySettings {
    pub queue_bytes: u64,
    pub progress_interval_ms: u64,
    /// Prints a line on stderr for each catch-up read, naming its tier.
    pub trace_catch_up: bool,
    pub max_frame_bytes: u64,
    pub max_subscriptions: usize,
    pub share_bytes: u64,
    pub window_bytes: u64,
    pub read_bytes: u64,
    pub memory_cap_bytes: u64,
}

/// Which stores a catch-up reads, nearest first.
#[derive(Clone)]
pub(crate) struct Stores {
    pub local: Option<Arc<dyn ObjectStore>>,
    pub object: Arc<dyn ObjectStore>,
    pub pages: Arc<PageCache>,
    /// How far each former generation's objects are read (section 8.1).
    pub ownership: Arc<Ownership>,
    /// The node's object table, which the cores read and the node and compactor change.
    pub objects: Arc<std::sync::RwLock<storage::ObjectTable>>,
}

/// What a home core counts.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HomeStats {
    pub behind_count: u64,
    pub stalled_release_count: u64,
    pub progress_count: u64,
    pub blocks_read_count: u64,
    pub blocks_skipped_count: u64,
    pub memory_bytes: u64,
    pub local_bytes: u64,
    pub object_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Attaching,
    Live { filter_id: u32 },
    StateOnly,
    CatchUp,
}

struct QueueItem {
    matched: Arc<Matched>,
    run: usize,
    offset: usize,
}

/// A catch-up partway through an object, so its region is found once.
struct ObjectProgress {
    object: ObjectRef,
    region: Option<Region>,
    next_block: usize,
    limit: u64,
}

/// A stream's region in an object, and the id a read takes records above.
#[derive(Debug, Clone)]
pub(crate) struct Region {
    pub extent: StreamExtent,
    pub table: Arc<BlockTable>,
    pub after: u64,
}

struct Sub {
    /// Changes when it falls behind, so that a read in flight is ignored.
    uid: u64,
    /// Stays for the subscription's life.
    serial: u64,
    conn: usize,
    id: u32,
    stream: KeyHash,
    filter: Filter,
    linger_ms: u16,
    name: Option<KeyHash>,
    tag: u64,
    mode: Mode,
    subscribed_sent: bool,
    queue: VecDeque<QueueItem>,
    queued_bytes: u64,
    /// The size of the allocations the queue keeps alive: each run of neighbours in one
    /// allocation counts once (D96).
    held_bytes: u64,
    /// The allocation this subscription's last frame was charged for.
    frame_held: Option<usize>,
    /// The highest id looked at: everything at or below it matching is queued or sent.
    position: u64,
    /// The highest id this subscriber has been told about.
    handed_through: u64,
    due: bool,
    in_turn: bool,
    timer_armed: bool,
    last_progress: Option<Instant>,
    head_progress_owed: bool,
    read_out: bool,
    /// A `BEHIND` is on its way. Catching up waits until the client has read it (D86, D99).
    behind_unread: bool,
    object: Option<ObjectProgress>,
    /// The object read last, so the next step goes past it.
    past: Option<ObjectRef>,
    /// The last id of the region read last, for the order check (section 7.5).
    past_last_id: u64,
    /// An object found missing, for the next step to tell the owner of.
    gone: Option<ObjectRef>,
    read_bytes_since: u64,
    state_ver: u64,
    latest_state: Option<(u64, Bytes)>,
}

impl Sub {
    /// What the queue costs against its bound: its bytes or the memory they keep, whichever
    /// is more.
    fn queue_cost_bytes(&self) -> u64 {
        self.queued_bytes.max(self.held_bytes)
    }

    /// `queue_cost_bytes` with `matched` added.
    fn queue_cost_with(&self, matched: &Matched, bytes: u64) -> u64 {
        let (held, size_bytes) = matched.bytes.held();
        let new_held_bytes = match self.queue.back() {
            Some(last) if last.matched.bytes.held().0 == held => 0,
            _ => size_bytes,
        };
        (self.queued_bytes + bytes).max(self.held_bytes + new_held_bytes)
    }

    fn push_item(&mut self, item: QueueItem, bytes: u64) {
        let (held, size_bytes) = item.matched.bytes.held();
        if self
            .queue
            .back()
            .is_none_or(|last| last.matched.bytes.held().0 != held)
        {
            self.held_bytes += size_bytes;
        }
        self.queued_bytes += bytes;
        self.queue.push_back(item);
    }

    fn pop_item(&mut self) {
        let Some(item) = self.queue.pop_front() else {
            return;
        };
        let (held, size_bytes) = item.matched.bytes.held();
        if self
            .queue
            .front()
            .is_none_or(|next| next.matched.bytes.held().0 != held)
        {
            self.held_bytes -= size_bytes;
        }
    }

    fn clear_queue(&mut self) {
        self.queue.clear();
        self.queued_bytes = 0;
        self.held_bytes = 0;
    }

    /// Whole records up to `budget_bytes`. Only a record over the budget by itself goes past
    /// it, and then it goes alone (D91). Bytes aren't copied.
    fn take_frame(
        &mut self,
        budget_bytes: u64,
        held_charge_bytes: u64,
        spans: &mut Vec<RecordSpan>,
    ) -> DataFrame {
        let mut frame = DataFrame {
            sub_serial: self.serial,
            before_id: self.handed_through,
            ..DataFrame::default()
        };
        let mut left_bytes = budget_bytes as usize;
        while left_bytes > 0 {
            let Some(item) = self.queue.front_mut() else {
                break;
            };
            let run = item.matched.runs[item.run];
            let start = item.offset.max(run.start);
            let mut end = run.end;
            let mut last_id = run.last_id;
            if end - start > left_bytes {
                // Only an oversized stretch needs its headers walked.
                walk_records(&item.matched.bytes[start..run.end], spans);
                let mut taken = 0;
                let frame_is_empty = frame.parts.is_empty();
                for span in spans.iter() {
                    if span.end > left_bytes && (taken > 0 || !frame_is_empty) {
                        break;
                    }
                    taken = span.end;
                    last_id = span.header.last_id();
                }
                if taken == 0 {
                    if !frame_is_empty {
                        break;
                    }
                    // Bytes that don't walk can't be whole records. Nothing valid gets here.
                    taken = end - start;
                }
                end = start + taken;
            }
            let part_bytes = end - start;
            let (held, size_bytes) = item.matched.bytes.held();
            frame.window_cost_bytes += if self.frame_held == Some(held) {
                part_bytes as u64
            } else {
                self.frame_held = Some(held);
                size_bytes.min(held_charge_bytes).max(part_bytes as u64)
            };
            frame.parts.push(DataPart {
                matched: Arc::clone(&item.matched),
                start,
                end,
            });
            frame.len_bytes += part_bytes as u64;
            self.queued_bytes -= part_bytes as u64;
            self.handed_through = self.handed_through.max(last_id);
            left_bytes = left_bytes.saturating_sub(part_bytes);
            if end == run.end {
                item.run += 1;
                item.offset = 0;
                if item.run == item.matched.runs.len() {
                    self.pop_item();
                }
            } else {
                item.offset = end;
            }
        }
        frame
    }

    /// The parts of `matched` above this subscriber's position.
    fn item_after_position(&self, matched: &Arc<Matched>) -> Option<(QueueItem, u64)> {
        if matched.last_id <= self.position {
            return None;
        }
        if matched.first_id > self.position {
            let item = QueueItem {
                matched: Arc::clone(matched),
                run: 0,
                offset: 0,
            };
            return Some((item, matched.total_bytes));
        }
        // A batch the position falls inside: only a subscriber just attached mid-way.
        let run = matched
            .runs
            .partition_point(|run| run.last_id <= self.position);
        let mut spans = Vec::new();
        walk_records(
            &matched.bytes[matched.runs[run].start..matched.runs[run].end],
            &mut spans,
        );
        let base = matched.runs[run].start;
        let offset = spans
            .iter()
            .find(|span| span.header.last_id() > self.position)
            .map_or(base, |span| base + span.start);
        let later_bytes: usize = matched.runs[run + 1..]
            .iter()
            .map(|run| run.end - run.start)
            .sum();
        let bytes = matched.runs[run].end - offset + later_bytes;
        let item = QueueItem {
            matched: Arc::clone(matched),
            run,
            offset,
        };
        Some((item, bytes as u64))
    }
}

struct Conn {
    uid: u64,
    out: Arc<Outbox>,
    /// `taken_count` at the last pressure check, if frames were waiting then.
    stall_seen: Option<u64>,
    window_left_bytes: i64,
    /// Subscriptions with something due, taking turns: this is the fair share.
    turns: VecDeque<usize>,
    subs: HashMap<u32, usize>,
    in_ready: bool,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.out.close();
    }
}

#[derive(Default)]
struct Group {
    subs: Vec<usize>,
    /// Nothing at or below this is still to come for these subscribers.
    through_id: u64,
}

/// A stream as this core's subscribers see it.
#[derive(Default)]
struct HomeStream {
    next_seq: u64,
    /// Messages from a new owner that overtook its predecessor's (D85).
    reorder: BTreeMap<u64, StreamBody>,
    groups: HashMap<u32, Group>,
    state_subs: Vec<usize>,
}

/// One core's connections and subscriptions.
pub(crate) struct Home {
    me: usize,
    links: Links,
    settings: DeliverySettings,
    stores: Stores,
    /// Every core's slab account: a subscriber here can pin a publisher's slabs anywhere.
    accounts: Vec<SlabAccount>,
    next_pressure_check: Instant,
    conns: Vec<Option<Conn>>,
    free_conns: Vec<usize>,
    subs: Vec<Option<Sub>>,
    free_subs: Vec<usize>,
    next_uid: u64,
    streams: HashMap<KeyHash, HomeStream>,
    ready: Vec<usize>,
    linger: BinaryHeap<Reverse<(Instant, usize, u64)>>,
    /// Streams whose subscribers may owe a `PROGRESS`.
    skipping: HashSet<KeyHash>,
    next_progress_check: Instant,
    spans: Vec<RecordSpan>,
    after_turn: Vec<usize>,
    pub stats: HomeStats,
}

impl Home {
    pub fn new(
        me: usize,
        links: Links,
        settings: DeliverySettings,
        stores: Stores,
        accounts: Vec<SlabAccount>,
    ) -> Home {
        Home {
            me,
            links,
            settings,
            stores,
            accounts,
            next_pressure_check: Instant::now(),
            conns: Vec::new(),
            free_conns: Vec::new(),
            subs: Vec::new(),
            free_subs: Vec::new(),
            next_uid: 1,
            streams: HashMap::new(),
            ready: Vec::new(),
            linger: BinaryHeap::new(),
            skipping: HashSet::new(),
            next_progress_check: Instant::now(),
            spans: Vec::new(),
            after_turn: Vec::new(),
            stats: HomeStats::default(),
        }
    }

    fn max_data_bytes(&self) -> u64 {
        self.settings
            .max_frame_bytes
            .saturating_sub(DATA_FRAME_HEAD_BYTES)
    }

    fn sub_mut(&mut self, key: usize, uid: u64) -> Option<&mut Sub> {
        self.subs
            .get_mut(key)
            .and_then(Option::as_mut)
            .filter(|sub| sub.uid == uid)
    }

    fn send_event(&self, conn: usize, event: Event) {
        if let Some(Some(conn)) = self.conns.get(conn) {
            conn.out.push(event);
        }
    }

    fn sub_ref(&self, key: usize, uid: u64) -> SubRef {
        SubRef {
            home: self.me,
            key,
            uid,
        }
    }

    /// Connection commands, from the handle.
    pub fn on_command(&mut self, command: ConnCmd) {
        match command {
            ConnCmd::Open { out, reply } => {
                let uid = self.next_uid;
                self.next_uid += 1;
                let conn = Conn {
                    uid,
                    out,
                    stall_seen: None,
                    window_left_bytes: self.settings.window_bytes as i64,
                    turns: VecDeque::new(),
                    subs: HashMap::new(),
                    in_ready: false,
                };
                let key = match self.free_conns.pop() {
                    Some(key) => {
                        self.conns[key] = Some(conn);
                        key
                    }
                    None => {
                        self.conns.push(Some(conn));
                        self.conns.len() - 1
                    }
                };
                let _ = reply.send(key);
            }
            ConnCmd::Subscribe {
                conn,
                sub,
                stream,
                from,
                linger_ms,
                filter,
                name,
                with_state,
                state_only,
                tag,
                ticket,
            } => self.subscribe(
                conn, sub, stream, from, linger_ms, filter, name, with_state, state_only, tag,
                ticket,
            ),
            ConnCmd::Unsubscribe { conn, sub, .. } => {
                let key = self.conns[conn]
                    .as_ref()
                    .and_then(|c| c.subs.get(&sub).copied());
                match key {
                    Some(key) => self.remove_sub(key),
                    None => self.send_event(
                        conn,
                        Event::Error {
                            reference: sub,
                            code: ErrorCode::NotFound,
                            msg: "no such subscription".to_owned(),
                            ended: None,
                        },
                    ),
                }
            }
            ConnCmd::Commit {
                conn,
                sub,
                id,
                ticket,
            } => self.commit(conn, sub, id, ticket),
            ConnCmd::ResetCursor {
                conn,
                stream,
                name,
                id,
            } => {
                let report = self.cursor_report(conn, 0);
                self.post_cursor(stream, name, CursorOp::Reset(id), report, None);
            }
            ConnCmd::ReadCursor {
                stream,
                name,
                reply,
            } => self.post_cursor(stream, name, CursorOp::Read(reply), None, None),
            ConnCmd::Credit { conn, bytes } => self.on_credit(conn, bytes),
            ConnCmd::BehindRead { conn, sub } => {
                let key = self.conns[conn]
                    .as_ref()
                    .and_then(|c| c.subs.get(&sub).copied());
                if let Some(key) = key {
                    if let Some(Some(sub)) = self.subs.get_mut(key) {
                        sub.behind_unread = false;
                    }
                    self.pump_catch_up(key);
                }
            }
            ConnCmd::Closed { conn } => self.close(conn),
        }
    }

    fn cursor_report(&self, conn: usize, reference: u32) -> Option<CursorReport> {
        self.conns[conn].as_ref().map(|c| CursorReport {
            home: self.me,
            conn,
            conn_uid: c.uid,
            reference,
        })
    }

    /// An owner's error about a cursor, for a connection that may have gone since.
    pub fn on_report(&mut self, report: CursorReport, event: Event) {
        if let Some(Some(conn)) = self.conns.get(report.conn)
            && conn.uid == report.conn_uid
        {
            conn.out.push(event);
        }
    }

    fn post_cursor(
        &self,
        stream: KeyHash,
        name: KeyHash,
        op: CursorOp,
        report: Option<CursorReport>,
        ticket: Option<CommandTicket>,
    ) {
        let _ = self.links.control[self.me].send(ControlMsg::Cursor {
            stream,
            name,
            op,
            report,
            ticket,
        });
    }

    fn commit(&mut self, conn: usize, sub: u32, id: u64, ticket: CommandTicket) {
        let key = self.conns[conn]
            .as_ref()
            .and_then(|c| c.subs.get(&sub).copied());
        let target = key
            .and_then(|key| self.subs[key].as_ref())
            .and_then(|sub| sub.name.map(|name| (sub.stream, name)));
        let Some((stream, name)) = target else {
            self.send_event(
                conn,
                Event::Error {
                    reference: sub,
                    code: ErrorCode::Malformed,
                    msg: "COMMIT needs a subscription with a named cursor".to_owned(),
                    ended: None,
                },
            );
            return;
        };
        let report = self.cursor_report(conn, sub);
        self.post_cursor(stream, name, CursorOp::Commit(id), report, Some(ticket));
    }

    #[allow(clippy::too_many_arguments)]
    fn subscribe(
        &mut self,
        conn: usize,
        id: u32,
        stream: KeyHash,
        from: u64,
        linger_ms: u16,
        filter: Option<Filter>,
        name: Option<KeyHash>,
        with_state: bool,
        state_only: bool,
        tag: u64,
        ticket: CommandTicket,
    ) {
        let error = |code, msg: &str| Event::Error {
            reference: id,
            code,
            msg: msg.to_owned(),
            ended: Some(tag),
        };
        let Some(connection) = self.conns.get(conn).and_then(Option::as_ref) else {
            return;
        };
        let refused = if connection.subs.contains_key(&id) {
            Some(error(ErrorCode::Malformed, "subscription id is in use"))
        } else if connection.subs.len() >= self.settings.max_subscriptions {
            Some(error(
                ErrorCode::TooManySubscriptions,
                "too many subscriptions",
            ))
        } else if filter.is_none() {
            Some(error(ErrorCode::Malformed, "more than 4 clauses"))
        } else {
            None
        };
        if let Some(event) = refused {
            self.send_event(conn, event);
            return;
        }
        let uid = self.next_uid;
        self.next_uid += 1;
        let sub = Sub {
            uid,
            serial: uid,
            conn,
            id,
            stream,
            filter: filter.expect("checked above"),
            linger_ms,
            name,
            tag,
            mode: Mode::Attaching,
            subscribed_sent: false,
            queue: VecDeque::new(),
            queued_bytes: 0,
            held_bytes: 0,
            frame_held: None,
            position: 0,
            handed_through: 0,
            due: false,
            in_turn: false,
            timer_armed: false,
            last_progress: None,
            head_progress_owed: false,
            read_out: false,
            behind_unread: false,
            object: None,
            past: None,
            past_last_id: 0,
            gone: None,
            read_bytes_since: 0,
            state_ver: 0,
            latest_state: None,
        };
        let filter = sub.filter;
        let key = match self.free_subs.pop() {
            Some(key) => {
                self.subs[key] = Some(sub);
                key
            }
            None => {
                self.subs.push(Some(sub));
                self.subs.len() - 1
            }
        };
        if let Some(Some(connection)) = self.conns.get_mut(conn) {
            connection.subs.insert(id, key);
        }
        let _ = self.links.control[self.me].send(ControlMsg::Attach(Box::new(AttachMsg {
            stream,
            sub: self.sub_ref(key, uid),
            filter,
            from,
            name,
            with_state,
            state_only,
            walk: None,
            _ticket: ticket,
        })));
    }

    fn detach_from_owner(&self, stream: &KeyHash, filter_id: u32) {
        let _ = self.links.control[self.me].send(ControlMsg::Detach {
            stream: *stream,
            filter_id,
            home: self.me,
        });
    }

    /// Takes a subscriber off the stream's lists, and tells the owner.
    fn leave_group(&mut self, key: usize) {
        let Some(sub) = self.subs[key].as_ref() else {
            return;
        };
        let stream = sub.stream;
        let filter_id = match sub.mode {
            Mode::Live { filter_id } => filter_id,
            Mode::StateOnly => STATE_ONLY_FILTER_ID,
            Mode::Attaching | Mode::CatchUp => return,
        };
        if let Some(home_stream) = self.streams.get_mut(&stream) {
            if filter_id == STATE_ONLY_FILTER_ID {
                home_stream.state_subs.retain(|other| *other != key);
            } else if let Some(group) = home_stream.groups.get_mut(&filter_id) {
                group.subs.retain(|other| *other != key);
                if group.subs.is_empty() {
                    home_stream.groups.remove(&filter_id);
                }
            }
        }
        self.detach_from_owner(&stream, filter_id);
    }

    fn remove_sub(&mut self, key: usize) {
        self.leave_group(key);
        let Some(sub) = self.subs[key].take() else {
            return;
        };
        self.free_subs.push(key);
        if let Some(Some(conn)) = self.conns.get_mut(sub.conn) {
            conn.subs.remove(&sub.id);
            if sub.in_turn {
                conn.turns.retain(|other| *other != key);
            }
        }
    }

    fn close(&mut self, conn: usize) {
        let Some(connection) = self.conns.get_mut(conn).and_then(Option::take) else {
            return;
        };
        for key in connection.subs.values() {
            self.remove_sub(*key);
        }
        self.free_conns.push(conn);
    }

    fn on_credit(&mut self, conn: usize, bytes: u64) {
        let Some(Some(connection)) = self.conns.get_mut(conn) else {
            return;
        };
        connection.window_left_bytes += bytes as i64;
        let keys: Vec<usize> = connection.subs.values().copied().collect();
        self.make_ready(conn);
        for key in keys {
            self.send_latest_state(key);
            self.owed_after_frame(key);
        }
    }

    fn make_ready(&mut self, conn: usize) {
        if let Some(Some(connection)) = self.conns.get_mut(conn)
            && !connection.in_ready
        {
            connection.in_ready = true;
            self.ready.push(conn);
        }
    }

    /// Puts a subscription in its connection's turns, if it has something due.
    fn schedule(&mut self, key: usize) {
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        if !sub.due || sub.in_turn || sub.queue.is_empty() {
            return;
        }
        sub.in_turn = true;
        let conn = sub.conn;
        if let Some(Some(connection)) = self.conns.get_mut(conn) {
            connection.turns.push_back(key);
        }
        self.make_ready(conn);
    }

    /// Records joined a queue. They go out now, or when the timer fires or the queue has
    /// reached a frame, whichever is first (section 5.2). A queue bound below a frame would
    /// come first, so half the bound counts as full too (D97).
    fn after_enqueue(&mut self, key: usize) {
        let queue_bytes = self.settings.queue_bytes;
        let full_bytes = self.max_data_bytes().min(queue_bytes / 2);
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        if sub.linger_ms == 0 || sub.queued_bytes >= full_bytes || sub.held_bytes >= queue_bytes {
            sub.due = true;
        } else if !sub.due && !sub.timer_armed {
            sub.timer_armed = true;
            let at = Instant::now() + Duration::from_millis(u64::from(sub.linger_ms));
            self.linger.push(Reverse((at, key, sub.uid)));
        }
        self.schedule(key);
    }

    /// Sends what is due, connection by connection, one share per subscription per turn.
    pub fn flush(&mut self) {
        if self.ready.is_empty() {
            return;
        }
        let share_bytes = self.settings.share_bytes.min(self.max_data_bytes()).max(1);
        let held_charge_bytes = self.settings.window_bytes / HELD_WINDOW_SHARE;
        let mut spans = std::mem::take(&mut self.spans);
        for conn in std::mem::take(&mut self.ready) {
            let Some(Some(connection)) = self.conns.get_mut(conn) else {
                continue;
            };
            connection.in_ready = false;
            while connection.window_left_bytes > 0 {
                let Some(key) = connection.turns.pop_front() else {
                    break;
                };
                let Some(Some(sub)) = self.subs.get_mut(key) else {
                    continue;
                };
                sub.in_turn = false;
                let frame = sub.take_frame(share_bytes, held_charge_bytes, &mut spans);
                if frame.len_bytes > 0 {
                    connection.window_left_bytes -= frame.window_cost_bytes as i64;
                    connection.out.push(Event::Data {
                        sub: sub.id,
                        records: frame,
                    });
                }
                if sub.queue.is_empty() {
                    sub.due = false;
                } else if sub.due {
                    sub.in_turn = true;
                    connection.turns.push_back(key);
                }
                if sub.mode == Mode::CatchUp || (sub.head_progress_owed && sub.queue.is_empty()) {
                    self.after_turn.push(key);
                }
            }
        }
        self.spans = spans;
        for key in std::mem::take(&mut self.after_turn) {
            self.owed_after_frame(key);
        }
    }

    fn owed_after_frame(&mut self, key: usize) {
        let owed = self.subs[key]
            .as_ref()
            .is_some_and(|sub| sub.head_progress_owed && sub.queue.is_empty());
        if owed {
            self.send_head_progress(key);
        }
        self.pump_catch_up(key);
    }

    fn send_head_progress(&mut self, key: usize) {
        let still_owed = !self.progress(key, 0, true);
        if let Some(Some(sub)) = self.subs.get_mut(key) {
            sub.head_progress_owed = still_owed;
        }
    }

    /// Sends `PROGRESS` if it means something and may go now. False if it is still owed.
    fn progress(&mut self, key: usize, floor_id: u64, force: bool) -> bool {
        let interval = Duration::from_millis(self.settings.progress_interval_ms);
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return true;
        };
        let id = sub.position.max(floor_id);
        if id <= sub.handed_through {
            return true;
        }
        // Everything matching at or below `id` has to be out ahead of it.
        if !sub.queue.is_empty() {
            return false;
        }
        let Some(Some(connection)) = self.conns.get_mut(sub.conn) else {
            return true;
        };
        if connection.window_left_bytes <= 0 {
            return false;
        }
        let now = Instant::now();
        if !force && sub.last_progress.is_some_and(|at| now - at < interval) {
            return false;
        }
        sub.last_progress = Some(now);
        sub.handed_through = id;
        sub.position = id;
        connection.window_left_bytes -= PROGRESS_FRAME_BYTES as i64;
        connection.out.push(Event::Progress { sub: sub.id, id });
        self.stats.progress_count += 1;
        true
    }

    /// The next time `on_timer` has something to do.
    pub fn next_deadline(&self) -> Option<Instant> {
        let linger = self.linger.peek().map(|Reverse((at, _, _))| *at);
        let progress = (!self.skipping.is_empty()).then_some(self.next_progress_check);
        let pressure =
            (self.conns.len() > self.free_conns.len()).then_some(self.next_pressure_check);
        [linger, progress, pressure].into_iter().flatten().min()
    }

    fn is_under_pressure(&self) -> bool {
        self.accounts
            .iter()
            .any(|account| account.pinned_bytes() / 3 >= self.settings.memory_cap_bytes / 4)
    }

    /// Nothing else lets go of what a stalled client holds once publishers have stopped, as
    /// overflow only happens when a record arrives. A connection whose frames waited through
    /// a whole check, with none taken, is stalled (D99).
    fn check_pressure(&mut self) {
        if !self.is_under_pressure() {
            return;
        }
        for conn in 0..self.conns.len() {
            let Some(Some(connection)) = self.conns.get_mut(conn) else {
                continue;
            };
            let (taken_count, waiting) = connection.out.reading_state();
            let stalled = waiting && connection.stall_seen == Some(taken_count);
            connection.stall_seen = waiting.then_some(taken_count);
            if stalled {
                self.release_stalled(conn);
            }
        }
    }

    /// Takes back all a stalled connection's records, queued or sent and unread. Each of its
    /// subscriptions that lost any goes back to what the client has really seen.
    fn release_stalled(&mut self, conn: usize) {
        let Some(Some(connection)) = self.conns.get_mut(conn) else {
            return;
        };
        let taken = connection.out.take_back();
        connection.window_left_bytes += taken.window_cost_bytes as i64;
        let keys: Vec<usize> = connection.subs.values().copied().collect();
        let mut released = false;
        for key in keys {
            let Some(Some(sub)) = self.subs.get_mut(key) else {
                continue;
            };
            let lost_frames = match taken.first_by_sub.get(&sub.id) {
                Some((serial, before_id)) if *serial == sub.serial => {
                    sub.handed_through = *before_id;
                    sub.frame_held = None;
                    true
                }
                _ => false,
            };
            if !lost_frames && sub.queue.is_empty() {
                continue;
            }
            released = true;
            if sub.behind_unread {
                // The `BEHIND` it has yet to read has been moved back to this.
                sub.position = sub.handed_through;
            } else if matches!(sub.mode, Mode::Live { .. } | Mode::CatchUp) {
                self.fall_behind(key);
            }
        }
        if released {
            self.stats.stalled_release_count += 1;
        }
        // The window has opened, and turns left over from when it closed need taking.
        self.make_ready(conn);
    }

    pub fn on_timer(&mut self) {
        let now = Instant::now();
        while let Some(Reverse((at, key, uid))) = self.linger.peek().copied() {
            if at > now {
                break;
            }
            self.linger.pop();
            if let Some(sub) = self.sub_mut(key, uid) {
                sub.timer_armed = false;
                if !sub.queue.is_empty() {
                    sub.due = true;
                }
                self.schedule(key);
            }
        }
        if !self.skipping.is_empty() && self.next_progress_check <= now {
            self.next_progress_check =
                now + Duration::from_millis((self.settings.progress_interval_ms / 4).max(1));
            self.check_progress();
        }
        if self.next_pressure_check <= now {
            self.next_pressure_check = now + Duration::from_millis(PRESSURE_CHECK_MS);
            self.check_pressure();
        }
    }

    fn check_progress(&mut self) {
        for stream in self.skipping.iter().cloned().collect::<Vec<_>>() {
            let mut owed = false;
            let groups: Vec<(u64, Vec<usize>)> = self
                .streams
                .get(&stream)
                .map(|home_stream| {
                    home_stream
                        .groups
                        .values()
                        .map(|group| (group.through_id, group.subs.clone()))
                        .collect()
                })
                .unwrap_or_default();
            for (through_id, subs) in groups {
                for key in subs {
                    owed |= !self.progress(key, through_id, false);
                }
            }
            if !owed {
                self.skipping.remove(&stream);
            }
        }
    }

    /// From an owner: `seq` puts its messages in order, whichever owner sent each.
    pub fn on_stream(&mut self, stream: KeyHash, seq: u64, body: StreamBody) {
        let home_stream = self.streams.entry(stream).or_default();
        if seq != home_stream.next_seq {
            home_stream.reorder.insert(seq, body);
            return;
        }
        home_stream.next_seq += 1;
        self.apply(&stream, body);
        loop {
            let home_stream = self.streams.entry(stream).or_default();
            let next = home_stream.next_seq;
            let Some(body) = home_stream.reorder.remove(&next) else {
                return;
            };
            home_stream.next_seq += 1;
            self.apply(&stream, body);
        }
    }

    fn apply(&mut self, stream: &KeyHash, body: StreamBody) {
        match body {
            StreamBody::Attached {
                sub,
                filter_id,
                head,
                cursor,
                state_ver,
                state,
            } => self.on_attached(stream, sub, filter_id, head, cursor, state_ver, state),
            StreamBody::Fan(fan) => self.on_fan(stream, &fan),
            StreamBody::Deleted => self.on_deleted(stream),
            StreamBody::Evicted => self.on_evicted(stream),
        }
    }

    /// Every subscription on the stream ends with error 2, and the owner's numbering starts
    /// over. What is in `reorder` overtook this, so it belongs to a new stream of this name.
    fn on_deleted(&mut self, stream: &KeyHash) {
        let keys: Vec<(usize, usize, u32, u64)> = self
            .subs
            .iter()
            .enumerate()
            .filter_map(|(key, sub)| {
                let sub = sub.as_ref()?;
                (sub.stream == *stream).then_some((key, sub.conn, sub.id, sub.tag))
            })
            .collect();
        for (key, conn, id, tag) in keys {
            self.send_event(
                conn,
                Event::Error {
                    reference: id,
                    code: ErrorCode::NotFound,
                    msg: "the stream was deleted".to_owned(),
                    ended: Some(tag),
                },
            );
            self.remove_sub(key);
        }
        if let Some(home_stream) = self.streams.get_mut(stream) {
            home_stream.next_seq = 0;
            home_stream.groups.clear();
            home_stream.state_subs.clear();
        }
    }

    /// The stream left RAM with no live subscriber, and its numbering starts over when it
    /// comes back. A subscriber still catching up attaches then, as it would have.
    fn on_evicted(&mut self, stream: &KeyHash) {
        if let Some(home_stream) = self.streams.get_mut(stream) {
            home_stream.next_seq = 0;
        }
    }

    fn say_subscribed(
        &mut self,
        key: usize,
        head: u64,
        cursor: u64,
        state_ver: u64,
        state: Bytes,
        caught_up: bool,
    ) {
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        if sub.subscribed_sent {
            return;
        }
        sub.subscribed_sent = true;
        sub.position = cursor;
        sub.handed_through = cursor;
        sub.state_ver = state_ver;
        let (conn, id) = (sub.conn, sub.id);
        self.send_event(
            conn,
            Event::Subscribed {
                sub: id,
                head,
                state_ver,
                state,
                caught_up,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn on_attached(
        &mut self,
        stream: &KeyHash,
        sub_ref: SubRef,
        filter_id: u32,
        head: u64,
        cursor: u64,
        state_ver: u64,
        state: Bytes,
    ) {
        if self.sub_mut(sub_ref.key, sub_ref.uid).is_none() {
            self.detach_from_owner(stream, filter_id);
            return;
        }
        let key = sub_ref.key;
        self.say_subscribed(key, head, cursor, state_ver, state, true);
        let home_stream = self.streams.entry(*stream).or_default();
        let sub = self.subs[key].as_mut().expect("checked above");
        let was_catching_up = sub.mode == Mode::CatchUp;
        if filter_id == STATE_ONLY_FILTER_ID {
            sub.mode = Mode::StateOnly;
            home_stream.state_subs.push(key);
            return;
        }
        sub.mode = Mode::Live { filter_id };
        sub.position = sub.position.max(cursor);
        home_stream
            .groups
            .entry(filter_id)
            .or_default()
            .subs
            .push(key);
        if was_catching_up {
            // Reaching head: section 5.2's other reason to send it.
            sub.head_progress_owed = true;
            if sub.queue.is_empty() {
                self.send_head_progress(key);
            }
        }
    }

    fn on_fan(&mut self, stream: &KeyHash, fan: &FanOut) {
        let Some(home_stream) = self.streams.get_mut(stream) else {
            return;
        };
        let mut overflowed = Vec::new();
        let mut enqueued = Vec::new();
        for (filter_id, matched) in &fan.matches {
            let Some(group) = home_stream.groups.get(filter_id) else {
                continue;
            };
            for key in &group.subs {
                let Some(Some(sub)) = self.subs.get_mut(*key) else {
                    continue;
                };
                let Some((item, bytes)) = sub.item_after_position(matched) else {
                    continue;
                };
                // With the window open, the end of this event sends the queue, so going
                // over is no sign of a slow client (D97).
                if sub.queue_cost_with(matched, bytes) > self.settings.queue_bytes
                    && !sub.queue.is_empty()
                    && self.conns[sub.conn]
                        .as_ref()
                        .is_none_or(|conn| conn.window_left_bytes <= 0)
                {
                    overflowed.push(*key);
                    continue;
                }
                sub.push_item(item, bytes);
                sub.position = matched.last_id;
                enqueued.push(*key);
            }
        }
        let has_groups = !home_stream.groups.is_empty();
        for group in home_stream.groups.values_mut() {
            group.through_id = group.through_id.max(fan.through_id);
        }
        let state_subs = if fan.state.is_some() {
            home_stream.state_subs.clone()
        } else {
            Vec::new()
        };
        if has_groups {
            self.skipping.insert(*stream);
        }
        for key in enqueued {
            self.after_enqueue(key);
        }
        for key in overflowed {
            self.fall_behind(key);
        }
        if let Some((ver, doc)) = &fan.state {
            for key in state_subs {
                if let Some(Some(sub)) = self.subs.get_mut(key)
                    && *ver > sub.state_ver
                {
                    sub.latest_state = Some((*ver, doc.clone()));
                    self.send_latest_state(key);
                }
            }
        }
    }

    /// State is how things stand now, so a slow client only needs the latest (D88).
    fn send_latest_state(&mut self, key: usize) {
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        let Some(Some(connection)) = self.conns.get_mut(sub.conn) else {
            return;
        };
        if connection.window_left_bytes <= 0 {
            return;
        }
        if let Some((ver, doc)) = sub.latest_state.take() {
            sub.state_ver = ver;
            connection.window_left_bytes -= doc.len() as i64;
            connection.out.push(Event::State {
                sub: sub.id,
                ver,
                doc,
            });
        }
    }

    /// Section 5.2's overflow: what is queued goes, so the bound holds, and the subscription
    /// carries on from what it was last given, by catching up (D86).
    fn fall_behind(&mut self, key: usize) {
        self.leave_group(key);
        let uid = self.next_uid;
        self.next_uid += 1;
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        sub.clear_queue();
        sub.due = false;
        sub.mode = Mode::CatchUp;
        sub.position = sub.handed_through;
        // What a read in flight brings back starts from a position this one has left.
        sub.uid = uid;
        sub.read_out = false;
        sub.timer_armed = false;
        sub.head_progress_owed = false;
        sub.behind_unread = true;
        // The position went back, so the list is walked again from it.
        sub.object = None;
        sub.past = None;
        sub.past_last_id = 0;
        let (conn, id, resume_from) = (sub.conn, sub.id, sub.handed_through);
        self.stats.behind_count += 1;
        self.send_event(
            conn,
            Event::Behind {
                sub: id,
                resume_from,
            },
        );
        self.pump_catch_up(key);
    }

    /// Asks for the next stretch, if the queue has room and the client is reading.
    fn pump_catch_up(&mut self, key: usize) {
        let queue_low_bytes = self.settings.queue_bytes / 2;
        let read_bytes = self.settings.read_bytes;
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        if sub.mode != Mode::CatchUp
            || sub.read_out
            || sub.behind_unread
            || sub.queue_cost_bytes() > queue_low_bytes
        {
            return;
        }
        let window_open = self
            .conns
            .get(sub.conn)
            .and_then(Option::as_ref)
            .is_some_and(|conn| conn.window_left_bytes > 0);
        if !window_open {
            return;
        }
        sub.read_out = true;
        if let Some(progress) = &sub.object {
            let job = ObjectJob {
                stores: self.stores.clone(),
                object: progress.object,
                region: progress.region.clone(),
                next_block: progress.next_block,
                limit: progress.limit,
                filter: sub.filter,
                position: sub.position,
                past: sub.past,
                past_last_id: sub.past_last_id,
                budget_bytes: read_bytes,
                back: self.links.reads[self.me].clone(),
                sub: SubRef {
                    home: self.me,
                    key,
                    uid: sub.uid,
                },
            };
            tokio::spawn(run_object_job(job));
            return;
        }
        let message = ReadStepMsg {
            stream: sub.stream,
            sub: SubRef {
                home: self.me,
                key,
                uid: sub.uid,
            },
            filter: sub.filter,
            after: sub.position,
            read_bytes: std::mem::take(&mut sub.read_bytes_since),
            past: sub.past,
            gone: sub.gone.take(),
        };
        let _ = self.links.reads[self.me].send(ReadMsg::Step(Box::new(message)));
    }

    fn enqueue_read(&mut self, key: usize, matched: Option<Arc<Matched>>) {
        let Some(Some(sub)) = self.subs.get_mut(key) else {
            return;
        };
        if let Some(matched) = matched {
            let bytes = matched.total_bytes;
            sub.push_item(
                QueueItem {
                    matched,
                    run: 0,
                    offset: 0,
                },
                bytes,
            );
            // A read isn't waiting for more, so it doesn't linger.
            sub.due = true;
        }
        self.schedule(key);
    }

    pub fn on_catch(&mut self, sub_ref: SubRef, reply: CatchReply) {
        let key = sub_ref.key;
        let Some(sub) = self.sub_mut(key, sub_ref.uid) else {
            return;
        };
        sub.read_out = false;
        let mut prefetch = None;
        match reply {
            CatchReply::Start {
                head,
                cursor,
                state_ver,
                state,
            } => {
                sub.mode = Mode::CatchUp;
                self.say_subscribed(key, head, cursor, state_ver, state, false);
            }
            CatchReply::Memory { batches, limit } => {
                let filter = range_filter(&sub.filter, sub.position, limit);
                // Nothing came, so nothing is left at or below the limit.
                let mut position = if batches.is_empty() {
                    limit
                } else {
                    sub.position
                };
                let mut matched = Vec::new();
                let mut spans = std::mem::take(&mut self.spans);
                let mut memory_bytes = 0;
                for batch in batches {
                    let bytes = SharedBytes::Slab(batch);
                    walk_records(&bytes, &mut spans);
                    self.stats.memory_bytes += bytes.len() as u64;
                    memory_bytes += bytes.len();
                    if let Some(last) = spans.last() {
                        position = position.max(last.header.last_id());
                    }
                    matched.extend(Matched::select(&bytes, &spans, &filter, &mut 0));
                }
                self.spans = spans;
                // One line per read, as the other tiers have, not one per batch (D213).
                if memory_bytes != 0 {
                    self.trace_read("memory", key, memory_bytes);
                }
                for one in matched {
                    self.account_read(key, one.total_bytes);
                    self.enqueue_read(key, Some(one));
                }
                if let Some(Some(sub)) = self.subs.get_mut(key) {
                    sub.position = position;
                }
            }
            CatchReply::Object {
                object,
                next,
                limit,
            } => {
                prefetch = next;
                sub.object = Some(ObjectProgress {
                    object,
                    region: None,
                    next_block: 0,
                    limit,
                });
            }
            CatchReply::Fail { code, msg } => {
                let (conn, id, tag) = (sub.conn, sub.id, sub.tag);
                self.send_event(
                    conn,
                    Event::Error {
                        reference: id,
                        code,
                        msg,
                        ended: Some(tag),
                    },
                );
                self.remove_sub(key);
                return;
            }
        }
        if let Some(next) = prefetch {
            tokio::spawn(prefetch_pages(self.stores.clone(), next));
        }
        self.progress(key, 0, false);
        self.pump_catch_up(key);
    }

    /// Off unless asked for, because a core shouldn't wait on stderr (D213).
    fn trace_read(&self, tier: &str, key: usize, bytes: usize) {
        if self.settings.trace_catch_up
            && let Some(Some(sub)) = self.subs.get(key)
        {
            eprintln!(
                "trace catch_up tier={tier} bytes={bytes} stream={:?}",
                sub.stream
            );
        }
    }

    fn account_read(&mut self, key: usize, bytes: u64) {
        if let Some(Some(sub)) = self.subs.get_mut(key) {
            sub.read_bytes_since += bytes;
        }
    }

    /// A read task's result.
    pub fn on_read_done(&mut self, sub_ref: SubRef, done: ReadDone) {
        let key = sub_ref.key;
        let Some(sub) = self.sub_mut(key, sub_ref.uid) else {
            return;
        };
        sub.read_out = false;
        match done {
            ReadDone::Retry => {}
            ReadDone::Gone => {
                // The owner knows if a compacted object has these records now.
                sub.gone = sub.object.take().map(|progress| progress.object);
            }
            ReadDone::Fail { code, msg } => {
                let (conn, id, tag) = (sub.conn, sub.id, sub.tag);
                self.send_event(
                    conn,
                    Event::Error {
                        reference: id,
                        code,
                        msg,
                        ended: Some(tag),
                    },
                );
                self.remove_sub(key);
                return;
            }
            ReadDone::Read(read) => {
                let limit = read.limit;
                let after = read
                    .region
                    .as_ref()
                    .map_or(sub.position, |region| region.after);
                let filter = range_filter(&sub.filter, after, limit);
                sub.position = sub.position.max(read.position);
                if let Some(progress) = &mut sub.object {
                    progress.region = read.region.clone();
                    progress.next_block = read.next_block;
                    progress.limit = limit;
                }
                if read.done
                    && let Some(progress) = sub.object.take()
                    && read.passed
                {
                    sub.past = Some(progress.object);
                    if let Some(region) = &read.region {
                        sub.past_last_id = region.extent.id_last;
                    }
                }
                let local = read.from_local;
                self.stats.blocks_read_count += read.blocks_read_count;
                self.stats.blocks_skipped_count += read.blocks_skipped_count;
                if let Some(bytes) = read.bytes {
                    if local {
                        self.stats.local_bytes += bytes.len() as u64;
                        self.trace_read("local", key, bytes.len());
                    } else {
                        self.stats.object_bytes += bytes.len() as u64;
                        self.trace_read("object", key, bytes.len());
                    }
                    self.account_read(key, bytes.len() as u64);
                    let bytes = SharedBytes::Plain(bytes);
                    let mut spans = std::mem::take(&mut self.spans);
                    walk_records(&bytes, &mut spans);
                    let matched = Matched::select(&bytes, &spans, &filter, &mut 0);
                    self.spans = spans;
                    self.enqueue_read(key, matched);
                }
            }
        }
        self.progress(key, 0, false);
        self.pump_catch_up(key);
    }
}

/// The filter, narrowed to what a read may take: whole records above `position`, and
/// nothing not yet visible.
fn range_filter(filter: &Filter, position: u64, limit: u64) -> Filter {
    let mut ranged = *filter;
    ranged.id_first = position.saturating_add(1);
    ranged.id_last = limit;
    ranged
}

/// One read of an object: the stream's region if not found yet, then one run of blocks.
struct ObjectJob {
    stores: Stores,
    object: ObjectRef,
    region: Option<Region>,
    next_block: usize,
    limit: u64,
    filter: Filter,
    position: u64,
    past: Option<ObjectRef>,
    past_last_id: u64,
    budget_bytes: u64,
    back: mpsc::UnboundedSender<ReadMsg>,
    sub: SubRef,
}

/// What came back from one read of an object.
#[derive(Debug)]
pub(crate) struct ObjectRead {
    /// `None` if the stream has no region there.
    pub region: Option<Region>,
    pub next_block: usize,
    /// Everything at or below this id has been looked at.
    pub position: u64,
    /// The read's limit, lowered for a former generation's object (section 8.1).
    pub limit: u64,
    pub done: bool,
    /// Done, and nothing of the object is left above the owner's limit, so the next read goes
    /// past it.
    pub passed: bool,
    pub bytes: Option<Bytes>,
    pub blocks_read_count: u64,
    pub blocks_skipped_count: u64,
    pub from_local: bool,
}

/// The page that may hold the stream's index entry, through the page cache: the top index and
/// the page are fetched only on a miss (section 7.3). `None` if no page can.
async fn stream_page(
    reader: &SegmentReader,
    pages: &PageCache,
    path: &object_store::path::Path,
    stream: KeyHash,
) -> Result<Option<Arc<Vec<storage::IndexEntry>>>, SegmentError> {
    let top = match pages.top(path) {
        Some(top) => top,
        None => {
            let top = Arc::new(reader.read_top_index().await?);
            pages.put_top(path, Arc::clone(&top));
            top
        }
    };
    let Some(number) = top.page_number_for(storage::stream_hash(stream)) else {
        return Ok(None);
    };
    let page = match pages.page(path, number) {
        Some(page) => page,
        None => {
            let page = Arc::new(reader.read_page(&top, number).await?);
            pages.put_page(path, number, Arc::clone(&page));
            page
        }
    };
    Ok(Some(page))
}

/// The stream's region in the object: its page through the cache, then one read for the
/// region's key and table.
async fn find_region(
    reader: &SegmentReader,
    pages: &PageCache,
    path: &object_store::path::Path,
    stream: KeyHash,
) -> Result<Option<(StreamExtent, BlockTable)>, SegmentError> {
    match stream_page(reader, pages, path, stream).await? {
        Some(page) => reader.find_region(&page, stream).await,
        None => Ok(None),
    }
}

/// Fetches the next object's top index and the stream's page into the cache while the current
/// object is read, so moving on costs one region read, not three reads in a row (section 7.3).
/// A failure here costs nothing: the read itself fetches them again.
async fn prefetch_pages(stores: Stores, object: ObjectRef) {
    let path = object.key.path();
    if let Some(local) = &stores.local {
        let reader = SegmentReader::new(Arc::clone(local), path.clone());
        if stream_page(&reader, &stores.pages, &path, object.stream)
            .await
            .is_ok()
        {
            return;
        }
    }
    let reader = SegmentReader::new(Arc::clone(&stores.object), path.clone());
    let _ = stream_page(&reader, &stores.pages, &path, object.stream).await;
}

async fn read_from(
    store: Arc<dyn ObjectStore>,
    job: &ObjectJob,
) -> Result<ObjectRead, SegmentError> {
    let path = job.object.key.path();
    let reader = SegmentReader::new(store, path.clone());
    // A sealed object of a former generation holds what it accepted only (section 8.1).
    let accepted = match job.object.key.class {
        None => job
            .stores
            .ownership
            .limit_for(job.object.key.id.generation())
            .unwrap_or(u64::MAX),
        Some(_) => u64::MAX,
    };
    let limit = job.limit.min(accepted);
    let region = match &job.region {
        Some(region) => region.clone(),
        None => match find_region(&reader, &job.stores.pages, &path, job.object.stream).await? {
            Some((extent, table)) => {
                // A list is in record order, so a former generation's sealed region starting
                // at or below the last one's end was uploaded late. Its records are read too,
                // rather than passed over (section 7.5).
                let late = job.object.key.class.is_none()
                    && job.past.is_some_and(|past| {
                        job.object.key.id.generation() < past.key.id.generation()
                    })
                    && extent.id_first <= job.past_last_id.min(job.position);
                let after = if late {
                    extent.id_first.saturating_sub(1)
                } else {
                    job.position
                };
                Region {
                    extent,
                    table: Arc::new(table),
                    after,
                }
            }
            None => {
                return Ok(ObjectRead {
                    region: None,
                    next_block: 0,
                    position: job.position,
                    limit,
                    done: true,
                    passed: true,
                    bytes: None,
                    blocks_read_count: 0,
                    blocks_skipped_count: 0,
                    from_local: false,
                });
            }
        },
    };
    let filter = range_filter(&job.filter, region.after, limit);
    let blocks = &region.table.blocks;
    let mut position = job.position;
    let mut at = job.next_block;
    let mut skipped = 0;
    let mut done = false;
    // Blocks the summaries rule out cost nothing beyond the table (section 4.1).
    while let Some(block) = blocks.get(at) {
        if block.summary.id_first > limit {
            // A list is in id order, so nothing at or below the limit is left anywhere.
            position = position.max(limit);
            done = true;
            break;
        }
        if filter.decide_block(&block.summary) != Decision::None {
            break;
        }
        skipped += 1;
        position = position.max(block.summary.id_last.min(limit));
        at += 1;
    }
    let start = at;
    let mut bytes = 0;
    while let Some(block) = blocks.get(at) {
        if block.summary.id_first > limit
            || bytes >= job.budget_bytes
            || filter.decide_block(&block.summary) == Decision::None
        {
            break;
        }
        bytes += block.summary.bytes;
        position = position.max(block.summary.id_last.min(limit));
        at += 1;
    }
    let data = if start < at {
        Some(
            reader
                .read_blocks(&region.extent, &region.table, start..at)
                .await?,
        )
    } else {
        None
    };
    if blocks
        .get(at)
        .is_some_and(|block| block.summary.id_first > limit)
    {
        position = position.max(limit);
        done = true;
    }
    done |= at >= blocks.len();
    // What a former generation's object holds above its limit is never read (section 8.1).
    let passed = done && region.extent.id_last.min(accepted) <= job.limit;
    Ok(ObjectRead {
        next_block: at,
        position,
        limit,
        done,
        passed,
        bytes: data,
        blocks_read_count: (at - start) as u64,
        blocks_skipped_count: skipped,
        from_local: false,
        region: Some(region),
    })
}

async fn run_object_job(job: ObjectJob) {
    // A miss or a bad copy in the local tier isn't an error: S3 has it (section 6.2).
    let local_read = match &job.stores.local {
        Some(local) => read_from(Arc::clone(local), &job).await.ok(),
        None => None,
    };
    let done = match local_read {
        Some(mut read) => {
            read.from_local = true;
            ReadDone::Read(read)
        }
        None => match read_from(Arc::clone(&job.stores.object), &job).await {
            Ok(read) => ReadDone::Read(read),
            Err(SegmentError::Store(object_store::Error::NotFound { .. })) => ReadDone::Gone,
            Err(SegmentError::Store(_)) => {
                tokio::time::sleep(Duration::from_millis(READ_RETRY_MS)).await;
                ReadDone::Retry
            }
            Err(error) => ReadDone::Fail {
                code: ErrorCode::Malformed,
                msg: error.to_string(),
            },
        },
    };
    let _ = job.back.send(ReadMsg::Done {
        sub: job.sub,
        done: Box::new(done),
    });
}
