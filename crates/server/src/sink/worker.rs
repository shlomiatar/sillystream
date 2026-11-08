//! One sink's task. It reads its stream from the saved cursor and sends one delivery at a time,
//! moving the cursor past each once it has gone. Anything it hasn't finished when it stops is
//! sent again by the next task, which starts from the cursor.
//!
//! A delivery is everything read and not yet sent, up to the budget, so it grows while the one
//! before it is in flight. What is sent again can be a longer run than before: each record
//! carries its id, and a destination drops those at or below the last it took (section 9).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use format::{Clause, KeyHash, RecordHeader, id_ms};
use object_store::path::Path;
use object_store::{PutMode, PutOptions, PutPayload};
use proto::ErrorCode;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;

use super::webhook::{self, PostError, PostParts, StatusClass, WebhookUrl};
use super::{PoolContext, SinkAction, SinkKey, SinkStep, WorkerExit, backoff_ms};
use crate::connection::{Connection, Event, Subscription};
use crate::delivery::retention_start_id;
use crate::message::{Reply, SinkOp, SinkReport, SinkSpec};
use crate::node::{Client, ClientError};

const CONTENT_RECORDS: &str = "application/x-sillystream-records";
const CONTENT_JSON: &str = "application/json";
/// How often a task with something unread looks again at the stream's retention.
const RETENTION_REFRESH_MS: u64 = 1_000;
/// How long a task waits before asking again after an error it can't act on.
const ERROR_WAIT_MS: u64 = 100;
/// Forwards wait for at least the WAL (section 7.2): a retry is then safe across a crash.
const FORWARD_ACK_LEVEL: u8 = 2;
/// How long a webhook connection is kept with nothing to send. Under the 5 s that common servers
/// keep one, so the other end rarely closes it first.
const IDLE_CONNECTION_MS: u64 = 4_000;

/// A turn from the node's in-flight limit, given back when dropped. A kept connection holds one,
/// since it holds a socket (D295).
struct Turn(Arc<PoolContext>);

impl Turn {
    async fn take(context: &Arc<PoolContext>) -> Option<Turn> {
        let permit = context.in_flight.acquire().await.ok()?;
        permit.forget();
        Some(Turn(Arc::clone(context)))
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        self.0.in_flight.add_permits(1);
    }
}

/// A webhook connection kept between deliveries.
struct Idle {
    conn: webhook::Conn,
    turn: Turn,
    since_ms: u64,
}

type IdleSlot = Arc<std::sync::Mutex<Option<Idle>>>;

/// Runs the sink until it no longer exists or the pool stops.
pub(crate) async fn run(
    context: Arc<PoolContext>,
    key: SinkKey,
    notes: watch::Receiver<u64>,
    exits: mpsc::UnboundedSender<WorkerExit>,
    start_delay_ms: u64,
) {
    // A task that ended abruptly isn't restarted in a tight loop.
    tokio::time::sleep(Duration::from_millis(start_delay_ms)).await;
    // If this task panics, the guard still reports it, and a new one takes over.
    let mut guard = ExitGuard {
        exits,
        key,
        seen_note_count: 0,
        restart: true,
    };
    let home = (key.1 as usize) % context.handle.core_count();
    let client = Arc::new(Mutex::new(context.handle.client(home)));
    let send_client = Arc::new(Mutex::new(context.handle.client(home)));
    let mut worker = Worker {
        context,
        key,
        client,
        send_client,
        notes,
        next_notify_ms: 0,
        epoch: 0,
        update_count: 0,
    };
    let end = worker.run(home, &mut guard.seen_note_count).await;
    guard.restart = end == End::Killed;
}

struct ExitGuard {
    exits: mpsc::UnboundedSender<WorkerExit>,
    key: SinkKey,
    seen_note_count: u64,
    restart: bool,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        let _ = self.exits.send(WorkerExit {
            key: self.key,
            seen_note_count: self.seen_note_count,
            restart: self.restart,
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// The sink is gone, or the node is stopping.
    Done,
    /// As a crash would end it: nothing further was done.
    Killed,
}

/// How a spell of reading ended.
enum Spell {
    /// Look at the spec again.
    Reload,
    End(End),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Notify,
    Deliver,
    Forward,
    Archive,
}

fn kind_of(spec: &SinkSpec) -> Option<Kind> {
    match spec.kind.as_str() {
        "webhook.notify" => Some(Kind::Notify),
        "webhook.deliver" => Some(Kind::Deliver),
        "forward" => Some(Kind::Forward),
        "archive" => Some(Kind::Archive),
        _ => None,
    }
}

/// One record that has been read, or for `webhook.notify` a run of them with only their count.
struct Unread {
    first_id: u64,
    last_id: u64,
    item_count: u64,
    bytes: Bytes,
    /// When the sink read it, by its own clock: `archive` writes what has waited long enough.
    read_ms: u64,
}

/// What is sent as one.
struct Delivery {
    first_id: u64,
    last_id: u64,
    item_count: u64,
    bytes: Vec<Bytes>,
    byte_count: u64,
    attempt: u32,
    next_try_ms: u64,
}

enum Outcome {
    Delivered,
    /// `forward`: the target had it already, from a try before a crash or an update.
    AlreadyThere,
    /// `archive`: an object of the same first id is there already, from a try before a crash,
    /// and holds the records up to this id.
    ArchivedThrough(u64),
    Retry(String),
    Permanent(String),
    Killed,
}

const LOAD_RETRY_MS: u64 = 1_000;

struct Worker {
    context: Arc<PoolContext>,
    key: SinkKey,
    /// For the sink's own ops, which never wait on a delivery.
    client: Arc<Mutex<Client>>,
    /// What deliveries use: a forward holds it until its ack.
    send_client: Arc<Mutex<Client>>,
    notes: watch::Receiver<u64>,
    /// `webhook.notify`'s next allowed send. Held here so an update or a resume doesn't forget it.
    next_notify_ms: u64,
    /// The sink's epoch and spec revision as last read, which its reports carry.
    epoch: u64,
    update_count: u64,
}

impl Worker {
    fn hook(&self, step: SinkStep) -> SinkAction {
        match &self.context.config.hook {
            Some(hook) => hook.at(self.key.1, step),
            None => SinkAction::Continue,
        }
    }

    async fn op(&self, op: SinkOp) -> Result<(), ErrorCode> {
        self.client.lock().await.sink_op(self.key.0, op).await
    }

    async fn report(&self, report: SinkReport) {
        let sink_id = self.key.1;
        let (epoch, update_count) = (self.epoch, self.update_count);
        let _ = self
            .op(SinkOp::Report {
                sink_id,
                epoch,
                update_count,
                report,
            })
            .await;
    }

    async fn run(&mut self, home: usize, seen_note_count: &mut u64) -> End {
        loop {
            *seen_note_count = *self.notes.borrow_and_update();
            let info = self
                .client
                .lock()
                .await
                .sink_info(self.key.0, self.key.1)
                .await;
            let info = match info {
                Ok(info) => info,
                // Its metadata can't be read just now (D243).
                Err(ErrorCode::RateLimited) => {
                    tokio::time::sleep(Duration::from_millis(LOAD_RETRY_MS)).await;
                    continue;
                }
                // No such stream, which a note or a restart can find, or the node stopping.
                Err(_) => return End::Done,
            };
            let Some((_, spec, cursor)) = info
                .sinks
                .into_iter()
                .find(|(sink_id, _, _)| *sink_id == self.key.1)
            else {
                return End::Done;
            };
            self.epoch = spec.epoch;
            self.update_count = spec.update_count;
            let cursor = cursor.unwrap_or(0);
            let retention_ms = info.config.retention_ms;
            let Some(kind) = kind_of(&spec) else {
                return End::Done;
            };
            let spell = if spec.paused {
                self.wait_paused(&spec, cursor, info.head, info.earliest_id, retention_ms)
                    .await
            } else {
                let spell = Reading::new(self, home, spec, kind, cursor, retention_ms);
                match spell {
                    Ok(mut spell) => spell.run().await,
                    Err(reason) => {
                        self.report(SinkReport::Paused { reason }).await;
                        continue;
                    }
                }
            };
            match spell {
                Spell::Reload => {}
                Spell::End(end) => return end,
            }
        }
    }

    /// Delivers nothing. Warns once if what is unread is about to expire, and goes on waiting:
    /// `stop` is what it is doing anyway, and a `skip` happens once it is resumed (D192).
    async fn wait_paused(
        &mut self,
        spec: &SinkSpec,
        mut cursor: u64,
        mut head: u64,
        mut earliest_id: u64,
        mut retention_ms: u64,
    ) -> Spell {
        let check = Duration::from_millis(self.context.config.paused_check_ms);
        let mut warned = spec.expiry_warning_id.is_some();
        loop {
            let now_ms = self.context.clock.now_ms();
            // At cursor 0 nothing has been read, so the oldest unread record is the oldest there
            // is, leaving out what had already expired when the sink began (D201, D239).
            let oldest_id = match cursor {
                0 => earliest_id.max(retention_start_id(spec.epoch, retention_ms)),
                cursor => cursor,
            };
            let unread = match cursor {
                0 => earliest_id != 0 && oldest_id <= head,
                cursor => cursor < head,
            };
            if !warned && unread {
                let expires_ms = id_ms(oldest_id).saturating_add(retention_ms);
                if now_ms.saturating_add(self.context.config.expire_warn_ms) >= expires_ms {
                    warned = true;
                    self.warn(oldest_id, expires_ms).await;
                }
            }
            tokio::select! {
                changed = self.notes.changed() => {
                    return match changed {
                        Ok(()) => Spell::Reload,
                        Err(_) => Spell::End(End::Done),
                    };
                }
                () = tokio::time::sleep(check) => {}
            }
            let info = self
                .client
                .lock()
                .await
                .sink_info(self.key.0, self.key.1)
                .await;
            let Ok(info) = info else {
                return Spell::Reload;
            };
            head = info.head;
            earliest_id = info.earliest_id;
            retention_ms = info.config.retention_ms;
            cursor = info
                .sinks
                .iter()
                .find(|(sink_id, _, _)| *sink_id == self.key.1)
                .and_then(|(_, _, cursor)| *cursor)
                .unwrap_or(cursor);
        }
    }

    async fn warn(&self, first_id: u64, expires_ms: u64) {
        eprintln!(
            "warning: sink {} on {:?} has unread data from id {first_id} that expires at {expires_ms} ms",
            self.key.1, self.key.0
        );
        self.report(SinkReport::Warning {
            first_id: Some(first_id),
        })
        .await;
    }
}

/// One spell of reading under one spec, until the spec changes or the sink pauses itself.
struct Reading<'a> {
    worker: &'a mut Worker,
    home: usize,
    spec: SinkSpec,
    kind: Kind,
    url: Option<WebhookUrl>,
    clauses: Vec<Clause>,
    author: Option<u64>,
    /// The saved position, or further once a commit goes through.
    cursor: u64,
    retention_ms: u64,
    interval_ms: u64,
    budget_bytes: u64,
    sub: u32,
    /// The last id read, past which records are new.
    read_id: u64,
    /// Read past `read_id` with nothing matching, for the cursor once nothing is before it.
    progress_id: u64,
    unread: VecDeque<Unread>,
    /// The one delivery being sent or waiting to be tried again.
    delivery: Option<Delivery>,
    /// Its try in progress, if any.
    tasks: JoinSet<Outcome>,
    /// The webhook connection the last try left open, for the next.
    idle: IdleSlot,
    held_bytes: u64,
    failure_count: u32,
    warned: bool,
    next_retention_refresh_ms: u64,
    /// Where a gap starts, if reading had to jump past what expired.
    gap_from: Option<u64>,
    /// `archive`: what an object written before this spell holds up to, which is read again
    /// and skipped.
    archived_through: u64,
    /// `forward`: one record per publish until the target takes one as new. What was published
    /// before this spell began can be any run of what is read again, and a batch overlapping
    /// its end would be taken whole as new or dropped whole (D189).
    probing: bool,
}

impl<'a> Reading<'a> {
    fn new(
        worker: &'a mut Worker,
        home: usize,
        spec: SinkSpec,
        kind: Kind,
        cursor: u64,
        retention_ms: u64,
    ) -> Result<Reading<'a>, String> {
        let url = match kind {
            Kind::Notify | Kind::Deliver => Some(
                spec.url
                    .as_deref()
                    .and_then(webhook::parse_url)
                    .ok_or_else(|| "the sink's url isn't valid".to_owned())?,
            ),
            _ => None,
        };
        let (clauses, author) = match spec.filter.as_deref() {
            Some(text) => crate::net::http::parse_filter(text)
                .ok_or_else(|| "the sink's filter isn't valid".to_owned())?,
            None => (Vec::new(), None),
        };
        if kind == Kind::Deliver && spec.secret.as_deref().is_none_or(str::is_empty) {
            return Err("webhook.deliver needs a secret".to_owned());
        }
        let config = &worker.context.config;
        let interval_ms = config.notify_interval_ms;
        // A sink can ask for less than the node's, never more: what it holds is outside the
        // memory cap (D185), so the node's figure is what isolates the others from it.
        let budget_bytes = match spec.budget_bytes {
            0 => config.budget_bytes,
            own => own.min(config.budget_bytes),
        };
        Ok(Reading {
            home,
            kind,
            url,
            clauses,
            author,
            cursor,
            retention_ms,
            interval_ms,
            budget_bytes,
            sub: 0,
            read_id: match cursor {
                // Nothing committed yet is still a position: what retention held when the sink
                // began. Asking for 0 would be "whatever is left", and pass over what expires
                // while it is paused or failing with no word to `on_expire` (D201).
                0 => retention_start_id(spec.epoch, retention_ms)
                    .saturating_sub(1)
                    .max(1),
                cursor => cursor,
            },
            progress_id: cursor,
            unread: VecDeque::new(),
            delivery: None,
            tasks: JoinSet::new(),
            idle: IdleSlot::default(),
            held_bytes: 0,
            failure_count: spec.failure_count,
            warned: spec.expiry_warning_id.is_some(),
            next_retention_refresh_ms: 0,
            gap_from: None,
            archived_through: 0,
            probing: true,
            spec,
            worker,
        })
    }

    fn subscribe(&mut self, connection: &Connection, from: u64) {
        self.sub += 1;
        let mut subscription = Subscription::new(self.sub, self.worker.key.0, from);
        subscription.clauses = &self.clauses;
        subscription.author = self.author;
        // System records are never sent on, but reading past them is what closes a batch window
        // whose last stamped id is one: no progress would come for them otherwise.
        subscription.include_system = true;
        connection.subscribe(subscription);
    }

    async fn run(&mut self) -> Spell {
        let Some(mut connection) = self.worker.context.handle.connection(self.home).await else {
            return Spell::End(End::Done);
        };
        self.subscribe(&connection, self.read_id);
        loop {
            let now_ms = self.worker.context.clock.now_ms();
            self.form_delivery(now_ms);
            if let Some(spell) = self.start_due(now_ms).await {
                return spell;
            }
            if let Some(spell) = self.check_expiry(now_ms).await {
                return spell;
            }
            if self.tasks.is_empty() {
                let mut idle = self.idle.lock().expect("no try panics holding it");
                // It gives its turn back once old, or as soon as another sink needs one.
                let crowded = self.worker.context.in_flight.available_permits() == 0;
                if idle.as_ref().is_some_and(|idle| {
                    crowded || now_ms.saturating_sub(idle.since_ms) >= IDLE_CONNECTION_MS
                }) {
                    *idle = None;
                }
            }
            let waiting = self.delivery.is_some() || !self.unread.is_empty();
            let tick = if waiting {
                self.worker.context.config.tick_ms
            } else {
                // Nothing here depends on the clock, and the stream wakes it.
                RETENTION_REFRESH_MS
            };
            tokio::select! {
                // A change first, so that as little as possible is read under the old spec.
                biased;
                changed = self.worker.notes.changed() => {
                    return match changed {
                        Ok(()) => self.finish_in_flight(Spell::Reload).await,
                        Err(_) => Spell::End(End::Done),
                    };
                }
                Some(joined) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(spell) = self.on_outcome(joined).await {
                        return spell;
                    }
                }
                event = connection.recv(), if self.held_bytes < self.budget_bytes => {
                    let Some(event) = event else {
                        return Spell::End(End::Done);
                    };
                    if let Some(spell) = self.on_event(event, &connection).await {
                        return spell;
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(tick)) => {}
            }
        }
    }

    async fn on_event(&mut self, event: Event, connection: &Connection) -> Option<Spell> {
        match event {
            Event::Data { sub, records } if sub == self.sub => {
                let bytes = Bytes::from(records.to_vec());
                if let Err(reason) = self.take_records(&bytes).await {
                    self.worker.report(SinkReport::Paused { reason }).await;
                    return Some(self.finish_in_flight(Spell::Reload).await);
                }
            }
            Event::Subscribed { sub, head, .. } if sub == self.sub => {
                // A jump past what expired with nothing left after it: no record or progress
                // will come to close the gap, since this read starts above the head.
                let now_ms = self.worker.context.clock.now_ms();
                if let Some(first_id) = self.gap_from
                    && head >= first_id
                    && head < retention_start_id(now_ms, self.retention_ms)
                {
                    self.gap_from = None;
                    self.read_id = self.read_id.max(head);
                    self.progress_id = self.progress_id.max(head);
                    self.record_gap(first_id, head).await;
                    self.commit_progress().await;
                }
            }
            Event::Progress { sub, id } if sub == self.sub => {
                self.read_id = self.read_id.max(id);
                self.progress_id = self.progress_id.max(id);
                // Progress short of the gap's start says nothing about the gap yet.
                if let Some(first_id) = self.gap_from
                    && id >= first_id
                {
                    self.gap_from = None;
                    self.record_gap(first_id, id).await;
                }
                self.commit_progress().await;
            }
            Event::Error {
                reference,
                code: ErrorCode::RetentionExpired,
                ..
            } if reference == self.sub => {
                return self.on_expired_read(connection).await;
            }
            Event::Error {
                reference,
                code,
                msg,
                ..
            } if reference == self.sub => {
                if code == ErrorCode::NotFound {
                    return Some(self.finish_in_flight(Spell::Reload).await);
                }
                // Nothing a retry of the same read can do better than a fresh look.
                eprintln!(
                    "sink {} on {:?}: reading gave error {code:?}: {msg:?}",
                    self.worker.key.1, self.worker.key.0
                );
                tokio::time::sleep(Duration::from_millis(ERROR_WAIT_MS)).await;
                return Some(self.finish_in_flight(Spell::Reload).await);
            }
            _ => {}
        }
        None
    }

    /// Takes a copy of each new record, reading headers alone (section 2.2).
    async fn take_records(&mut self, bytes: &Bytes) -> Result<(), String> {
        let read_ms = self.worker.context.clock.now_ms();
        let mut at = 0;
        while at < bytes.len() {
            let header =
                RecordHeader::read(&bytes[at..]).map_err(|error| format!("reading: {error}"))?;
            let total = header.total_bytes();
            let end = at + total;
            if end > bytes.len() {
                return Err("reading: a record runs off its frame".to_owned());
            }
            let (first_id, last_id) = (header.id, header.last_id());
            // A system record isn't sent on, but it has been read: a batch window that closed at
            // its id is complete once reading is past it.
            if last_id > self.read_id && header.is_system() {
                self.read_id = last_id;
            }
            // After a `BEHIND`, what has already been read can come again.
            if last_id > self.read_id && !header.is_system() {
                self.read_id = last_id;
                if let Some(gap_from) = self.gap_from.take()
                    && first_id > gap_from
                {
                    self.record_gap(gap_from, first_id - 1).await;
                }
                let item_count = u64::from(header.count);
                if self.kind == Kind::Notify {
                    // Only the count matters, so a burst holds one entry and no bytes.
                    match self.unread.back_mut() {
                        Some(run) => {
                            run.last_id = last_id;
                            run.item_count += item_count;
                        }
                        None => self.unread.push_back(Unread {
                            first_id,
                            last_id,
                            item_count,
                            bytes: Bytes::new(),
                            read_ms,
                        }),
                    }
                } else {
                    self.held_bytes += total as u64;
                    if let Some(hook) = &self.worker.context.config.hook {
                        hook.held(self.worker.key.1, self.held_bytes);
                    }
                    self.unread.push_back(Unread {
                        first_id,
                        last_id,
                        item_count,
                        bytes: bytes.slice(at..end),
                        read_ms,
                    });
                }
            }
            at = end;
        }
        Ok(())
    }

    fn new_delivery(&self, first: &Unread, now_ms: u64) -> Delivery {
        Delivery {
            first_id: first.first_id,
            last_id: first.last_id,
            item_count: first.item_count,
            bytes: if first.bytes.is_empty() {
                Vec::new()
            } else {
                vec![first.bytes.clone()]
            },
            byte_count: first.bytes.len() as u64,
            attempt: 0,
            next_try_ms: now_ms,
        }
    }

    fn form_delivery(&mut self, now_ms: u64) {
        while let Some(front) = self.unread.front()
            && front.last_id <= self.archived_through
        {
            self.held_bytes = self.held_bytes.saturating_sub(front.bytes.len() as u64);
            self.unread.pop_front();
        }
        if self.delivery.is_some() {
            return;
        }
        let Some(first) = self.unread.front() else {
            return;
        };
        let delivery = match self.kind {
            Kind::Notify => {
                if now_ms < self.worker.next_notify_ms {
                    return;
                }
                self.worker.next_notify_ms = now_ms.saturating_add(self.interval_ms);
                self.new_delivery(first, now_ms)
            }
            Kind::Forward if self.probing => self.new_delivery(first, now_ms),
            Kind::Archive => match self.archive_batch(now_ms) {
                Some(delivery) => delivery,
                None => return,
            },
            Kind::Forward | Kind::Deliver => self.greedy_batch(now_ms),
        };
        let take = match self.kind {
            Kind::Notify => 1,
            _ => delivery.bytes.len(),
        };
        self.unread.drain(..take);
        self.delivery = Some(delivery);
    }

    /// Everything unread, up to the budget, as one publish or POST. A batch forms while the one
    /// before it is in flight, so it is as big as the destination is slow, and waits on no clock.
    fn greedy_batch(&self, now_ms: u64) -> Delivery {
        let mut records = self.unread.iter();
        let first = records.next().expect("a batch starts at an unread record");
        let mut delivery = self.new_delivery(first, now_ms);
        for next in records {
            if delivery.byte_count + next.bytes.len() as u64 > self.budget_bytes {
                break;
            }
            delivery.last_id = next.last_id;
            delivery.item_count += next.item_count;
            delivery.byte_count += next.bytes.len() as u64;
            delivery.bytes.push(next.bytes.clone());
        }
        delivery
    }

    /// `archive`: everything unread, up to the budget, once that fills it or its oldest record
    /// has waited the flush time (section 9). Nothing in it depends on the clock but when.
    fn archive_batch(&self, now_ms: u64) -> Option<Delivery> {
        let first = self.unread.front()?;
        let delivery = self.greedy_batch(now_ms);
        let full =
            delivery.byte_count >= self.budget_bytes || delivery.bytes.len() < self.unread.len();
        let flush_ms = self.worker.context.config.archive_flush_ms;
        let due = now_ms >= first.read_ms.saturating_add(flush_ms);
        (full || due).then_some(delivery)
    }

    async fn start_due(&mut self, now_ms: u64) -> Option<Spell> {
        if !self.tasks.is_empty()
            || self
                .delivery
                .as_ref()
                .is_none_or(|delivery| delivery.next_try_ms > now_ms)
        {
            return None;
        }
        storage::crash_point("sink.before_delivery");
        if self.worker.hook(SinkStep::BeforeDelivery) == SinkAction::Kill {
            return Some(Spell::End(End::Killed));
        }
        let delivery = self.delivery.as_mut()?;
        if self.kind == Kind::Notify
            && let Some(run) = self.unread.pop_front()
        {
            // `unread` is everything since the last success, retries included (D187).
            delivery.last_id = run.last_id;
            delivery.item_count += run.item_count;
        }
        delivery.attempt += 1;
        let attempt = Attempt {
            context: Arc::clone(&self.worker.context),
            client: Arc::clone(&self.worker.send_client),
            stream: self.worker.key.0,
            sink_id: self.worker.key.1,
            kind: self.kind,
            spec: self.spec.clone(),
            url: self.url.clone(),
            idle: Arc::clone(&self.idle),
            first_id: delivery.first_id,
            last_id: delivery.last_id,
            item_count: delivery.item_count,
            bytes: delivery.bytes.clone(),
        };
        self.tasks.spawn(attempt.run());
        None
    }

    async fn on_outcome(
        &mut self,
        joined: Result<Outcome, tokio::task::JoinError>,
    ) -> Option<Spell> {
        let Ok(outcome) = joined else {
            return Some(Spell::End(End::Killed));
        };
        let outcome = match outcome {
            Outcome::ArchivedThrough(through) => match self.archived_part(through).await {
                Some(outcome) => outcome,
                None => return None,
            },
            other => other,
        };
        match outcome {
            Outcome::ArchivedThrough(_) => unreachable!("taken apart above"),
            Outcome::Delivered | Outcome::AlreadyThere => {
                if matches!(outcome, Outcome::Delivered) {
                    // Taken as new: everything after it is past what an earlier try published.
                    self.probing = false;
                }
                storage::crash_point("sink.after_delivery");
                if self.worker.hook(SinkStep::AfterDelivery) == SinkAction::Kill {
                    return Some(Spell::End(End::Killed));
                }
                if self.failure_count > 0 {
                    self.failure_count = 0;
                    self.worker.report(SinkReport::Healthy).await;
                }
                if self.commit_delivered().await.is_none() {
                    return Some(Spell::End(End::Killed));
                }
                self.commit_progress().await;
            }
            Outcome::Killed => return Some(Spell::End(End::Killed)),
            Outcome::Retry(error) => {
                self.failure_count += 1;
                let now_ms = self.worker.context.clock.now_ms();
                let config = &self.worker.context.config;
                let delivery = self.delivery.as_mut()?;
                let mut wait_ms = backoff_ms(
                    config.retry_initial_ms,
                    config.retry_max_ms,
                    delivery.attempt,
                );
                if self.kind == Kind::Notify {
                    // A retry is a send, and the rate is on sends.
                    wait_ms = wait_ms.max(self.interval_ms);
                    self.worker.next_notify_ms = now_ms.saturating_add(wait_ms);
                }
                delivery.next_try_ms = now_ms.saturating_add(wait_ms);
                let max_count = config.max_failure_count;
                let count = self.failure_count;
                self.worker
                    .report(SinkReport::Failure {
                        count,
                        error: error.clone(),
                    })
                    .await;
                if count >= max_count {
                    let reason = format!("{count} failures in a row, the last: {error}");
                    self.worker.report(SinkReport::Paused { reason }).await;
                    return Some(self.finish_in_flight(Spell::Reload).await);
                }
            }
            Outcome::Permanent(reason) => {
                self.worker
                    .report(SinkReport::Failure {
                        count: self.failure_count + 1,
                        error: reason.clone(),
                    })
                    .await;
                self.worker.report(SinkReport::Paused { reason }).await;
                return Some(self.finish_in_flight(Spell::Reload).await);
            }
        }
        None
    }

    /// An object already held this delivery's first records, up to `through`. All of them, or
    /// more: the delivery is done, and what else it holds is skipped as it is read. Fewer: the
    /// cursor moves past them, and the rest goes as a new object at once.
    async fn archived_part(&mut self, through: u64) -> Option<Outcome> {
        let delivery = self.delivery.as_mut()?;
        if through >= delivery.last_id {
            delivery.last_id = through;
            self.archived_through = self.archived_through.max(through);
            return Some(Outcome::AlreadyThere);
        }
        let mut dropped_bytes = 0;
        while let Some(part) = delivery.bytes.first() {
            let header = format::RecordHeader::read(part).ok()?;
            if header.last_id() > through {
                delivery.first_id = header.id;
                break;
            }
            dropped_bytes += part.len() as u64;
            delivery.item_count -= u64::from(header.count);
            delivery.bytes.remove(0);
        }
        delivery.byte_count -= dropped_bytes;
        delivery.next_try_ms = self.worker.context.clock.now_ms();
        self.held_bytes = self.held_bytes.saturating_sub(dropped_bytes);
        self.commit(through).await;
        None
    }

    /// Moves the cursor past the delivery that has gone. `None` to stop.
    async fn commit_delivered(&mut self) -> Option<()> {
        let Some(delivery) = self.delivery.take() else {
            return Some(());
        };
        self.held_bytes = self.held_bytes.saturating_sub(delivery.byte_count);
        self.commit(delivery.last_id).await;
        storage::crash_point("sink.after_cursor");
        if self.worker.hook(SinkStep::AfterCursor) == SinkAction::Kill {
            return None;
        }
        Some(())
    }

    async fn commit(&mut self, id: u64) {
        self.cursor = self.cursor.max(id);
        let op = SinkOp::Commit {
            sink_id: self.worker.key.1,
            id,
            epoch: self.spec.epoch,
            update_count: self.spec.update_count,
        };
        let _ = self.worker.op(op).await;
    }

    /// Progress can move the cursor only with nothing read and not yet delivered before it.
    async fn commit_progress(&mut self) {
        if self.unread.is_empty() && self.delivery.is_none() && self.progress_id > self.cursor {
            self.commit(self.progress_id).await;
        }
    }

    async fn record_gap(&mut self, first_id: u64, last_id: u64) {
        eprintln!(
            "sink {} on {:?} skipped ids {first_id} to {last_id}, which expired unread",
            self.worker.key.1, self.worker.key.0
        );
        self.worker
            .report(SinkReport::Gap { first_id, last_id })
            .await;
    }

    /// What is in progress finishes, and goes into the cursor if it went.
    async fn finish_in_flight(&mut self, then: Spell) -> Spell {
        let mut delivered = false;
        while let Some(joined) = self.tasks.join_next().await {
            delivered |= matches!(joined, Ok(Outcome::Delivered));
        }
        if delivered && self.commit_delivered().await.is_none() {
            return Spell::End(End::Killed);
        }
        then
    }

    /// The read itself says data has gone from under the position: section 9's `on_expire`.
    async fn on_expired_read(&mut self, connection: &Connection) -> Option<Spell> {
        let from = self.read_id;
        if self.spec.on_expire == "skip" {
            // What is already held still goes. Reading carries on from what is left.
            self.gap_from = Some(from.saturating_add(1));
            self.subscribe(connection, 0);
            return None;
        }
        let reason = format!("unread data after id {from} has expired");
        self.worker.report(SinkReport::Paused { reason }).await;
        Some(self.finish_in_flight(Spell::Reload).await)
    }

    /// Warns of what is first in line, then follows `on_expire` once it has expired by the clock.
    async fn check_expiry(&mut self, now_ms: u64) -> Option<Spell> {
        let first_id = match (&self.delivery, self.unread.front()) {
            (Some(delivery), _) => delivery.first_id,
            (None, Some(unread)) => unread.first_id,
            (None, None) => {
                if self.warned {
                    self.warned = false;
                    self.worker
                        .report(SinkReport::Warning { first_id: None })
                        .await;
                }
                return None;
            }
        };
        if now_ms >= self.next_retention_refresh_ms {
            self.next_retention_refresh_ms = now_ms.saturating_add(RETENTION_REFRESH_MS);
            let info = self
                .worker
                .client
                .lock()
                .await
                .sink_info(self.worker.key.0, self.worker.key.1)
                .await;
            if let Ok(info) = info {
                self.retention_ms = info.config.retention_ms;
            }
        }
        let expires_ms = id_ms(first_id).saturating_add(self.retention_ms);
        let warn_ms = self.worker.context.config.expire_warn_ms;
        if now_ms < expires_ms {
            let soon = now_ms.saturating_add(warn_ms) >= expires_ms;
            if soon && !self.warned {
                self.warned = true;
                self.worker.warn(first_id, expires_ms).await;
            } else if !soon && self.warned {
                self.warned = false;
                self.worker
                    .report(SinkReport::Warning { first_id: None })
                    .await;
            }
            return None;
        }
        if self.spec.on_expire != "skip" {
            let reason = format!("unread data from id {first_id} has expired");
            self.worker.report(SinkReport::Paused { reason }).await;
            return Some(self.finish_in_flight(Spell::Reload).await);
        }
        self.skip_expired(now_ms).await
    }

    /// Passes over what is first in line once all of it has expired: a notify's run and an
    /// archive's batch can hold younger ids, which aren't a gap (D208). A request in progress is
    /// let finish.
    async fn skip_expired(&mut self, now_ms: u64) -> Option<Spell> {
        let expired =
            |last_id: u64, retention_ms: u64| id_ms(last_id).saturating_add(retention_ms) <= now_ms;
        let retention_ms = self.retention_ms;
        let mut gap: Option<(u64, u64)> = None;
        let extend = |gap: &mut Option<(u64, u64)>, first_id: u64, last_id: u64| {
            *gap = Some(gap.map_or((first_id, last_id), |(first, _)| (first, last_id)));
        };
        if self.tasks.is_empty()
            && let Some(delivery) = self
                .delivery
                .take_if(|delivery| expired(delivery.last_id, retention_ms))
        {
            self.held_bytes = self.held_bytes.saturating_sub(delivery.byte_count);
            extend(&mut gap, delivery.first_id, delivery.last_id);
        }
        if self.delivery.is_none() {
            while let Some(front) = self.unread.front() {
                if !expired(front.last_id, retention_ms) {
                    break;
                }
                let unread = self.unread.pop_front()?;
                self.held_bytes = self.held_bytes.saturating_sub(unread.bytes.len() as u64);
                extend(&mut gap, unread.first_id, unread.last_id);
            }
        }
        if let Some((first_id, last_id)) = gap {
            self.record_gap(first_id, last_id).await;
            self.commit(last_id).await;
            if self.warned {
                self.warned = false;
                self.worker
                    .report(SinkReport::Warning { first_id: None })
                    .await;
            }
        }
        None
    }
}

/// One try at one delivery, on its own task.
struct Attempt {
    context: Arc<PoolContext>,
    client: Arc<Mutex<Client>>,
    stream: KeyHash,
    sink_id: u64,
    kind: Kind,
    spec: SinkSpec,
    url: Option<WebhookUrl>,
    idle: IdleSlot,
    first_id: u64,
    last_id: u64,
    item_count: u64,
    bytes: Vec<Bytes>,
}

impl Attempt {
    async fn run(self) -> Outcome {
        match self.kind {
            Kind::Forward => self.forward().await,
            Kind::Archive => {
                // Before the attempt's own timeout starts, and held for all of it. A forward only
                // waits on a core and holds no socket, so a stalled webhook has no reason to hold
                // it up.
                let _turn = Turn::take(&self.context).await;
                self.archive().await
            }
            Kind::Deliver | Kind::Notify => {
                let idle = self.idle.lock().expect("no try panics holding it").take();
                let (turn, reuse) = match idle {
                    Some(idle) => (Some(idle.turn), Some(idle.conn)),
                    None => (Turn::take(&self.context).await, None),
                };
                let (outcome, kept) = match self.kind {
                    Kind::Deliver => self.deliver(reuse).await,
                    _ => self.notify(reuse).await,
                };
                if let (Some(conn), Some(turn)) = (kept, turn) {
                    let since_ms = self.context.clock.now_ms();
                    *self.idle.lock().expect("no try panics holding it") = Some(Idle {
                        conn,
                        turn,
                        since_ms,
                    });
                }
                outcome
            }
        }
    }

    async fn post(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: &[u8],
        signature: Option<String>,
        reuse: Option<webhook::Conn>,
    ) -> (Outcome, Option<webhook::Conn>) {
        let Some(url) = &self.url else {
            return (Outcome::Permanent("the sink has no url".to_owned()), None);
        };
        let mut headers = vec![
            ("X-Sink-Id", self.sink_id.to_string()),
            ("X-First-Id", self.first_id.to_string()),
            ("X-Last-Id", self.last_id.to_string()),
        ];
        if let Some(signature) = signature {
            headers.push(("X-Signature", signature));
        }
        let config = &self.context.config;
        let (result, kept) = webhook::post(
            PostParts {
                url,
                content_type,
                content_encoding,
                headers: &headers,
                body,
                allow_private: config.allow_private_addresses,
                resolver: config.resolver.as_ref(),
                tls: &self.context.tls,
                timeout_ms: config.request_timeout_ms,
            },
            reuse,
        )
        .await;
        let outcome = match result {
            Ok(status) => match webhook::classify_status(status) {
                StatusClass::Delivered => Outcome::Delivered,
                StatusClass::Retry => Outcome::Retry(format!("status {status}")),
                StatusClass::Permanent => Outcome::Permanent(format!("status {status}")),
            },
            Err(PostError::Blocked(reason)) => Outcome::Permanent(reason),
            Err(PostError::Failed(reason)) => Outcome::Retry(reason),
        };
        (outcome, kept)
    }

    /// The batch's records as stored, one after another, each with its id in its header, and
    /// compressed if the sink asks. The signature is of the bytes sent, so a destination checks
    /// it before it decompresses anything. Both are work for a whole core, so they are done off
    /// the pool's threads.
    async fn deliver(&self, reuse: Option<webhook::Conn>) -> (Outcome, Option<webhook::Conn>) {
        if self.bytes.is_empty() {
            return (Outcome::Delivered, reuse);
        }
        let (parts, compression) = (self.bytes.clone(), self.spec.compression.clone());
        let secret = self.spec.secret.clone();
        let built = tokio::task::spawn_blocking(move || {
            let body = webhook::encode_body(&parts, compression.as_deref())?;
            let signature = secret.map(|secret| webhook::signature_hex(&secret, &body));
            Ok::<_, std::io::Error>((body, signature))
        })
        .await;
        let (body, signature) = match built {
            Ok(Ok(built)) => built,
            Ok(Err(error)) => return (Outcome::Permanent(format!("the body: {error}")), None),
            Err(_) => return (Outcome::Killed, None),
        };
        let encoding = self.spec.compression.as_deref();
        self.post(CONTENT_RECORDS, encoding, &body, signature, reuse)
            .await
    }

    /// `{stream, head, unread}`, with ids as strings like everywhere else in JSON (D111).
    async fn notify(&self, reuse: Option<webhook::Conn>) -> (Outcome, Option<webhook::Conn>) {
        let head = match self
            .client
            .lock()
            .await
            .sink_info(self.stream, self.sink_id)
            .await
        {
            Ok(info) => info.head,
            Err(code) => return (Outcome::Retry(format!("reading the head: {code:?}")), reuse),
        };
        let body = format!(
            "{{\"sink_id\":\"{}\",\"head\":\"{head}\",\"unread\":{}}}",
            self.sink_id, self.item_count
        );
        let signature = self
            .spec
            .secret
            .as_ref()
            .map(|secret| webhook::signature_hex(secret, body.as_bytes()));
        self.post(CONTENT_JSON, None, body.as_bytes(), signature, reuse)
            .await
    }

    /// One publish per batch, whose seq is the source id of its last record. The target drops a
    /// seq at or below the highest it has taken, so a record it has is never taken again once
    /// probing has found where it stopped.
    async fn forward(&self) -> Outcome {
        if self.bytes.is_empty() {
            return Outcome::Delivered;
        }
        let Some(target) = self.spec.target else {
            return Outcome::Permanent("the sink has no target".to_owned());
        };
        let wait = Duration::from_millis(self.context.config.request_timeout_ms);
        let mut client = self.client.lock().await;
        match client
            .publish_forward(
                target,
                &self.bytes,
                format::forward_producer_id(self.stream, self.sink_id),
                self.last_id,
                FORWARD_ACK_LEVEL,
            )
            .await
        {
            Ok(()) => {}
            Err(ClientError::Full | ClientError::NodeStopped) => {
                return Outcome::Retry("the node took no more".to_owned());
            }
        }
        storage::crash_point("sink.after_publish");
        if let Some(hook) = &self.context.config.hook
            && hook.at(self.sink_id, SinkStep::AfterPublish) == SinkAction::Kill
        {
            return Outcome::Killed;
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let reply = tokio::time::timeout_at(deadline, client.recv()).await;
            match reply {
                Err(_) => return Outcome::Retry("waiting for the ack timed out".to_owned()),
                Ok(None) => return Outcome::Retry("the node has stopped".to_owned()),
                // An answer to an earlier try that timed out.
                // `first_id = 0`: an older seq than the target's latest, so it has the records.
                Ok(Some(Reply::Ack {
                    producer_seq,
                    first_id,
                    ..
                })) if producer_seq == self.last_id => {
                    return if first_id == 0 {
                        Outcome::AlreadyThere
                    } else {
                        Outcome::Delivered
                    };
                }
                Ok(Some(Reply::Nack {
                    producer_seq,
                    code,
                    msg,
                    ..
                })) if producer_seq == self.last_id => {
                    return if code == ErrorCode::RateLimited {
                        Outcome::Retry(msg)
                    } else {
                        Outcome::Permanent(format!("the target refused it: {msg}"))
                    };
                }
                Ok(Some(Reply::Error { code, msg, .. })) => {
                    // Possibly in the file (D79). The same seq is safe to send again.
                    return Outcome::Retry(format!("{code:?}: {msg}"));
                }
                Ok(Some(_)) => {}
            }
        }
    }

    async fn archive(&self) -> Outcome {
        let path = Path::from_iter([
            "archive".to_owned(),
            self.stream.to_string(),
            self.sink_id.to_string(),
            format!("{:020}.records", self.first_id),
        ]);
        let payload = PutPayload::from_iter(self.bytes.iter().cloned());
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let wait = Duration::from_millis(self.context.config.request_timeout_ms);
        let put = self.context.archive.put_opts(&path, payload, options);
        match tokio::time::timeout(wait, put).await {
            Ok(Ok(_)) => Outcome::Delivered,
            Ok(Err(object_store::Error::AlreadyExists { .. })) => self.archive_taken(&path).await,
            Ok(Err(object_store::Error::InvalidPath { source })) => {
                Outcome::Permanent(format!("archive path: {source}"))
            }
            Ok(Err(error)) => Outcome::Retry(format!("archive: {error}")),
            Err(_) => Outcome::Retry("the archive put timed out".to_owned()),
        }
    }

    /// The name is taken. By these same bytes, a repeat whose response was lost. By others, the
    /// same ends under a different filter, and these records would go nowhere (D202).
    async fn archive_taken(&self, path: &Path) -> Outcome {
        let wait = Duration::from_millis(self.context.config.request_timeout_ms);
        let read = async { self.context.archive.get(path).await?.bytes().await };
        let existing = match tokio::time::timeout(wait, read).await {
            Ok(Ok(existing)) => existing,
            Ok(Err(error)) => return Outcome::Retry(format!("archive: {error}")),
            Err(_) => return Outcome::Retry("reading the archive object timed out".to_owned()),
        };
        // One run of the stream's records begins at this first id, so of this delivery and what
        // a try before a crash wrote, one is the start of the other.
        let mut left = &existing[..];
        let mut same = true;
        for part in &self.bytes {
            if left.is_empty() {
                break;
            }
            if !left.starts_with(part) {
                same = false;
                break;
            }
            left = &left[part.len()..];
        }
        let through = same
            .then(|| {
                format::Records::new(&existing)
                    .map(|record| record.map(|record| record.header.last_id()))
                    .collect::<Result<Vec<u64>, _>>()
                    .ok()
                    .and_then(|ids| ids.last().copied())
            })
            .flatten();
        match through {
            Some(through) if through == self.last_id => Outcome::Delivered,
            Some(through) => Outcome::ArchivedThrough(through),
            None => Outcome::Permanent(format!(
                "{path} already holds different records from this id, as a changed filter can \
                 make; use another sink id to archive these"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_its_limit() {
        let delays: Vec<u64> = (1..8)
            .map(|attempt| backoff_ms(100, 1_000, attempt))
            .collect();
        assert_eq!(delays, [100, 200, 400, 800, 1000, 1000, 1000]);
        assert_eq!(backoff_ms(100, u64::MAX, 200), u64::MAX);
    }
}
