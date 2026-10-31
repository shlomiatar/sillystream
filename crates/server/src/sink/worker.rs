//! One sink's task. It reads its stream from the saved cursor, forms deliveries, sends them,
//! and moves the cursor once what comes before them has gone too. Anything it hasn't finished
//! when it stops is sent again by the next task, which starts from the cursor.
//!
//! A delivery is one whole record for `webhook.deliver` and `forward`, so what is sent again has
//! the same `{sink_id, first_id, last_id}` however much more has since arrived (D189).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use format::{Clause, RecordHeader, id_ms};
use object_store::path::Path;
use object_store::{PutMode, PutOptions, PutPayload};
use proto::ErrorCode;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;

use super::webhook::{self, PostError, PostParts, StatusClass, WebhookUrl};
use super::{ForwardClaim, PoolContext, SinkAction, SinkKey, SinkStep, WorkerExit, backoff_ms};
use crate::connection::{Connection, Event, Subscription};
use crate::delivery::retention_start_id;
use crate::message::{Reply, SinkOp, SinkReport, SinkSpec};
use crate::net::json::quoted;
use crate::node::{Client, ClientError};

const CONTENT_RECORDS: &str = "application/x-sillystream-records";
const CONTENT_JSON: &str = "application/json";
/// How often a task with something unread looks again at the stream's retention.
const RETENTION_REFRESH_MS: u64 = 1_000;
/// How long a task waits before asking again after an error it can't act on.
const ERROR_WAIT_MS: u64 = 100;
/// Forwards wait for at least the WAL (section 7.2): a retry is then safe across a crash.
const FORWARD_ACK_LEVEL: u8 = 2;

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
        key: key.clone(),
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
            key: self.key.clone(),
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
}

/// What is sent as one, in order among the others.
struct Delivery {
    first_id: u64,
    last_id: u64,
    item_count: u64,
    bytes: Vec<Bytes>,
    byte_count: u64,
    attempt: u32,
    next_try_ms: u64,
    running: bool,
    done: bool,
}

enum Outcome {
    Delivered,
    Retry(String),
    Permanent(String),
    Killed,
}

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
}

impl Worker {
    fn hook(&self, step: SinkStep) -> SinkAction {
        match &self.context.config.hook {
            Some(hook) => hook.at(self.key.1, step),
            None => SinkAction::Continue,
        }
    }

    /// Takes `(target, sink id)` for this source. False if another source holds it.
    fn take_claim(&self, target: &str) -> bool {
        let mut claims = self
            .context
            .forward_claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let claim = claims
            .entry((target.to_owned(), self.key.1))
            .or_insert_with(|| ForwardClaim {
                source: self.key.0.to_string(),
                acked_seq: 0,
            });
        *claim.source == *self.key.0
    }

    async fn op(&self, op: SinkOp) -> Result<(), ErrorCode> {
        self.client.lock().await.sink_op(&self.key.0, op).await
    }

    async fn report(&self, report: SinkReport) {
        let sink_id = self.key.1;
        let _ = self.op(SinkOp::Report { sink_id, report }).await;
    }

    async fn run(&mut self, home: usize, seen_note_count: &mut u64) -> End {
        loop {
            *seen_note_count = *self.notes.borrow_and_update();
            let info = self.client.lock().await.stream_info(&self.key.0).await;
            let Ok(info) = info else {
                // No such stream, which a note or a restart can find, or the node stopping.
                return End::Done;
            };
            let Some((_, spec, cursor)) = info
                .sinks
                .into_iter()
                .find(|(sink_id, _, _)| *sink_id == self.key.1)
            else {
                return End::Done;
            };
            let cursor = cursor.unwrap_or(0);
            let retention_ms = info.config.retention_ms;
            let Some(kind) = kind_of(&spec) else {
                return End::Done;
            };
            let spell = if spec.paused {
                self.wait_paused(&spec, cursor, info.head, retention_ms)
                    .await
            } else {
                if kind == Kind::Forward {
                    let target = spec.target.clone().unwrap_or_default();
                    if !self.take_claim(&target) {
                        let reason = format!(
                            "another stream forwards to {target} with sink id {}, so one's \
                             records would be dropped as the other's duplicates",
                            self.key.1
                        );
                        self.report(SinkReport::Paused { reason }).await;
                        continue;
                    }
                }
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
        mut retention_ms: u64,
    ) -> Spell {
        let check = Duration::from_millis(self.context.config.paused_check_ms);
        let mut warned = spec.expiry_warning_id.is_some();
        loop {
            let now_ms = self.context.clock.now_ms();
            if !warned && cursor != 0 && cursor < head {
                let expires_ms = id_ms(cursor).saturating_add(retention_ms);
                if now_ms.saturating_add(self.context.config.expire_warn_ms) >= expires_ms {
                    warned = true;
                    self.warn(cursor, expires_ms).await;
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
            let info = self.client.lock().await.stream_info(&self.key.0).await;
            let Ok(info) = info else {
                return Spell::Reload;
            };
            head = info.head;
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
            "warning: sink {} on {} has unread data from id {first_id} that expires at {expires_ms} ms",
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
    deliveries: VecDeque<Delivery>,
    tasks: JoinSet<(u64, Outcome)>,
    held_bytes: u64,
    failure_count: u32,
    warned: bool,
    next_retention_refresh_ms: u64,
    /// Where a gap starts, if reading had to jump past what expired.
    gap_from: Option<u64>,
    /// `archive`: a window the clock has closed, and the last id given out once it had. Nothing
    /// more can join that window, so it is fully read when reading reaches this id.
    closed_window: Option<(u64, u64)>,
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
        let interval_ms = match spec.interval_ms {
            0 => config.notify_interval_ms,
            own => own,
        };
        let budget_bytes = match spec.budget_bytes {
            0 => config.budget_bytes,
            own => own,
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
            deliveries: VecDeque::new(),
            tasks: JoinSet::new(),
            held_bytes: 0,
            failure_count: spec.failure_count,
            warned: spec.expiry_warning_id.is_some(),
            next_retention_refresh_ms: 0,
            gap_from: None,
            closed_window: None,
            spec,
            worker,
        })
    }

    fn subscribe(&mut self, connection: &Connection, from: u64) {
        self.sub += 1;
        let mut subscription = Subscription::new(self.sub, &self.worker.key.0, from);
        subscription.clauses = &self.clauses;
        subscription.author = self.author;
        connection.subscribe(subscription);
    }

    async fn run(&mut self) -> Spell {
        let Some(mut connection) = self.worker.context.handle.connection(self.home).await else {
            return Spell::End(End::Done);
        };
        self.subscribe(&connection, self.read_id);
        loop {
            let now_ms = self.worker.context.clock.now_ms();
            self.note_closed_window(now_ms).await;
            self.form_deliveries(now_ms);
            if let Some(spell) = self.start_due(now_ms).await {
                return spell;
            }
            if let Some(spell) = self.check_expiry(now_ms).await {
                return spell;
            }
            let waiting = !self.deliveries.is_empty() || !self.unread.is_empty();
            let tick = if waiting {
                self.worker.context.config.tick_ms
            } else {
                // Nothing here depends on the clock, and the stream wakes it.
                RETENTION_REFRESH_MS
            };
            tokio::select! {
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
                if let Some(first_id) = self.gap_from.take()
                    && id >= first_id
                {
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
            } if reference == self.sub => {
                if code == ErrorCode::NotFound {
                    return Some(self.finish_in_flight(Spell::Reload).await);
                }
                // Nothing a retry of the same read can do better than a fresh look.
                eprintln!(
                    "sink {} on {}: reading gave error {code:?}: {msg}",
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
            running: false,
            done: false,
        }
    }

    fn form_deliveries(&mut self, now_ms: u64) {
        let limit = self.spec.concurrency.max(1) as usize;
        while self.deliveries.len() < limit {
            let Some(first) = self.unread.front() else {
                return;
            };
            let delivery = match self.kind {
                Kind::Deliver | Kind::Forward => self.new_delivery(first, now_ms),
                Kind::Notify => {
                    if !self.deliveries.is_empty() || now_ms < self.worker.next_notify_ms {
                        return;
                    }
                    self.worker.next_notify_ms = now_ms.saturating_add(self.interval_ms);
                    self.new_delivery(first, now_ms)
                }
                Kind::Archive => match self.archive_batch(now_ms) {
                    Some(delivery) => delivery,
                    None => return,
                },
            };
            let take = match self.kind {
                Kind::Archive => delivery.bytes.len(),
                _ => 1,
            };
            self.unread.drain(..take);
            self.deliveries.push_back(delivery);
        }
    }

    /// Once the clock has passed the window of what is first in line, asks how far the stream
    /// has got. Ids never go back, so nothing given out after this can join that window.
    async fn note_closed_window(&mut self, now_ms: u64) {
        if self.kind != Kind::Archive {
            return;
        }
        let Some(first) = self.unread.front() else {
            return;
        };
        let window_ms = self.worker.context.config.archive_window_ms.max(1);
        let window = id_ms(first.first_id) / window_ms;
        if now_ms / window_ms <= window
            || self.closed_window.is_some_and(|(known, _)| known == window)
        {
            return;
        }
        let info = self
            .worker
            .client
            .lock()
            .await
            .stream_info(&self.worker.key.0)
            .await;
        if let Ok(info) = info {
            self.closed_window = Some((window, info.stamped_id));
        }
    }

    /// Everything unread in one window, up to the budget, once no more can join it and all of it
    /// has been read: a later record has arrived, or the clock has passed the window and reading
    /// has reached what was given out by then. So the batch doesn't depend on how the records
    /// happened to be framed, and a repeat has the same `{first_id, last_id}` (D190).
    fn archive_batch(&self, now_ms: u64) -> Option<Delivery> {
        let window_ms = self.worker.context.config.archive_window_ms.max(1);
        let first = self.unread.front()?;
        let window = id_ms(first.first_id) / window_ms;
        let mut delivery = self.new_delivery(first, now_ms);
        let mut closed = self
            .closed_window
            .is_some_and(|(known, stamped_id)| known == window && self.read_id >= stamped_id);
        for next in self.unread.iter().skip(1) {
            if id_ms(next.first_id) / window_ms != window {
                closed = true;
                break;
            }
            if delivery.byte_count + next.bytes.len() as u64 > self.budget_bytes {
                closed = true;
                break;
            }
            delivery.last_id = next.last_id;
            delivery.item_count += next.item_count;
            delivery.byte_count += next.bytes.len() as u64;
            delivery.bytes.push(next.bytes.clone());
        }
        (closed || delivery.byte_count >= self.budget_bytes).then_some(delivery)
    }

    async fn start_due(&mut self, now_ms: u64) -> Option<Spell> {
        let limit = self.spec.concurrency.max(1) as usize;
        let mut starts = Vec::new();
        for (at, delivery) in self.deliveries.iter().enumerate() {
            if self.tasks.len() + starts.len() >= limit {
                break;
            }
            if !delivery.running && !delivery.done && delivery.next_try_ms <= now_ms {
                starts.push(at);
            }
        }
        for at in starts {
            storage::crash_point("sink.before_delivery");
            if self.worker.hook(SinkStep::BeforeDelivery) == SinkAction::Kill {
                return Some(Spell::End(End::Killed));
            }
            let delivery = &mut self.deliveries[at];
            delivery.running = true;
            delivery.attempt += 1;
            let first_id = delivery.first_id;
            let attempt = Attempt {
                context: Arc::clone(&self.worker.context),
                client: Arc::clone(&self.worker.send_client),
                stream: Arc::clone(&self.worker.key.0),
                sink_id: self.worker.key.1,
                kind: self.kind,
                spec: self.spec.clone(),
                url: self.url.clone(),
                first_id,
                last_id: delivery.last_id,
                item_count: delivery.item_count,
                bytes: delivery.bytes.clone(),
            };
            self.tasks
                .spawn(async move { (first_id, attempt.run().await) });
        }
        None
    }

    async fn on_outcome(
        &mut self,
        joined: Result<(u64, Outcome), tokio::task::JoinError>,
    ) -> Option<Spell> {
        let Ok((first_id, outcome)) = joined else {
            return Some(Spell::End(End::Killed));
        };
        let at = self
            .deliveries
            .iter()
            .position(|delivery| delivery.first_id == first_id)?;
        self.deliveries[at].running = false;
        match outcome {
            Outcome::Delivered => {
                storage::crash_point("sink.after_delivery");
                if self.worker.hook(SinkStep::AfterDelivery) == SinkAction::Kill {
                    return Some(Spell::End(End::Killed));
                }
                self.deliveries[at].done = true;
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
                let delivery = &mut self.deliveries[at];
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

    /// Moves the cursor over every delivery at the front that is done. `None` to stop.
    async fn commit_delivered(&mut self) -> Option<()> {
        let mut through = None;
        while self.deliveries.front().is_some_and(|front| front.done) {
            let delivery = self.deliveries.pop_front()?;
            self.held_bytes = self.held_bytes.saturating_sub(delivery.byte_count);
            through = Some(delivery.last_id);
        }
        if let Some(id) = through {
            self.commit(id).await;
            storage::crash_point("sink.after_cursor");
            if self.worker.hook(SinkStep::AfterCursor) == SinkAction::Kill {
                return None;
            }
        }
        Some(())
    }

    async fn commit(&mut self, id: u64) {
        self.cursor = self.cursor.max(id);
        let (sink_id, epoch) = (self.worker.key.1, self.spec.epoch);
        let _ = self.worker.op(SinkOp::Commit { sink_id, id, epoch }).await;
    }

    /// Progress can move the cursor only with nothing read and not yet delivered before it.
    async fn commit_progress(&mut self) {
        if self.unread.is_empty() && self.deliveries.is_empty() && self.progress_id > self.cursor {
            self.commit(self.progress_id).await;
        }
    }

    async fn record_gap(&mut self, first_id: u64, last_id: u64) {
        eprintln!(
            "sink {} on {} skipped ids {first_id} to {last_id}, which expired unread",
            self.worker.key.1, self.worker.key.0
        );
        self.worker
            .report(SinkReport::Gap { first_id, last_id })
            .await;
    }

    /// What is in progress finishes, and goes into the cursor if it is first in line.
    async fn finish_in_flight(&mut self, then: Spell) -> Spell {
        while let Some(joined) = self.tasks.join_next().await {
            let Ok((first_id, outcome)) = joined else {
                continue;
            };
            let found = self
                .deliveries
                .iter_mut()
                .find(|delivery| delivery.first_id == first_id);
            if let (Some(delivery), Outcome::Delivered) = (found, outcome) {
                delivery.done = true;
            }
        }
        if self.commit_delivered().await.is_none() {
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
        let first_id = match (self.deliveries.front(), self.unread.front()) {
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
                .stream_info(&self.worker.key.0)
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

    /// Passes over what is first in line and has expired. A request in progress is let finish.
    async fn skip_expired(&mut self, now_ms: u64) -> Option<Spell> {
        let expired = |first_id: u64, retention_ms: u64| {
            id_ms(first_id).saturating_add(retention_ms) <= now_ms
        };
        let retention_ms = self.retention_ms;
        let mut gap: Option<(u64, u64)> = None;
        let extend = |gap: &mut Option<(u64, u64)>, first_id: u64, last_id: u64| {
            *gap = Some(gap.map_or((first_id, last_id), |(first, _)| (first, last_id)));
        };
        while let Some(front) = self.deliveries.front() {
            if front.running || front.done || !expired(front.first_id, retention_ms) {
                break;
            }
            let delivery = self.deliveries.pop_front()?;
            self.held_bytes = self.held_bytes.saturating_sub(delivery.byte_count);
            extend(&mut gap, delivery.first_id, delivery.last_id);
        }
        if self.deliveries.is_empty() {
            while let Some(front) = self.unread.front() {
                if !expired(front.first_id, retention_ms) {
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
            self.commit_delivered().await;
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
    stream: Arc<str>,
    sink_id: u64,
    kind: Kind,
    spec: SinkSpec,
    url: Option<WebhookUrl>,
    first_id: u64,
    last_id: u64,
    item_count: u64,
    bytes: Vec<Bytes>,
}

impl Attempt {
    async fn run(self) -> Outcome {
        match self.kind {
            Kind::Deliver => self.deliver().await,
            Kind::Notify => self.notify().await,
            Kind::Forward => self.forward().await,
            Kind::Archive => self.archive().await,
        }
    }

    async fn post(&self, content_type: &str, body: &[u8]) -> Outcome {
        let Some(url) = &self.url else {
            return Outcome::Permanent("the sink has no url".to_owned());
        };
        let mut headers = vec![
            ("X-Sink-Id", self.sink_id.to_string()),
            ("X-First-Id", self.first_id.to_string()),
            ("X-Last-Id", self.last_id.to_string()),
        ];
        if let Some(secret) = &self.spec.secret {
            headers.push(("X-Signature", webhook::signature_hex(secret, body)));
        }
        let config = &self.context.config;
        let result = webhook::post(PostParts {
            url,
            content_type,
            headers: &headers,
            body,
            allow_private: config.allow_private_addresses,
            resolver: config.resolver.as_ref(),
            tls: &self.context.tls,
            timeout_ms: config.request_timeout_ms,
        })
        .await;
        match result {
            Ok(status) => match webhook::classify_status(status) {
                StatusClass::Delivered => Outcome::Delivered,
                StatusClass::Retry => Outcome::Retry(format!("status {status}")),
                StatusClass::Permanent => Outcome::Permanent(format!("status {status}")),
            },
            Err(PostError::Blocked(reason)) => Outcome::Permanent(reason),
            Err(PostError::Failed(reason)) => Outcome::Retry(reason),
        }
    }

    async fn deliver(&self) -> Outcome {
        let Some(body) = self.bytes.first() else {
            return Outcome::Delivered;
        };
        self.post(CONTENT_RECORDS, body).await
    }

    /// `{stream, head, unread}`, with ids as strings like everywhere else in JSON (D111).
    async fn notify(&self) -> Outcome {
        let head = match self.client.lock().await.stream_info(&self.stream).await {
            Ok(info) => info.head,
            Err(code) => return Outcome::Retry(format!("reading the head: {code:?}")),
        };
        let body = format!(
            "{{\"stream\":{},\"head\":\"{head}\",\"unread\":{}}}",
            quoted(&self.stream),
            self.item_count
        );
        self.post(CONTENT_JSON, body.as_bytes()).await
    }

    /// One publish per record, since a publish's seq is the source id of its first record, and
    /// a batch that differed on a retry would be taken whole as a duplicate (D189).
    async fn forward(&self) -> Outcome {
        let Some(record) = self.bytes.first() else {
            return Outcome::Delivered;
        };
        let Some(target) = self.spec.target.as_deref() else {
            return Outcome::Permanent("the sink has no target".to_owned());
        };
        // The id is the target's to give. Everything else stays as it was, data unopened.
        let mut copy = BytesMut::from(&record[..]);
        copy[format::ID_OFFSET..format::ID_OFFSET + 8].fill(0);
        let wait = Duration::from_millis(self.context.config.request_timeout_ms);
        let mut client = self.client.lock().await;
        match client
            .publish_forward(
                target,
                &copy,
                self.sink_id,
                self.first_id,
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
                Ok(Some(Reply::Ack { producer_seq, .. })) if producer_seq == self.first_id => {
                    self.note_acked(target);
                    return Outcome::Delivered;
                }
                Ok(Some(Reply::ForwardOlder {
                    producer_seq,
                    latest_seq,
                })) if producer_seq == self.first_id => {
                    let head = match client.stream_info(&self.stream).await {
                        Ok(info) => info.head,
                        Err(code) => return Outcome::Retry(format!("reading the head: {code:?}")),
                    };
                    return if self.older_is_our_own(target, latest_seq, head) {
                        Outcome::Delivered
                    } else {
                        Outcome::Permanent(format!(
                            "{target} already has seq {latest_seq} from producer id {}, which \
                             isn't this sink's, so this record ({}) would be dropped as a \
                             duplicate; use another sink id",
                            self.sink_id, self.first_id
                        ))
                    };
                }
                Ok(Some(Reply::Nack {
                    producer_seq,
                    code,
                    msg,
                    ..
                })) if producer_seq == self.first_id => {
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

    fn note_acked(&self, target: &str) {
        let mut claims = self
            .context
            .forward_claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(claim) = claims.get_mut(&(target.to_owned(), self.sink_id)) {
            claim.acked_seq = claim.acked_seq.max(self.first_id);
        }
    }

    /// Whether a drop as older can be a repeat of what this sink sent, after a restart whose
    /// cursor was behind, or a reset. It can't be if the target's latest seq is one the source
    /// hasn't reached, or if since this node started the target has taken this sink's seqs only
    /// up to below this one. Anything else passes: it can't be told apart (D200, Q37).
    fn older_is_our_own(&self, target: &str, latest_seq: u64, head: u64) -> bool {
        if latest_seq > head {
            return false;
        }
        let claims = self
            .context
            .forward_claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        claims
            .get(&(target.to_owned(), self.sink_id))
            .is_none_or(|claim| claim.acked_seq == 0 || self.first_id <= claim.acked_seq)
    }

    async fn archive(&self) -> Outcome {
        let path = Path::from_iter([
            "archive".to_owned(),
            self.stream.to_string(),
            self.sink_id.to_string(),
            format!("{:020}-{:020}.records", self.first_id, self.last_id),
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
        let mut left = &existing[..];
        let same = self.bytes.iter().all(|part| {
            let matches = left.starts_with(part);
            if matches {
                left = &left[part.len()..];
            }
            matches
        }) && left.is_empty();
        if same {
            Outcome::Delivered
        } else {
            Outcome::Permanent(format!(
                "{path} already holds different records for this range, as a changed filter \
                 can make; use another sink id to archive these"
            ))
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
