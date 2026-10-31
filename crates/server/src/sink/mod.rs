//! Sinks (SPEC section 9): a pool of threads apart from the cores, where each sink is one task
//! reading its stream as any subscriber does and pushing what matches to a destination.
//!
//! A sink's spec and cursor belong to its stream, on the stream's owner core, so they move with
//! the shard and are saved with the stream. The pool holds no record of them: it hears that a
//! sink changed, and the sink's task asks the owner what it now is. Records are never opened,
//! and a task holds its own copy of what it has yet to deliver, so a stalled destination pins no
//! slab and nothing on the write path waits for it.

mod webhook;
mod worker;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use object_store::ObjectStore;
use rustls::ClientConfig;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};

use crate::clock::Clock;
use crate::node::NodeHandle;

pub use webhook::{
    SinkResolver, StatusClass, WebhookUrl, classify_status, is_private_address, parse_url,
    signature_hex,
};

/// Node settings for sinks (D13). Each is a plain starting value.
#[derive(Clone)]
pub struct SinkConfig {
    /// Threads in the pool. 0 runs no sinks: they are only recorded on their streams.
    pub pool_thread_count: usize,
    /// Lets webhooks reach private addresses (D13).
    pub allow_private_addresses: bool,
    /// The first retry's wait, doubling up to `retry_max_ms`.
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    /// Failures in a row that pause a sink.
    pub max_failure_count: u32,
    /// A whole webhook attempt, or a forward's wait for its ack.
    pub request_timeout_ms: u64,
    /// `webhook.notify`'s least time between sends, unless the sink sets its own.
    pub notify_interval_ms: u64,
    /// What one sink may hold, unless it sets its own. One frame can pass it.
    pub budget_bytes: u64,
    /// How long before unread data expires the sink warns.
    pub expire_warn_ms: u64,
    /// `archive`: records of one such window go in one object, up to the byte budget.
    pub archive_window_ms: u64,
    /// How often a task looks at the clock while it has something waiting on it.
    pub tick_ms: u64,
    /// How often a paused sink checks whether its unread data is about to expire.
    pub paused_check_ms: u64,
    /// `archive`'s store. The node's own store if `None`.
    pub archive_store: Option<Arc<dyn ObjectStore>>,
    /// More certificates for webhook TLS to trust, as PEM.
    pub extra_roots_pem: Option<Vec<u8>>,
    pub resolver: Option<Arc<dyn SinkResolver>>,
    pub hook: Option<Arc<dyn SinkHook>>,
}

impl Default for SinkConfig {
    fn default() -> SinkConfig {
        SinkConfig {
            pool_thread_count: 2,
            allow_private_addresses: false,
            retry_initial_ms: 1_000,
            retry_max_ms: 60_000,
            max_failure_count: 20,
            request_timeout_ms: 30_000,
            notify_interval_ms: 1_000,
            budget_bytes: 8 * 1024 * 1024,
            expire_warn_ms: 3_600_000,
            archive_window_ms: 10_000,
            tick_ms: 50,
            paused_check_ms: 60_000,
            archive_store: None,
            extra_roots_pem: None,
            resolver: None,
            hook: None,
        }
    }
}

/// Where in a delivery a test can stop a sink's task, as a crash would (section 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkStep {
    BeforeDelivery,
    /// `forward` only: the publish has gone and its ack hasn't been waited for.
    AfterPublish,
    AfterDelivery,
    AfterCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkAction {
    Continue,
    /// Ends the task with nothing further done. A new one starts from the saved cursor.
    Kill,
}

pub trait SinkHook: Send + Sync {
    fn at(&self, sink_id: u64, step: SinkStep) -> SinkAction;

    /// What the sink's task holds, each time that grows.
    fn held(&self, _sink_id: u64, _held_bytes: u64) {}
}

/// A core saying that a sink changed.
#[derive(Debug, Clone)]
pub(crate) struct SinkNote {
    pub stream: Arc<str>,
    pub sink_id: u64,
}

/// `retry_initial_ms × 2^(attempt - 1)`, up to `retry_max_ms`. `attempt` counts from 1.
pub fn backoff_ms(initial_ms: u64, max_ms: u64, attempt: u32) -> u64 {
    let doubled = 1u64
        .checked_shl(attempt.saturating_sub(1))
        .map_or(u64::MAX, |factor| initial_ms.saturating_mul(factor));
    doubled.min(max_ms)
}

/// What every sink's task shares. None of it is near a core.
pub(crate) struct PoolContext {
    pub config: SinkConfig,
    pub clock: Arc<dyn Clock>,
    pub handle: NodeHandle,
    pub archive: Arc<dyn ObjectStore>,
    pub tls: Arc<ClientConfig>,
    /// `(target, sink id)` → source of each running forward sink. A sink id is unique per stream
    /// only, and two sources with one id would share a producer id at the target, where one's
    /// records would be dropped as the other's duplicates (D193).
    pub forward_claims: Mutex<HashMap<(String, u64), String>>,
}

type SinkKey = (Arc<str>, u64);

const RESTART_DELAY_MS: u64 = 5;

/// A task's end, which the registry hears of.
pub(crate) struct WorkerExit {
    pub key: SinkKey,
    /// The last change it had looked at.
    pub seen_note_count: u64,
    /// It ended as a crash would, so a new one takes over.
    pub restart: bool,
}

struct Running {
    notes: watch::Sender<u64>,
    note_count: u64,
}

/// One entry per sink with a task, on one task of its own, so there's nothing to lock.
struct Registry {
    context: Arc<PoolContext>,
    handles: Vec<Handle>,
    running: HashMap<SinkKey, Running>,
    exits_tx: mpsc::UnboundedSender<WorkerExit>,
    next_thread: usize,
}

impl Registry {
    fn spawn(&mut self, key: SinkKey, note_count: u64, start_delay_ms: u64) {
        let (notes, notes_rx) = watch::channel(note_count);
        self.running
            .insert(key.clone(), Running { notes, note_count });
        let thread = self.next_thread % self.handles.len();
        self.next_thread = self.next_thread.wrapping_add(1);
        let (context, exits) = (Arc::clone(&self.context), self.exits_tx.clone());
        self.handles[thread].spawn(worker::run(context, key, notes_rx, exits, start_delay_ms));
    }

    fn on_note(&mut self, key: SinkKey) {
        match self.running.get_mut(&key) {
            Some(running) => {
                running.note_count += 1;
                running.notes.send_replace(running.note_count);
            }
            None => self.spawn(key, 0, 0),
        }
    }

    fn on_exit(&mut self, exit: WorkerExit) {
        let Some(running) = self.running.remove(&exit.key) else {
            return;
        };
        // A change it never looked at may have brought the sink back.
        if exit.restart || running.note_count > exit.seen_note_count {
            let delay_ms = if exit.restart { RESTART_DELAY_MS } else { 0 };
            self.spawn(exit.key, running.note_count, delay_ms);
        }
    }

    async fn run(
        mut self,
        mut notes: mpsc::UnboundedReceiver<SinkNote>,
        mut exits: mpsc::UnboundedReceiver<WorkerExit>,
    ) {
        loop {
            tokio::select! {
                note = notes.recv() => match note {
                    Some(note) => self.on_note((note.stream, note.sink_id)),
                    None => return,
                },
                Some(exit) = exits.recv() => self.on_exit(exit),
            }
        }
    }
}

/// The running pool. Dropping it stops the threads, and what they were delivering is left as a
/// crash would leave it.
pub(crate) struct SinkPool {
    stops: Vec<oneshot::Sender<()>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl SinkPool {
    /// Starts the threads, and a sink for each one found on `streams`, which a startup read
    /// from its checkpoints and WAL.
    pub(crate) fn start(
        config: SinkConfig,
        clock: Arc<dyn Clock>,
        handle: NodeHandle,
        store: Arc<dyn ObjectStore>,
        notes_tx: mpsc::UnboundedSender<SinkNote>,
        notes: mpsc::UnboundedReceiver<SinkNote>,
        streams: Vec<String>,
    ) -> std::io::Result<SinkPool> {
        let mut stops = Vec::new();
        let mut threads = Vec::new();
        let mut handles = Vec::new();
        for number in 0..config.pool_thread_count {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            handles.push(runtime.handle().clone());
            let (stop, stopped) = oneshot::channel::<()>();
            stops.push(stop);
            threads.push(
                std::thread::Builder::new()
                    .name(format!("sink-{number}"))
                    .spawn(move || {
                        let _ = runtime.block_on(stopped);
                        // A request in progress mustn't hold up the stop.
                        runtime.shutdown_background();
                    })?,
            );
        }
        let Some(first) = handles.first().cloned() else {
            return Ok(SinkPool { stops, threads });
        };
        let context = Arc::new(PoolContext {
            archive: config.archive_store.clone().unwrap_or(store),
            tls: webhook::tls_client_config(config.extra_roots_pem.as_deref())?,
            config,
            clock,
            handle: handle.clone(),
            forward_claims: Mutex::new(HashMap::new()),
        });
        let (exits_tx, exits) = mpsc::unbounded_channel();
        let registry = Registry {
            context,
            handles,
            running: HashMap::new(),
            exits_tx,
            next_thread: 0,
        };
        first.spawn(registry.run(notes, exits));
        first.spawn(async move {
            let mut client = handle.client(0);
            for stream in streams {
                // Asking loads the stream, and its load is retried until it works (D140).
                if let Ok(info) = client.stream_info(&stream).await {
                    let stream: Arc<str> = Arc::from(stream);
                    for (sink_id, _, _) in info.sinks {
                        let stream = Arc::clone(&stream);
                        let _ = notes_tx.send(SinkNote { stream, sink_id });
                    }
                }
            }
        });
        Ok(SinkPool { stops, threads })
    }

    pub(crate) fn stop(&mut self) {
        self.stops.clear();
    }

    pub(crate) fn join(&mut self) {
        self.stop();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
