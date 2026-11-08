//! The uploader thread: one object per seal (SPEC section 6.4, D12, D55).
//!
//! The WAL writer hands over each sealed file and each dedicated object (D268). A seal becomes
//! one segment, written to the local segment directory and uploaded from that file (D266). A failed PUT is retried with the references still held, so
//! nothing is lost and memory stays pinned while the store is down (Q9). An upload that no retry
//! can help fails on its own, so one object can't stop the ones behind it (D59).

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use format::KeyHash;
use futures::StreamExt;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender, channel};

use crate::cache::{FillReceiver, PendingWriter, SegmentCache};
use crate::object::{ObjectId, sealed_path};
use crate::segment_builder::{BuiltSegment, SegmentBuilder};
use crate::segment_format::{SegmentError, StreamExtent};
use crate::wal_writer::Sealer;

/// One stream's records in a seal, in id order. Each `Bytes` holds whole stamped records, and
/// is a reference to what the WAL wrote.
#[derive(Debug, Clone)]
pub struct SealExtent {
    pub stream: KeyHash,
    pub records: Vec<Bytes>,
}

/// What the WAL writer hands over. `tickets` holds, per core, the highest ticket the object
/// covers: for a seal, every entry that core sent up to it that no earlier object took, and
/// for a dedicated object, that stream's entries up to it.
#[derive(Debug, Clone)]
pub enum Handoff {
    Seal {
        seal: u64,
        extents: Vec<SealExtent>,
        tickets: Vec<u64>,
    },
    /// `index` is its place among the window's dedicated objects, 1 to 255 (section 7.3).
    Dedicated {
        stream: KeyHash,
        number: u64,
        index: u8,
        records: Vec<Bytes>,
        tickets: Vec<u64>,
    },
}

/// Which upload an event is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadKey {
    Seal(u64),
    /// A dedicated object: its stream, and the caller's number for it.
    Dedicated {
        stream: KeyHash,
        number: u64,
    },
}

/// What cores hear from the uploader, through an awaitable channel.
#[derive(Debug, Clone)]
pub enum UploadEvent {
    /// The object is under way, with the tickets it covers (see `Handoff`). It comes before any
    /// other event for the same key, on the same channel.
    Sealed {
        key: UploadKey,
        tickets: Arc<Vec<u64>>,
    },
    /// The object is fsynced in the local segment directory and readable there, so the cores
    /// can let their references go (section 6.2). Only with a local directory, and before
    /// `Uploaded`.
    Written {
        key: UploadKey,
        path: Path,
        extents: Arc<Vec<StreamExtent>>,
    },
    /// The object is in the store. The extents are what level 3 acks and reads need.
    Uploaded {
        key: UploadKey,
        path: Path,
        len_bytes: u64,
        extents: Arc<Vec<StreamExtent>>,
    },
    /// A segment a former process left in `pending/` is in the store now (section 7.3).
    Restored { path: Path },
    /// A seal with no records. Nothing was written.
    Empty { key: UploadKey },
    /// A PUT failed and will be retried. The references are still held.
    Retrying {
        key: UploadKey,
        attempt: u64,
        error: String,
    },
    /// This upload can't succeed, and its references are returned at exit: the
    /// records were invalid, or the store can't take this object (D59).
    Failed { key: UploadKey, error: String },
}

/// What didn't get uploaded by the time the uploader exited. The references are
/// handed back, not dropped.
#[derive(Debug)]
pub enum UnfinishedUpload {
    Seal {
        seal: u64,
        extents: Vec<SealExtent>,
    },
    Dedicated {
        stream: KeyHash,
        number: u64,
        records: Vec<Bytes>,
    },
}

impl UnfinishedUpload {
    fn of(handoff: Handoff) -> UnfinishedUpload {
        match handoff {
            Handoff::Seal { seal, extents, .. } => UnfinishedUpload::Seal { seal, extents },
            Handoff::Dedicated {
                stream,
                number,
                records,
                ..
            } => UnfinishedUpload::Dedicated {
                stream,
                number,
                records,
            },
        }
    }
}

#[derive(Debug)]
pub struct UploaderExit {
    pub unfinished: Vec<UnfinishedUpload>,
}

pub struct UploaderConfig {
    pub store: Arc<dyn ObjectStore>,
    pub generation: u64,
    pub channel_capacity: usize,
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    /// The least one PUT attempt gets, before allowing for its size. Past that it counts as a
    /// retryable failure (D242).
    pub put_timeout_min_ms: u64,
    /// Where segments are written before they upload, and stay as a read cache after (section
    /// 6.2). A node with none uploads from memory, which only tests do.
    pub cache: Option<SegmentCache>,
    /// Every core's event channel. Each event goes to all.
    pub events: Vec<UnboundedSender<UploadEvent>>,
    /// The time in this generation's last object id from before the uploader started, which
    /// its own ids come after (section 7.3). 0 if there is none.
    pub last_object_ms: u64,
    /// Objects that reads found only in the store, to copy into the cache (section 7.3).
    pub fills: Option<FillReceiver>,
}

enum UploadCommand {
    Handoff(Handoff),
    /// Gets the thread out of `recv` so it sees the stop flag.
    Wake,
}

/// Why a `try_*` call didn't queue, with the value back. Never blocks.
#[derive(Debug, PartialEq, Eq)]
pub enum UploadSendError<T> {
    Full(T),
    Stopped(T),
}

pub struct UploaderSender {
    tx: Sender<UploadCommand>,
    stop: Arc<AtomicBool>,
    /// Sends that have passed the stop check and may not have reached the channel. See
    /// `send`.
    sending_count: Arc<AtomicUsize>,
}

impl Clone for UploaderSender {
    fn clone(&self) -> Self {
        UploaderSender {
            tx: self.tx.clone(),
            stop: Arc::clone(&self.stop),
            sending_count: Arc::clone(&self.sending_count),
        }
    }
}

impl UploaderSender {
    /// A command the channel accepts must come back from `run` if it isn't uploaded,
    /// and `run` can only know it has them all if no more can arrive. So a sender
    /// announces itself and then checks the flag, and `run` sets the flag and then waits
    /// out the announced ones: with sequential consistency, either this sender sees the
    /// flag, or `run` sees this sender and drains after it. No sender ever waits, and
    /// the drain has an end however fast they send.
    fn send(&self, command: UploadCommand) -> Result<(), TrySendError<UploadCommand>> {
        self.sending_count.fetch_add(1, Ordering::SeqCst);
        let result = if self.stop.load(Ordering::SeqCst) {
            Err(TrySendError::Closed(command))
        } else {
            self.tx.try_send(command)
        };
        self.sending_count.fetch_sub(1, Ordering::SeqCst);
        result
    }

    /// Hands over a sealed file or a dedicated object. Only the WAL writer does.
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, handoff: Handoff) -> Result<(), UploadSendError<Handoff>> {
        self.send(UploadCommand::Handoff(handoff))
            .map_err(|error| match error {
                TrySendError::Full(UploadCommand::Handoff(handoff)) => {
                    UploadSendError::Full(handoff)
                }
                TrySendError::Closed(UploadCommand::Handoff(handoff)) => {
                    UploadSendError::Stopped(handoff)
                }
                _ => unreachable!("try_send returns the command it was given"),
            })
    }
}

/// How long a seal the uploader can't take yet waits before trying again.
const SEAL_RETRY: std::time::Duration = std::time::Duration::from_millis(1);

/// The node's sealer: handoffs go to the uploader, and a full file asks for a seal.
pub struct UploadSealer<F> {
    pub uploads: UploaderSender,
    pub on_full: F,
}

impl<F: FnMut() + Send> Sealer for UploadSealer<F> {
    fn full(&mut self) {
        (self.on_full)();
    }

    fn seal(&mut self, mut handoff: Handoff) {
        loop {
            match self.uploads.try_send(handoff) {
                Ok(()) | Err(UploadSendError::Stopped(_)) => return,
                Err(UploadSendError::Full(back)) => {
                    handoff = back;
                    std::thread::sleep(SEAL_RETRY);
                }
            }
        }
    }

    fn dedicated(&mut self, handoff: Handoff) -> Result<(), Handoff> {
        self.uploads.try_send(handoff).map_err(|error| match error {
            UploadSendError::Full(handoff) | UploadSendError::Stopped(handoff) => handoff,
        })
    }
}

pub struct Uploader {
    /// Kept only to send `Wake`. `finish` drops it, so the channel can close.
    wake: Option<Sender<UploadCommand>>,
    thread: Option<JoinHandle<UploaderExit>>,
    stop: Arc<AtomicBool>,
}

impl Uploader {
    pub fn start(config: UploaderConfig) -> std::io::Result<(UploaderSender, Uploader)> {
        let (tx, rx) = channel(config.channel_capacity);
        let stop = Arc::new(AtomicBool::new(false));
        let sending_count = Arc::new(AtomicUsize::new(0));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let thread_stop = Arc::clone(&stop);
        let thread_sending_count = Arc::clone(&sending_count);
        let thread = std::thread::Builder::new()
            .name("uploader".to_owned())
            .spawn(move || {
                let putter = Arc::new(Putter {
                    store: Arc::clone(&config.store),
                    stop: Arc::clone(&thread_stop),
                    events: config.events.clone(),
                    retry_initial_ms: config.retry_initial_ms,
                    retry_max_ms: config.retry_max_ms,
                    put_timeout_min_ms: config.put_timeout_min_ms,
                    local: config.cache.as_ref().map(SegmentCache::writer),
                });
                let last_object_ms = config.last_object_ms;
                let worker = Worker {
                    config,
                    putter,
                    stop: thread_stop,
                    sending_count: thread_sending_count,
                    unfinished: Vec::new(),
                    last_object_ms,
                };
                let exit = runtime.block_on(worker.run(rx));
                // Dropping the runtime would wait for a cancelled PUT's blocking thread,
                // which a stalled mount never lets go.
                runtime.shutdown_background();
                exit
            })?;
        Ok((
            UploaderSender {
                tx: tx.clone(),
                stop: Arc::clone(&stop),
                sending_count,
            },
            Uploader {
                wake: Some(tx),
                thread: Some(thread),
                stop,
            },
        ))
    }

    /// Waits until everything queued is uploaded, once every sender has been dropped.
    /// With the store down this waits until it comes back, or forever.
    pub fn finish(mut self) -> UploaderExit {
        self.wake = None;
        self.join()
    }

    /// Stops retrying, abandons a PUT in progress and exits soon, returning whatever
    /// wasn't uploaded.
    pub fn shutdown(mut self) -> UploaderExit {
        self.request_stop();
        self.join()
    }

    fn join(&mut self) -> UploaderExit {
        let thread = self.thread.take().expect("joined once, by value");
        match thread.join() {
            Ok(exit) => exit,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

impl Uploader {
    /// If the channel is full, the thread has commands to wake it anyway.
    fn request_stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(wake) = self.wake.take() {
            let _ = wake.try_send(UploadCommand::Wake);
        }
    }
}

impl Drop for Uploader {
    /// A dropped uploader must not leave a thread retrying forever. After `finish`
    /// the thread has already exited.
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// How often an upload that is waiting looks at the stop flag.
const STOP_CHECK_MS: u64 = 20;
/// What a PUT attempt is allowed on top of its minimum: 1 MiB/s, about.
const PUT_TIMEOUT_BYTES_PER_MS: u64 = 1_024;

/// PUTs running at once. Each is one object, so this bounds how far uploads can fall behind a
/// store whose single PUT is slow, and it doesn't change how many objects a seal makes (D261).
const CONCURRENT_PUTS: usize = 8;
/// Objects copied into the cache at once.
const CONCURRENT_FILLS: usize = 4;

/// A fill's object, and the file it wrote and its length.
type Fill = Pin<Box<dyn Future<Output = (Path, Result<(std::path::PathBuf, u64), String>)>>>;

/// What a PUT needs from the uploader, apart from `Worker`, which only runs between them.
struct Putter {
    store: Arc<dyn ObjectStore>,
    stop: Arc<AtomicBool>,
    events: Vec<UnboundedSender<UploadEvent>>,
    retry_initial_ms: u64,
    retry_max_ms: u64,
    put_timeout_min_ms: u64,
    local: Option<PendingWriter>,
}

enum PutEnd {
    Uploaded {
        path: Path,
        len_bytes: u64,
        extents: Vec<StreamExtent>,
    },
    /// Nothing to upload.
    Empty,
    Failed(String),
    Stopped,
}

struct Finished {
    key: UploadKey,
    ended: PutEnd,
    /// The references, which go back if the object didn't reach the store.
    keep: UnfinishedUpload,
}

type Put = Pin<Box<dyn Future<Output = Finished>>>;

/// A former process's segment: its path, its size, and how its upload ended (`None`: stopped).
type Restore = Pin<Box<dyn Future<Output = (Path, u64, Option<Result<(), String>>)>>>;

struct Worker {
    config: UploaderConfig,
    putter: Arc<Putter>,
    stop: Arc<AtomicBool>,
    sending_count: Arc<AtomicUsize>,
    unfinished: Vec<UnfinishedUpload>,
    /// The time in the last object id given out, so that the next one's is later.
    last_object_ms: u64,
}

#[derive(Debug)]
enum PutError {
    /// Worth another try.
    Retryable(object_store::Error),
    /// Another object holds the path, with different bytes.
    Collision,
    /// The store says that another try can't change the answer.
    Permanent(object_store::Error),
}

impl fmt::Display for PutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PutError::Retryable(error) => write!(f, "{error}"),
            PutError::Collision => f.write_str("another object already holds this path"),
            PutError::Permanent(error) => write!(f, "{error}"),
        }
    }
}

impl Worker {
    async fn run(mut self, mut rx: Receiver<UploadCommand>) -> UploaderExit {
        let mut in_flight: FuturesOrdered<Put> = FuturesOrdered::new();
        // What a former process left in `pending/`, uploaded beside the new seals and read
        // locally until then (section 7.3).
        let mut restoring: FuturesUnordered<Restore> = FuturesUnordered::new();
        let mut fill_rx = self.config.fills.take();
        let mut fills: FuturesUnordered<Fill> = FuturesUnordered::new();
        let mut filling: HashSet<Path> = HashSet::new();
        if let Some(cache) = &self.config.cache {
            match cache.pending_segments() {
                Ok(found) => {
                    for (path, len_bytes) in found {
                        let file = cache.pending_file(&path);
                        let putter = Arc::clone(&self.putter);
                        restoring.push(Box::pin(async move {
                            let result = putter.upload_file(&path, &file).await;
                            (path, len_bytes, result)
                        }));
                    }
                }
                Err(error) => eprintln!("listing pending/: {error}"),
            }
        }
        loop {
            let has_puts = !in_flight.is_empty();
            // With `pending/` full, seals wait here, and the channel fills behind them. What is
            // there with nothing in flight failed for good, and is the next startup's.
            let has_room = in_flight.len() < CONCURRENT_PUTS
                && (!has_puts
                    || self
                        .config
                        .cache
                        .as_ref()
                        .is_none_or(SegmentCache::has_room));
            // This has to wait on the runtime, not block the thread: connections opened here
            // run on this runtime, and the store's other users share them (D260).
            let command = tokio::select! {
                restored = restoring.next(), if !restoring.is_empty() => {
                    if let Some((path, len_bytes, result)) = restored {
                        self.restored(path, len_bytes, result);
                    }
                    continue;
                },
                finished = in_flight.next(), if has_puts => {
                    if let Some(finished) = finished {
                        self.finish(finished);
                    }
                    continue;
                },
                filled = fills.next(), if !fills.is_empty() => {
                    if let Some((path, result)) = filled {
                        filling.remove(&path);
                        if let (Ok((file, len_bytes)), Some(cache)) =
                            (result, &mut self.config.cache)
                        {
                            let _ = cache.filled(file, len_bytes);
                        }
                    }
                    continue;
                },
                path = async { fill_rx.as_mut()?.0.recv().await },
                    if fill_rx.is_some() && fills.len() < CONCURRENT_FILLS =>
                {
                    match path {
                        Some(path) => {
                            if let Some(fill) = self.start_fill(path, &mut filling) {
                                fills.push(fill);
                            }
                        }
                        None => fill_rx = None,
                    }
                    continue;
                },
                command = rx.recv(), if has_room => match command {
                    Some(command) => command,
                    None => break,
                },
            };
            if self.stop.load(Ordering::SeqCst) {
                self.keep_unfinished(command);
                break;
            }
            if let UploadCommand::Handoff(handoff) = command {
                in_flight.push_back(self.start_handoff(handoff));
            }
        }
        // These end soon if the flag is set. Otherwise they retry until they're in the store.
        while let Some(finished) = in_flight.next().await {
            self.finish(finished);
        }
        // So do the restores: `finish` means `pending/` is in the store too.
        while let Some((path, len_bytes, result)) = restoring.next().await {
            self.restored(path, len_bytes, result);
        }
        // The flag is set or every sender is gone, so nothing can arrive after these
        // sends (see `UploaderSender::send`). They take a moment, so this is a short wait.
        while self.sending_count.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
        // Whatever is left, when stopped early.
        while let Ok(command) = rx.try_recv() {
            self.keep_unfinished(command);
        }
        UploaderExit {
            unfinished: self.unfinished,
        }
    }

    /// Copies an object a read found only in the store into the cache, unless it is already
    /// there or on its way. A failure costs nothing: reads go on to the store.
    fn start_fill(&self, path: Path, filling: &mut HashSet<Path>) -> Option<Fill> {
        let cache = self.config.cache.as_ref()?;
        if filling.contains(&path) || cache.holds(&path) {
            return None;
        }
        filling.insert(path.clone());
        let (writer, room_bytes) = (cache.fill_writer(), cache.fill_room_bytes());
        let store = Arc::clone(&self.config.store);
        Some(Box::pin(async move {
            let result = writer.fill(store.as_ref(), &path, room_bytes).await;
            (path, result)
        }))
    }

    fn keep_unfinished(&mut self, command: UploadCommand) {
        if let UploadCommand::Handoff(handoff) = command {
            self.unfinished.push(UnfinishedUpload::of(handoff));
        }
    }

    fn restored(&mut self, path: Path, len_bytes: u64, result: Option<Result<(), String>>) {
        match result {
            Some(Ok(())) => {
                if let Some(cache) = &mut self.config.cache {
                    let _ = cache.uploaded(&path, len_bytes);
                }
                self.emit(UploadEvent::Restored { path });
            }
            // It stays in `pending/`, and so do the WAL files with its records.
            Some(Err(error)) => eprintln!("uploading {path} from pending/: {error}"),
            None => {}
        }
    }

    fn emit(&self, event: UploadEvent) {
        for events in &self.config.events {
            // A core that has gone away no longer needs to hear about it.
            let _ = events.send(event.clone());
        }
    }

    /// Runs in the order uploads started, however their PUTs finish, because a seal's event
    /// lets its WAL files go and that seal's records have to be in the store by then (D261).
    fn finish(&mut self, finished: Finished) {
        let Finished { key, ended, keep } = finished;
        match ended {
            PutEnd::Uploaded {
                path,
                len_bytes,
                extents,
            } => {
                if let Some(cache) = &mut self.config.cache {
                    // The object is safe in the store, so a local failure only costs a read.
                    let _ = cache.uploaded(&path, len_bytes);
                }
                self.emit(UploadEvent::Uploaded {
                    key,
                    path,
                    len_bytes,
                    extents: Arc::new(extents),
                });
            }
            PutEnd::Empty => self.emit(UploadEvent::Empty { key }),
            PutEnd::Failed(error) => {
                self.emit(UploadEvent::Failed { key, error });
                self.unfinished.push(keep);
            }
            PutEnd::Stopped => self.unfinished.push(keep),
        }
    }

    /// Tells the cores what the object covers before anything else is heard about it, so that
    /// they know which of their records each event is about.
    fn start_handoff(&mut self, handoff: Handoff) -> Put {
        let (key, tickets, built, index) = match &handoff {
            Handoff::Seal {
                seal,
                extents,
                tickets,
            } => (UploadKey::Seal(*seal), tickets, build_seal(extents), 0),
            Handoff::Dedicated {
                stream,
                number,
                index,
                records,
                tickets,
            } => {
                let mut builder = SegmentBuilder::new();
                let built = match builder.add_extent(*stream, records.iter().cloned()) {
                    Ok(()) => builder.finish(),
                    Err(error) => Err(error),
                };
                let key = UploadKey::Dedicated {
                    stream: *stream,
                    number: *number,
                };
                (key, tickets, built, *index)
            }
        };
        self.emit(UploadEvent::Sealed {
            key: key.clone(),
            tickets: Arc::new(tickets.clone()),
        });
        self.start(key, built, index, UnfinishedUpload::of(handoff))
    }

    /// The next object's id: at or after its newest record's ms, and after the last object's
    /// (section 7.3). Handoffs come one at a time, so no two objects share one.
    fn next_object_id(&mut self, last_id: u64, index: u8) -> ObjectId {
        let time_ms = crate::object::sealed_time_ms(
            format::id_ms(last_id)
                .max(self.last_object_ms + 1)
                .max(crate::OBJECT_EPOCH_MS),
        );
        self.last_object_ms = time_ms;
        ObjectId::new(time_ms, self.config.generation, index)
    }

    fn start(
        &mut self,
        key: UploadKey,
        built: Result<BuiltSegment, SegmentError>,
        index: u8,
        keep: UnfinishedUpload,
    ) -> Put {
        let putter = Arc::clone(&self.putter);
        let path = match &built {
            Ok(built) => built.first_id.map(|first_id| {
                sealed_path(self.next_object_id(built.last_id.unwrap_or(first_id), index))
            }),
            Err(_) => None,
        };
        if let (Some(cache), Ok(built)) = (&mut self.config.cache, &built)
            && built.first_id.is_some()
        {
            // A local failure here costs only room, and the write reports its own.
            let _ = cache.reserve(built.len_bytes);
        }
        Box::pin(async move {
            let ended = match (built, path) {
                (Ok(built), Some(path)) => putter.upload(&key, path, built).await,
                (Ok(_), None) => PutEnd::Empty,
                (Err(error), _) => PutEnd::Failed(error.to_string()),
            };
            Finished { key, ended, keep }
        })
    }
}

impl Putter {
    /// Retries until the PUT succeeds or the uploader is stopped. No two objects get one id, so
    /// a path held by different bytes is something else's doing, and fails the upload.
    async fn upload(&self, key: &UploadKey, path: Path, built: BuiltSegment) -> PutEnd {
        let BuiltSegment {
            parts,
            len_bytes,
            extents,
            ..
        } = built;
        // The references have done their job once it is a file, and the PUT reads that.
        let parts = match &self.local {
            Some(local) => match self.write_pending(key, local, &path, parts).await {
                Some(file) => {
                    let written = UploadEvent::Written {
                        key: key.clone(),
                        path: path.clone(),
                        extents: Arc::new(extents.clone()),
                    };
                    for events in &self.events {
                        let _ = events.send(written.clone());
                    }
                    vec![file]
                }
                None => return PutEnd::Stopped,
            },
            None => parts,
        };
        let mut backoff_ms = self.retry_initial_ms;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let Some(put) = self.put_unless_stopped(&path, &parts).await else {
                return PutEnd::Stopped;
            };
            match put {
                Ok(()) => {
                    return PutEnd::Uploaded {
                        path,
                        len_bytes,
                        extents,
                    };
                }
                Err(error @ (PutError::Collision | PutError::Permanent(_))) => {
                    return PutEnd::Failed(format!("{path}: {error}"));
                }
                Err(error @ PutError::Retryable(_)) => {
                    for events in &self.events {
                        let _ = events.send(UploadEvent::Retrying {
                            key: key.clone(),
                            attempt,
                            error: error.to_string(),
                        });
                    }
                }
            }
            if !self.wait_unless_stopped(backoff_ms).await {
                return PutEnd::Stopped;
            }
            backoff_ms = backoff_ms.saturating_mul(2).min(self.retry_max_ms);
        }
    }

    /// Writes the segment on a blocking thread, so the PUTs on this one go on. A disk that can't
    /// take it now is retried like a store that can't, with the references still held. `None`
    /// if the uploader was stopped first.
    async fn write_pending(
        &self,
        key: &UploadKey,
        local: &PendingWriter,
        path: &Path,
        parts: Vec<Bytes>,
    ) -> Option<Bytes> {
        let mut backoff_ms = self.retry_initial_ms;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let write = tokio::task::spawn_blocking({
                let (local, path, parts) = (local.clone(), path.clone(), parts.clone());
                move || {
                    crate::crash::crash_point("seal.before_pending");
                    local.write(&path, &parts)
                }
            });
            let error = tokio::select! {
                biased;
                written = write => match written {
                    Ok(Ok(file)) => {
                        crate::crash::crash_point("seal.after_pending");
                        return Some(file);
                    }
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => error.to_string(),
                },
                _ = self.wait_for_stop() => return None,
            };
            for events in &self.events {
                let _ = events.send(UploadEvent::Retrying {
                    key: key.clone(),
                    attempt,
                    error: format!("writing {path} locally: {error}"),
                });
            }
            if !self.wait_unless_stopped(backoff_ms).await {
                return None;
            }
            backoff_ms = backoff_ms.saturating_mul(2).min(self.retry_max_ms);
        }
    }

    /// One PUT, abandoned if the uploader is stopped: nothing bounds how long a store
    /// takes. An abandoned PUT may still land, so the references that come back may
    /// already be in the store, which a later upload of them counts as done (D55).
    async fn put_unless_stopped(
        &self,
        path: &Path,
        parts: &[Bytes],
    ) -> Option<Result<(), PutError>> {
        let total_bytes: usize = parts.iter().map(Bytes::len).sum();
        let timeout_ms = self
            .put_timeout_min_ms
            .saturating_add(total_bytes as u64 / PUT_TIMEOUT_BYTES_PER_MS);
        let put = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            put_once(self.store.as_ref(), path, parts),
        );
        tokio::select! {
            biased;
            put = put => Some(put.unwrap_or_else(|_| {
                // If the abandoned PUT lands after all, the retry finds the same bytes there.
                Err(PutError::Retryable(object_store::Error::Generic {
                    store: "uploader",
                    source: format!("a PUT of {total_bytes} bytes took over {timeout_ms} ms").into(),
                }))
            })),
            _ = self.wait_for_stop() => None,
        }
    }

    /// Uploads a file from `pending/`, retrying until it's in the store or stopped.
    async fn upload_file(&self, path: &Path, file: &std::path::Path) -> Option<Result<(), String>> {
        let bytes = match std::fs::read(file) {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) => return Some(Err(format!("{}: {error}", file.display()))),
        };
        let mut backoff_ms = self.retry_initial_ms;
        loop {
            match self
                .put_unless_stopped(path, std::slice::from_ref(&bytes))
                .await?
            {
                Ok(()) => return Some(Ok(())),
                Err(error @ (PutError::Collision | PutError::Permanent(_))) => {
                    return Some(Err(error.to_string()));
                }
                Err(PutError::Retryable(_)) => {}
            }
            if !self.wait_unless_stopped(backoff_ms).await {
                return None;
            }
            backoff_ms = backoff_ms.saturating_mul(2).min(self.retry_max_ms);
        }
    }

    async fn wait_for_stop(&self) {
        while !self.stop.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(STOP_CHECK_MS)).await;
        }
    }

    /// Sleeps in short steps, so `shutdown` is prompt during a long backoff.
    async fn wait_unless_stopped(&self, wait_ms: u64) -> bool {
        let mut left_ms = wait_ms;
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            if left_ms == 0 {
                return true;
            }
            let step_ms = left_ms.min(STOP_CHECK_MS);
            tokio::time::sleep(Duration::from_millis(step_ms)).await;
            left_ms -= step_ms;
        }
    }
}

/// Groups the extents by stream, keeping their order, so a stream with more than one extent
/// keeps them in id order.
fn build_seal(extents: &[SealExtent]) -> Result<BuiltSegment, SegmentError> {
    let mut sorted: Vec<&SealExtent> = extents.iter().collect();
    sorted.sort_by_key(|a| a.stream);
    let mut builder = SegmentBuilder::new();
    for extent in sorted {
        builder.add_extent(extent.stream, extent.records.iter().cloned())?;
    }
    builder.finish()
}

/// One conditional create, so that a path collision can't overwrite a seal. If the
/// path exists with the same bytes, an earlier attempt succeeded after reporting a
/// failure, which counts as done (D55).
async fn put_once(store: &dyn ObjectStore, path: &Path, parts: &[Bytes]) -> Result<(), PutError> {
    crate::crash::crash_point("seal.before_put");
    put_create(store, path, parts).await
}

/// One object under its own path, which nobody else can overwrite. The same bytes there count
/// as done. A store error is the caller's to retry.
pub async fn put_object(
    store: &dyn ObjectStore,
    path: &Path,
    parts: &[Bytes],
) -> Result<(), String> {
    put_create(store, path, parts)
        .await
        .map_err(|error| format!("{path}: {error}"))
}

/// A compacted object of `hour` under `root` (section 7.3). The same bytes already under one of
/// this generation's ids there are an earlier try that got that far, and count as done.
/// Otherwise it takes the index after this generation's highest.
pub async fn put_compacted(
    store: &dyn ObjectStore,
    root: &str,
    hour: u64,
    generation: u64,
    parts: &[Bytes],
) -> Result<Path, String> {
    let len_bytes: usize = parts.iter().map(Bytes::len).sum();
    let mut next = 0u16;
    let listed = crate::list_hour_objects(store, root, hour)
        .await
        .map_err(|error| format!("{root}/{hour}: {error}"))?;
    for object in listed {
        let Some(id) = ObjectId::of_path(&object.location) else {
            continue;
        };
        if id.generation() != generation {
            continue;
        }
        next = next.max(u16::from(id.index()) + 1);
        if object.size == len_bytes {
            let existing = async { store.get(&object.location).await?.bytes().await };
            let existing = existing
                .await
                .map_err(|error| format!("{}: {error}", object.location))?;
            if same_bytes(&existing, parts) {
                return Ok(object.location);
            }
        }
    }
    loop {
        let Ok(index) = u8::try_from(next) else {
            return Err(format!(
                "{root}/{hour} holds generation {generation}'s every index already"
            ));
        };
        let path = crate::object_path(root, ObjectId::compacted(hour, generation, index));
        match put_create(store, &path, parts).await {
            Ok(()) => return Ok(path),
            Err(PutError::Collision) => next += 1,
            Err(error) => return Err(format!("{path}: {error}")),
        }
    }
}

async fn put_create(store: &dyn ObjectStore, path: &Path, parts: &[Bytes]) -> Result<(), PutError> {
    let payload = PutPayload::from_iter(parts.iter().cloned());
    let options = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store.put_opts(path, payload, options).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = store
                .get(path)
                .await
                .map_err(PutError::Retryable)?
                .bytes()
                .await
                .map_err(PutError::Retryable)?;
            if same_bytes(&existing, parts) {
                Ok(())
            } else {
                Err(PutError::Collision)
            }
        }
        Err(
            error @ (object_store::Error::InvalidPath { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented),
        ) => Err(PutError::Permanent(error)),
        Err(error) => Err(PutError::Retryable(error)),
    }
}

fn same_bytes(existing: &[u8], parts: &[Bytes]) -> bool {
    let mut rest = existing;
    for part in parts {
        match rest.strip_prefix(&part[..]) {
            Some(after) => rest = after,
            None => return false,
        }
    }
    rest.is_empty()
}
