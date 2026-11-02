//! The uploader thread: one object per seal (SPEC section 6.4, D12, D55).
//!
//! Each core hands over one sorted section per seal. When every core's section for
//! a seal has arrived, they become one segment, written to the local segment directory and
//! uploaded from that file (D266). A failed PUT is retried with the references still held, so
//! nothing is lost and memory stays pinned while the store is down (Q9). An upload that no retry
//! can help fails on its own, so one object can't stop the ones behind it (D59).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender, channel};

use crate::cache::{PendingWriter, SegmentCache};
use crate::object::{path_fits_stores, sealed_path};
use crate::segment_builder::{BuiltSegment, SegmentBuilder};
use crate::segment_format::{SegmentError, StreamExtent};

/// One stream's records in one section, in id order. `records` are references to
/// whole stamped records, as with the WAL.
#[derive(Debug, Clone)]
pub struct SectionExtent<R> {
    pub stream: Arc<str>,
    pub records: Vec<R>,
}

/// One core's references for one seal, sorted by stream.
#[derive(Debug, Clone)]
pub struct Section<R> {
    pub extents: Vec<SectionExtent<R>>,
}

/// Which upload an event is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadKey {
    Seal(u64),
    /// A dedicated object: its stream, and the caller's number for it.
    Dedicated {
        stream: Arc<str>,
        number: u64,
    },
}

/// What cores hear from the uploader, through an awaitable channel.
#[derive(Debug, Clone)]
pub enum UploadEvent {
    /// The object is in the store. The extents are what level 3 acks and reads need.
    Uploaded {
        key: UploadKey,
        path: Path,
        len_bytes: u64,
        extents: Arc<Vec<StreamExtent>>,
    },
    /// A seal in which no core had any records. Nothing was written.
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
pub enum UnfinishedUpload<R> {
    Seal {
        seal: u64,
        sections: BTreeMap<usize, Section<R>>,
    },
    Dedicated {
        stream: Arc<str>,
        number: u64,
        records: Vec<R>,
    },
}

#[derive(Debug)]
pub struct UploaderExit<R> {
    pub unfinished: Vec<UnfinishedUpload<R>>,
}

pub struct UploaderConfig {
    pub store: Arc<dyn ObjectStore>,
    /// Part of the object path (section 7.3).
    pub node: String,
    pub generation: u64,
    /// How many cores send a section per seal. Each sends one, even if it is empty.
    pub section_count: usize,
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
}

enum UploadCommand<R> {
    Section {
        seal: u64,
        core: usize,
        section: Section<R>,
    },
    Dedicated {
        stream: Arc<str>,
        number: u64,
        records: Vec<R>,
    },
    /// Gets the thread out of `recv` so it sees the stop flag.
    Wake,
}

/// Why a `try_*` call didn't queue, with the value back. Never blocks.
#[derive(Debug, PartialEq, Eq)]
pub enum UploadSendError<T> {
    Full(T),
    Stopped(T),
}

pub struct UploaderSender<R> {
    tx: Sender<UploadCommand<R>>,
    stop: Arc<AtomicBool>,
    /// Sends that have passed the stop check and may not have reached the channel. See
    /// `send`.
    sending_count: Arc<AtomicUsize>,
}

impl<R> Clone for UploaderSender<R> {
    fn clone(&self) -> Self {
        UploaderSender {
            tx: self.tx.clone(),
            stop: Arc::clone(&self.stop),
            sending_count: Arc::clone(&self.sending_count),
        }
    }
}

fn unwrap_send<R, T>(
    error: TrySendError<UploadCommand<R>>,
    value: impl FnOnce(UploadCommand<R>) -> T,
) -> UploadSendError<T> {
    match error {
        TrySendError::Full(command) => UploadSendError::Full(value(command)),
        TrySendError::Closed(command) => UploadSendError::Stopped(value(command)),
    }
}

impl<R> UploaderSender<R> {
    /// A command the channel accepts must come back from `run` if it isn't uploaded,
    /// and `run` can only know it has them all if no more can arrive. So a sender
    /// announces itself and then checks the flag, and `run` sets the flag and then waits
    /// out the announced ones: with sequential consistency, either this sender sees the
    /// flag, or `run` sees this sender and drains after it. No sender ever waits, and
    /// the drain has an end however fast they send.
    fn send(&self, command: UploadCommand<R>) -> Result<(), TrySendError<UploadCommand<R>>> {
        self.sending_count.fetch_add(1, Ordering::SeqCst);
        let result = if self.stop.load(Ordering::SeqCst) {
            Err(TrySendError::Closed(command))
        } else {
            self.tx.try_send(command)
        };
        self.sending_count.fetch_sub(1, Ordering::SeqCst);
        result
    }

    /// Hands over a core's section for a seal.
    #[allow(clippy::result_large_err)]
    pub fn try_send_section(
        &self,
        seal: u64,
        core: usize,
        section: Section<R>,
    ) -> Result<(), UploadSendError<Section<R>>> {
        self.send(UploadCommand::Section {
            seal,
            core,
            section,
        })
        .map_err(|error| {
            unwrap_send(error, |command| match command {
                UploadCommand::Section { section, .. } => section,
                _ => unreachable!("try_send returns the command it was given"),
            })
        })
    }

    /// Uploads a stream that filled its own buffer as its own object (section 7.3).
    /// Deciding when is the core's job.
    #[allow(clippy::result_large_err)]
    pub fn try_send_dedicated(
        &self,
        stream: Arc<str>,
        number: u64,
        records: Vec<R>,
    ) -> Result<(), UploadSendError<Vec<R>>> {
        self.send(UploadCommand::Dedicated {
            stream,
            number,
            records,
        })
        .map_err(|error| {
            unwrap_send(error, |command| match command {
                UploadCommand::Dedicated { records, .. } => records,
                _ => unreachable!("try_send returns the command it was given"),
            })
        })
    }
}

pub struct Uploader<R> {
    /// Kept only to send `Wake`. `finish` drops it, so the channel can close.
    wake: Option<Sender<UploadCommand<R>>>,
    thread: Option<JoinHandle<UploaderExit<R>>>,
    stop: Arc<AtomicBool>,
}

impl<R> Uploader<R>
where
    R: AsRef<[u8]> + Clone + Send + 'static,
{
    pub fn start(config: UploaderConfig) -> std::io::Result<(UploaderSender<R>, Uploader<R>)> {
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
                let section_count = config.section_count;
                let putter = Arc::new(Putter {
                    store: Arc::clone(&config.store),
                    stop: Arc::clone(&thread_stop),
                    events: config.events.clone(),
                    retry_initial_ms: config.retry_initial_ms,
                    retry_max_ms: config.retry_max_ms,
                    put_timeout_min_ms: config.put_timeout_min_ms,
                    local: config.cache.as_ref().map(SegmentCache::writer),
                });
                let worker = Worker {
                    config,
                    putter,
                    stop: thread_stop,
                    sending_count: thread_sending_count,
                    pending: BTreeMap::new(),
                    last_seal_by_core: vec![None; section_count],
                    unfinished: Vec::new(),
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
    pub fn finish(mut self) -> UploaderExit<R> {
        self.wake = None;
        self.join()
    }

    /// Stops retrying, abandons a PUT in progress and exits soon, returning whatever
    /// wasn't uploaded.
    pub fn shutdown(mut self) -> UploaderExit<R> {
        self.request_stop();
        self.join()
    }

    fn join(&mut self) -> UploaderExit<R> {
        let thread = self.thread.take().expect("joined once, by value");
        match thread.join() {
            Ok(exit) => exit,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

impl<R> Uploader<R> {
    /// If the channel is full, the thread has commands to wake it anyway.
    fn request_stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(wake) = self.wake.take() {
            let _ = wake.try_send(UploadCommand::Wake);
        }
    }
}

impl<R> Drop for Uploader<R> {
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
        /// The name it has in `pending/`, which a collision can leave behind `path`.
        written: Path,
        path: Path,
        len_bytes: u64,
        extents: Vec<StreamExtent>,
    },
    /// Nothing to upload.
    Empty,
    Failed(String),
    Stopped,
}

struct Finished<R> {
    key: UploadKey,
    ended: PutEnd,
    /// The references, which go back if the object didn't reach the store.
    keep: UnfinishedUpload<R>,
}

type Put<R> = Pin<Box<dyn Future<Output = Finished<R>>>>;

struct Worker<R> {
    config: UploaderConfig,
    putter: Arc<Putter>,
    stop: Arc<AtomicBool>,
    sending_count: Arc<AtomicUsize>,
    pending: BTreeMap<u64, BTreeMap<usize, Section<R>>>,
    /// The highest seal each core has handed over. A core's seals only go up, so this
    /// recognises a repeat even after its seal has uploaded and left `pending`.
    last_seal_by_core: Vec<Option<u64>>,
    unfinished: Vec<UnfinishedUpload<R>>,
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

impl<R: AsRef<[u8]> + Clone + Send + 'static> Worker<R> {
    async fn run(mut self, mut rx: Receiver<UploadCommand<R>>) -> UploaderExit<R> {
        let mut in_flight: FuturesOrdered<Put<R>> = FuturesOrdered::new();
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
                finished = in_flight.next(), if has_puts => {
                    if let Some(finished) = finished {
                        self.finish(finished);
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
            match command {
                UploadCommand::Section {
                    seal,
                    core,
                    section,
                } => {
                    if let Some(put) = self.receive_section(seal, core, section) {
                        in_flight.push_back(put);
                    }
                }
                UploadCommand::Dedicated {
                    stream,
                    number,
                    records,
                } => in_flight.push_back(self.start_dedicated(stream, number, records)),
                UploadCommand::Wake => {}
            }
        }
        // These end soon if the flag is set. Otherwise they retry until they're in the store.
        while let Some(finished) = in_flight.next().await {
            self.finish(finished);
        }
        // The flag is set or every sender is gone, so nothing can arrive after these
        // sends (see `UploaderSender::send`). They take a moment, so this is a short wait.
        while self.sending_count.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
        // Whatever is left: stopped early, or a seal that never got all its sections.
        while let Ok(command) = rx.try_recv() {
            self.keep_unfinished(command);
        }
        for (seal, sections) in std::mem::take(&mut self.pending) {
            self.unfinished
                .push(UnfinishedUpload::Seal { seal, sections });
        }
        UploaderExit {
            unfinished: self.unfinished,
        }
    }

    /// The seal's upload, once this was its last section.
    fn receive_section(&mut self, seal: u64, core: usize, section: Section<R>) -> Option<Put<R>> {
        // Counting sections is what fires the upload, so a wrong one must not count,
        // and replacing one would drop its references.
        let in_order = self
            .last_seal_by_core
            .get_mut(core)
            .filter(|last| last.is_none_or(|last| seal > last));
        let Some(last) = in_order else {
            self.unfinished.push(UnfinishedUpload::Seal {
                seal,
                sections: BTreeMap::from([(core, section)]),
            });
            self.emit(UploadEvent::Failed {
                key: UploadKey::Seal(seal),
                error: format!("unexpected section from core {core}"),
            });
            return None;
        };
        *last = Some(seal);
        let sections = self.pending.entry(seal).or_default();
        sections.insert(core, section);
        if sections.len() != self.config.section_count {
            return None;
        }
        let sections = self.pending.remove(&seal)?;
        Some(self.start_seal(seal, sections))
    }

    fn keep_unfinished(&mut self, command: UploadCommand<R>) {
        match command {
            UploadCommand::Section {
                seal,
                core,
                section,
            } => match self.pending.entry(seal).or_default().entry(core) {
                Entry::Vacant(slot) => {
                    slot.insert(section);
                }
                Entry::Occupied(_) => self.unfinished.push(UnfinishedUpload::Seal {
                    seal,
                    sections: BTreeMap::from([(core, section)]),
                }),
            },
            UploadCommand::Dedicated {
                stream,
                number,
                records,
            } => self.unfinished.push(UnfinishedUpload::Dedicated {
                stream,
                number,
                records,
            }),
            UploadCommand::Wake => {}
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
    fn finish(&mut self, finished: Finished<R>) {
        let Finished { key, ended, keep } = finished;
        match ended {
            PutEnd::Uploaded {
                written,
                path,
                len_bytes,
                extents,
            } => {
                if let Some(cache) = &mut self.config.cache {
                    // The object is safe in the store, so a local failure only costs a read.
                    let _ = cache.uploaded(&written, &path, len_bytes);
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

    fn start_seal(&mut self, seal: u64, sections: BTreeMap<usize, Section<R>>) -> Put<R> {
        let key = UploadKey::Seal(seal);
        let built = build_seal(&sections);
        let (node, generation) = (self.config.node.clone(), self.config.generation);
        self.start(
            key,
            built,
            Box::new(move |first_id, last_id| sealed_path(&node, generation, first_id, last_id)),
            UnfinishedUpload::Seal { seal, sections },
        )
    }

    fn start_dedicated(&mut self, stream: Arc<str>, number: u64, records: Vec<R>) -> Put<R> {
        let key = UploadKey::Dedicated {
            stream: Arc::clone(&stream),
            number,
        };
        let parts = records.iter().cloned().map(Bytes::from_owner);
        let mut builder = SegmentBuilder::new();
        let built = match builder.add_extent(&stream, parts) {
            Ok(()) => builder.finish(),
            Err(error) => Err(error),
        };
        let name = Arc::clone(&stream);
        let generation = self.config.generation;
        self.start(
            key,
            built,
            Box::new(move |first_id, last_id| sealed_path(&name, generation, first_id, last_id)),
            UnfinishedUpload::Dedicated {
                stream,
                number,
                records,
            },
        )
    }

    fn start(
        &mut self,
        key: UploadKey,
        built: Result<BuiltSegment, SegmentError>,
        path_for: Box<dyn Fn(u64, u64) -> Path>,
        keep: UnfinishedUpload<R>,
    ) -> Put<R> {
        let putter = Arc::clone(&self.putter);
        if let (Some(cache), Ok(built)) = (&mut self.config.cache, &built)
            && built.first_id.is_some()
        {
            // A local failure here costs only room, and the write reports its own.
            let _ = cache.reserve(built.len_bytes);
        }
        Box::pin(async move {
            let ended = match built {
                Ok(built) => match built.first_id {
                    Some(first_id) => {
                        let last_id = built.last_id.unwrap_or(first_id);
                        let path = |first_id| path_for(first_id, last_id);
                        putter.upload(&key, &path, first_id, built).await
                    }
                    None => PutEnd::Empty,
                },
                Err(error) => PutEnd::Failed(error.to_string()),
            };
            Finished { key, ended, keep }
        })
    }
}

impl Putter {
    /// Retries until the PUT succeeds or the uploader is stopped. A path held by different bytes
    /// moves to the next `first_id`: the name only has to be unique, and readers find objects by
    /// extent (D156).
    async fn upload(
        &self,
        key: &UploadKey,
        path_for: &dyn Fn(u64) -> Path,
        mut first_id: u64,
        built: BuiltSegment,
    ) -> PutEnd {
        let mut path = path_for(first_id);
        if !path_fits_stores(&path) {
            return PutEnd::Failed(format!("{path} has a part too long for an object path"));
        }
        let written = path.clone();
        let BuiltSegment {
            parts,
            len_bytes,
            extents,
            ..
        } = built;
        // The references have done their job once it is a file, and the PUT reads that.
        let parts = match &self.local {
            Some(local) => match self.write_pending(key, local, &written, parts).await {
                Some(file) => vec![file],
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
                        written,
                        path,
                        len_bytes,
                        extents,
                    };
                }
                Err(PutError::Collision) => {
                    first_id += 1;
                    path = path_for(first_id);
                    continue;
                }
                Err(error @ PutError::Permanent(_)) => return PutEnd::Failed(error.to_string()),
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

/// Lays the sections out in core order, combining their footer indexes. A stream that
/// moved cores appears in two sections, so it gets two entries.
fn build_seal<R>(sections: &BTreeMap<usize, Section<R>>) -> Result<BuiltSegment, SegmentError>
where
    R: AsRef<[u8]> + Clone + Send + 'static,
{
    let mut builder = SegmentBuilder::new();
    for section in sections.values() {
        let mut previous: Option<&str> = None;
        for extent in &section.extents {
            if previous.is_some_and(|stream| stream >= &*extent.stream) {
                return Err(SegmentError::OutOfOrder("section streams"));
            }
            previous = Some(&extent.stream);
            let parts = extent.records.iter().cloned().map(Bytes::from_owner);
            builder.add_extent(&extent.stream, parts)?;
        }
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

/// An object nobody else can overwrite, under the first free name from `first_id` on (D156). The
/// same bytes at a path count as done. Compaction's outputs go through this. A store error is
/// the caller's to retry.
pub async fn put_new_object(
    store: &dyn ObjectStore,
    path_for: impl Fn(u64) -> Path,
    mut first_id: u64,
    parts: &[Bytes],
) -> Result<Path, String> {
    loop {
        let path = path_for(first_id);
        match put_create(store, &path, parts).await {
            Ok(()) => return Ok(path),
            Err(PutError::Collision) => first_id += 1,
            Err(error) => return Err(error.to_string()),
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
