//! The uploader thread: one object per seal (SPEC section 6.4, D12, D55).
//!
//! Each core hands over one sorted section per seal. When every core's section for
//! a seal has arrived, they become one segment and one PUT, without sorting or copying
//! data. A failed PUT is retried with the references still held, so nothing is lost
//! and memory stays pinned while the store is down (Q9). An upload that no retry can
//! help fails on its own, so one object can't stop the ones behind it (D59).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded, never, select};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use tokio::sync::mpsc::UnboundedSender;

use crate::cache::SegmentCache;
use crate::object::{dedicated_path, path_fits_stores, seal_path};
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
    /// records were invalid, another object already holds its path (D55), or the
    /// store can't take this object (D59).
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
    /// Where uploaded objects are copied (section 6.2, step 5).
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
    /// Separate from `tx`, so reads can't use up the room sections need (D54).
    touch_tx: Sender<Path>,
    stop: Arc<AtomicBool>,
    /// Sends that have passed the stop check and may not have reached the channel. See
    /// `send`.
    sending_count: Arc<AtomicUsize>,
}

impl<R> Clone for UploaderSender<R> {
    fn clone(&self) -> Self {
        UploaderSender {
            tx: self.tx.clone(),
            touch_tx: self.touch_tx.clone(),
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
        TrySendError::Disconnected(command) => UploadSendError::Stopped(value(command)),
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
            Err(TrySendError::Disconnected(command))
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

    /// Tells the local cache an object was read. Losing this on a full queue costs
    /// only a slightly worse eviction choice, and it has its own queue, so a burst of
    /// reads can't make a section or dedicated object come back `Full`.
    pub fn try_cache_touch(&self, path: Path) -> bool {
        !self.stop.load(Ordering::SeqCst) && self.touch_tx.try_send(path).is_ok()
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
        let (tx, rx) = bounded(config.channel_capacity);
        let (touch_tx, touch_rx) = bounded(config.channel_capacity);
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
                let worker = Worker {
                    config,
                    stop: thread_stop,
                    sending_count: thread_sending_count,
                    pending: BTreeMap::new(),
                    unfinished: Vec::new(),
                };
                runtime.block_on(worker.run(rx, touch_rx))
            })?;
        Ok((
            UploaderSender {
                tx: tx.clone(),
                touch_tx,
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

    /// Stops retrying and exits soon, returning whatever wasn't uploaded.
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

struct Worker<R> {
    config: UploaderConfig,
    stop: Arc<AtomicBool>,
    sending_count: Arc<AtomicUsize>,
    pending: BTreeMap<u64, BTreeMap<usize, Section<R>>>,
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
    async fn run(
        mut self,
        rx: Receiver<UploadCommand<R>>,
        mut touch_rx: Receiver<Path>,
    ) -> UploaderExit<R> {
        loop {
            // `select!` blocks this thread, which runs nothing else: the runtime is here
            // only for `object_store` calls (D12).
            let command = select! {
                recv(rx) -> command => match command {
                    Ok(command) => command,
                    Err(_) => break,
                },
                recv(touch_rx) -> path => {
                    match path {
                        Ok(path) => self.touch_cache(&path),
                        // Every sender is gone, and `rx` will say so next.
                        Err(_) => touch_rx = never(),
                    }
                    if self.stop.load(Ordering::SeqCst) {
                        break;
                    }
                    continue;
                },
            };
            // Reads that came before this upload count before its eviction. The bound
            // keeps a fast reader from holding the upload up.
            for path in touch_rx
                .try_iter()
                .take(self.config.channel_capacity)
                .collect::<Vec<_>>()
            {
                self.touch_cache(&path);
            }
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
                    self.receive_section(seal, core, section).await;
                }
                UploadCommand::Dedicated {
                    stream,
                    number,
                    records,
                } => self.upload_dedicated(stream, number, records).await,
                UploadCommand::Wake => {}
            }
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

    fn touch_cache(&mut self, path: &Path) {
        if let Some(cache) = &mut self.config.cache {
            // A cache problem never affects the upload path.
            let _ = cache.touch(path);
        }
    }

    async fn receive_section(&mut self, seal: u64, core: usize, section: Section<R>) {
        let sections = self.pending.entry(seal).or_default();
        // Counting sections is what fires the upload, so a wrong one must not count,
        // and replacing one would drop its references.
        if core >= self.config.section_count || sections.contains_key(&core) {
            self.unfinished.push(UnfinishedUpload::Seal {
                seal,
                sections: BTreeMap::from([(core, section)]),
            });
            self.emit(UploadEvent::Failed {
                key: UploadKey::Seal(seal),
                error: format!("unexpected section from core {core}"),
            });
            return;
        }
        sections.insert(core, section);
        if sections.len() == self.config.section_count
            && let Some(sections) = self.pending.remove(&seal)
        {
            self.upload_seal(seal, sections).await;
        }
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

    async fn upload_seal(&mut self, seal: u64, sections: BTreeMap<usize, Section<R>>) {
        let key = UploadKey::Seal(seal);
        let built = build_seal(&sections);
        let uploaded = match built {
            Ok(built) => {
                let Some(first_id) = built.first_id else {
                    self.emit(UploadEvent::Empty { key });
                    return;
                };
                let path = seal_path(&self.config.node, self.config.generation, first_id);
                self.upload(key, path, built).await
            }
            Err(error) => {
                self.emit(UploadEvent::Failed {
                    key,
                    error: error.to_string(),
                });
                false
            }
        };
        if !uploaded {
            self.unfinished
                .push(UnfinishedUpload::Seal { seal, sections });
        }
    }

    async fn upload_dedicated(&mut self, stream: Arc<str>, number: u64, records: Vec<R>) {
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
        let uploaded = match built {
            Ok(built) => {
                let Some(first_id) = built.first_id else {
                    self.emit(UploadEvent::Empty { key });
                    return;
                };
                let path = dedicated_path(&stream, self.config.generation, first_id);
                self.upload(key, path, built).await
            }
            Err(error) => {
                self.emit(UploadEvent::Failed {
                    key,
                    error: error.to_string(),
                });
                false
            }
        };
        if !uploaded {
            self.unfinished.push(UnfinishedUpload::Dedicated {
                stream,
                number,
                records,
            });
        }
    }

    /// Retries until the PUT succeeds or the uploader is stopped. Returns whether the
    /// object is in the store.
    async fn upload(&mut self, key: UploadKey, path: Path, built: BuiltSegment) -> bool {
        if !path_fits_stores(&path) {
            self.emit(UploadEvent::Failed {
                key,
                error: format!("{path} has a part too long for an object path"),
            });
            return false;
        }
        let mut backoff_ms = self.config.retry_initial_ms;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match put_once(self.config.store.as_ref(), &path, &built.parts).await {
                Ok(()) => break,
                Err(error @ (PutError::Collision | PutError::Permanent(_))) => {
                    self.emit(UploadEvent::Failed {
                        key,
                        error: error.to_string(),
                    });
                    return false;
                }
                Err(error @ PutError::Retryable(_)) => {
                    self.emit(UploadEvent::Retrying {
                        key: key.clone(),
                        attempt,
                        error: error.to_string(),
                    });
                }
            }
            if !self.wait_unless_stopped(backoff_ms).await {
                return false;
            }
            backoff_ms = backoff_ms.saturating_mul(2).min(self.config.retry_max_ms);
        }
        if let Some(cache) = &mut self.config.cache {
            // The object is safe in the store, so a local failure only costs a read.
            let _ = cache.put(&path, &built.parts);
        }
        self.emit(UploadEvent::Uploaded {
            key,
            path,
            len_bytes: built.len_bytes,
            extents: Arc::new(built.extents),
        });
        true
    }

    /// Sleeps in short steps, so `shutdown` is prompt during a long backoff.
    async fn wait_unless_stopped(&self, wait_ms: u64) -> bool {
        const STEP_MS: u64 = 20;
        let mut left_ms = wait_ms;
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            if left_ms == 0 {
                return true;
            }
            let step_ms = left_ms.min(STEP_MS);
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
