//! The WAL writer thread (SPEC section 6.3, D8, D9).
//!
//! The loop is the spec's: wait for one command, take whatever else has queued,
//! write it all, `sync_data`, and only then tell the cores. There is no timer: what
//! queues during a sync goes in the next one.
//!
//! The writer keeps a reference to every entry in its active file, and a seal hands those to
//! the uploader, so a seal holds exactly what its file holds (D268).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use tokio::sync::{oneshot, watch};

use crate::crash::{crash_point, wal_write_gate};
use crate::object::stream_fits_dedicated_path;
use crate::uploader::{Handoff, SealExtent};
use crate::wal_format::{WAL_FILE_HEADER_BYTES, WalEntry, ZERO_PADDING, encode_file_header};
use crate::wal_replay::wal_file_name;

/// An open WAL file. The real one is a `File`, and tests inject others to record calls.
pub trait WalFile: Send {
    /// One `writev`, which may write only part of what it is given.
    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize>;
    fn sync_data(&mut self) -> io::Result<()>;
}

/// Where WAL files come from. `create` returns a file positioned for its first entry, with a
/// durable header and a durable name.
pub trait WalVolume: Send {
    type File: WalFile;
    fn create(&mut self, seq: u64) -> io::Result<Self::File>;
    /// Removes a closed file nothing needs. Files are never reused: record data is the
    /// publisher's, so old bytes could hold an entry a CRC can't tell apart (D265).
    fn remove(&mut self, seq: u64) -> io::Result<()>;
}

/// Tells cores what is durable. Called only after the `sync_data` covering `ticket`.
pub trait DurableNotify: Send {
    fn durable(&mut self, core: usize, ticket: u64);
}

/// The wake-up cores await: one `watch` per core holding the highest durable ticket.
/// `changed()` returning an error means the writer has stopped and nothing more will
/// become durable (section 6.3: cores can't block on a crossbeam `recv`).
pub struct DurableWatch {
    senders: Vec<watch::Sender<u64>>,
}

/// One notifier and one receiver per core. Tickets start above 0.
pub fn durable_watches(core_count: usize) -> (DurableWatch, Vec<watch::Receiver<u64>>) {
    let (senders, receivers) = (0..core_count).map(|_| watch::channel(0)).unzip();
    (DurableWatch { senders }, receivers)
}

impl DurableNotify for DurableWatch {
    fn durable(&mut self, core: usize, ticket: u64) {
        self.senders[core].send_replace(ticket);
    }
}

/// Where sealed files and dedicated objects go, and who hears that the active file is full.
pub trait Sealer: Send {
    /// The active file has reached the size cap. Called once per file.
    fn full(&mut self);
    /// Returns once the seal is taken, or can't ever be. Waiting here for an uploader that is
    /// behind is the backpressure section 7.3 asks for. One that has stopped drops it: the
    /// cores keep their references, and the WAL keeps the file.
    fn seal(&mut self, handoff: Handoff);
    /// Gives a dedicated object back if it can't be taken now, and it stays in the seal.
    #[allow(clippy::result_large_err)]
    fn dedicated(&mut self, handoff: Handoff) -> Result<(), Handoff>;
}

/// Seals nothing, for a writer whose files nothing uploads.
impl Sealer for () {
    fn full(&mut self) {}

    fn seal(&mut self, _handoff: Handoff) {}

    fn dedicated(&mut self, _handoff: Handoff) -> Result<(), Handoff> {
        Ok(())
    }
}

impl WalFile for File {
    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        Write::write_vectored(self, slices)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }
}

/// WAL files in a directory, preallocated with `set_len` (D51).
#[derive(Debug, Clone)]
pub struct DirVolume {
    dir: PathBuf,
    preallocate_bytes: u64,
    /// Goes in each file's header. 0 for a node with no ownership record.
    generation: u64,
}

impl DirVolume {
    pub fn new(dir: impl Into<PathBuf>, preallocate_bytes: u64) -> DirVolume {
        DirVolume {
            dir: dir.into(),
            preallocate_bytes,
            generation: 0,
        }
    }

    /// The volume of a process that owns `generation`.
    pub fn for_generation(
        dir: impl Into<PathBuf>,
        preallocate_bytes: u64,
        generation: u64,
    ) -> DirVolume {
        DirVolume {
            generation,
            ..DirVolume::new(dir, preallocate_bytes)
        }
    }

    fn path(&self, seq: u64) -> PathBuf {
        self.dir.join(wal_file_name(seq))
    }

    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.dir)?.sync_all()
    }

    fn write_header(&self, file: &mut File, seq: u64) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&encode_file_header(seq, self.generation))?;
        file.sync_all()?;
        Ok(())
    }
}

impl WalVolume for DirVolume {
    type File = File;

    fn create(&mut self, seq: u64) -> io::Result<File> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.path(seq))?;
        let ready = file
            .set_len(self.preallocate_bytes.max(WAL_FILE_HEADER_BYTES as u64))
            .and_then(|()| self.write_header(&mut file, seq))
            .and_then(|()| self.sync_dir());
        if let Err(error) = ready {
            // A retry of this seq (D233) needs the name free, and a half-made file holds nothing.
            let _ = std::fs::remove_file(self.path(seq));
            return Err(error);
        }
        Ok(file)
    }

    fn remove(&mut self, seq: u64) -> io::Result<()> {
        std::fs::remove_file(self.path(seq))
    }
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    /// The active file's `seq`. After a restart, one above every file in `wal_dir`.
    pub first_seq: u64,
    /// Files left from before a restart, which `try_remove` removes in their turn.
    pub closed_seqs: Vec<u64>,
    /// Bound of the channel, in commands. A full channel is backpressure.
    pub channel_capacity: usize,
    /// The active file asks for a seal once it holds this many record bytes (section 7.3).
    pub segment_cap_bytes: u64,
    /// A stream with this many record bytes in the active file gets a dedicated object.
    pub stream_buffer_bytes: u64,
    /// What a former process left in closed files and no object holds. It goes into the first
    /// seal, each extent as it is, since the caller splits a stream wherever another object's
    /// extent lies between its records.
    pub carried: Vec<SealExtent>,
}

/// What a finished writer did. `largest_group_count` and the totals are what the
/// group commit tests and the `wal_throughput` bench read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStats {
    pub entry_count: u64,
    pub record_bytes: u64,
    pub sync_count: u64,
    pub largest_group_count: u64,
}

/// Why the writer stopped. After this it writes and notifies nothing more, and a
/// failed sync is never retried: the kernel may have dropped the dirty pages, so a
/// later sync succeeding would prove nothing.
#[derive(Debug)]
pub enum WalError {
    Write(io::Error),
    Sync(io::Error),
    Rotate(io::Error),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Write(error) => write!(f, "WAL write failed: {error}"),
            WalError::Sync(error) => write!(f, "WAL sync failed: {error}"),
            WalError::Rotate(error) => write!(f, "WAL rotation failed: {error}"),
        }
    }
}

impl std::error::Error for WalError {}

/// The files around a seal. Every entry queued before the seal command is in `closed_seq` or
/// earlier. A seal of an empty file doesn't rotate, and then `new_seq` is the file still active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRotated {
    pub closed_seq: u64,
    pub new_seq: u64,
}

enum WalCommand<R> {
    Append(WalEntry<R>),
    Seal {
        seal: u64,
        done: oneshot::Sender<WalRotated>,
    },
    Remove {
        up_to_seq: u64,
    },
}

/// Why a `try_*` call didn't queue. It always returns at once, and gives the value back.
#[derive(Debug, PartialEq, Eq)]
pub enum WalSendError<T> {
    /// Backpressure on publishers (section 6.3).
    Full(T),
    /// The writer has stopped.
    Stopped(T),
}

/// A core's end of the channel. Every method is `try_send`, so no core ever blocks.
pub struct WalSender<R> {
    tx: Sender<WalCommand<R>>,
    /// The writer has no active file. Appends wait as they do for a full channel (D233).
    no_file: Arc<AtomicBool>,
}

impl<R> Clone for WalSender<R> {
    fn clone(&self) -> Self {
        WalSender {
            tx: self.tx.clone(),
            no_file: Arc::clone(&self.no_file),
        }
    }
}

impl<R> WalSender<R> {
    pub fn try_append(&self, entry: WalEntry<R>) -> Result<(), WalSendError<WalEntry<R>>> {
        if self.no_file.load(Ordering::Acquire) {
            return Err(WalSendError::Full(entry));
        }
        self.tx
            .try_send(WalCommand::Append(entry))
            .map_err(|error| match error {
                TrySendError::Full(WalCommand::Append(entry)) => WalSendError::Full(entry),
                TrySendError::Disconnected(WalCommand::Append(entry)) => {
                    WalSendError::Stopped(entry)
                }
                _ => unreachable!("try_send returns the command it was given"),
            })
    }

    /// Whether the writer has an active file. Without one, nothing more is written until a seal
    /// frees room (D233).
    pub fn has_file(&self) -> bool {
        !self.no_file.load(Ordering::Acquire)
    }

    /// Seals the active file as `seal`: it is rotated and handed to the uploader (D8, D268).
    /// The receiver resolves once the old file is durable and the new one is ready.
    pub fn try_seal(&self, seal: u64) -> Result<oneshot::Receiver<WalRotated>, WalSendError<()>> {
        let (done, done_rx) = oneshot::channel();
        match self.tx.try_send(WalCommand::Seal { seal, done }) {
            Ok(()) => Ok(done_rx),
            Err(TrySendError::Full(_)) => Err(WalSendError::Full(())),
            Err(TrySendError::Disconnected(_)) => Err(WalSendError::Stopped(())),
        }
    }

    /// Closed files up to `up_to_seq` are covered by uploaded segments and checkpoints, so they
    /// can go (D16). The active file is never affected.
    pub fn try_remove(&self, up_to_seq: u64) -> Result<(), WalSendError<u64>> {
        match self.tx.try_send(WalCommand::Remove { up_to_seq }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(WalSendError::Full(up_to_seq)),
            Err(TrySendError::Disconnected(_)) => Err(WalSendError::Stopped(up_to_seq)),
        }
    }
}

/// The running writer. The channel closes, and the writer finishes, once every
/// `WalSender` is dropped.
pub struct WalHandle {
    thread: JoinHandle<Result<WalStats, WalError>>,
}

impl WalHandle {
    pub fn join(self) -> Result<WalStats, WalError> {
        match self.thread.join() {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

/// Opens the first file and starts the writer thread.
pub fn start_wal_writer<R, V, N, S>(
    mut volume: V,
    notify: N,
    sealer: S,
    config: WalConfig,
) -> io::Result<(WalSender<R>, WalHandle)>
where
    R: AsRef<[u8]> + Send + 'static,
    V: WalVolume + 'static,
    N: DurableNotify + 'static,
    S: Sealer + 'static,
{
    // Closed files are what a seal uploads and then frees, so a full volume can drain (D233).
    let file = match volume.create(config.first_seq) {
        Ok(file) => Some(file),
        Err(error)
            if error.kind() == io::ErrorKind::StorageFull && !config.closed_seqs.is_empty() =>
        {
            eprintln!(
                "the WAL volume has no room for a new file: {error}. Writes wait until a seal has \
                 uploaded what the WAL holds and freed one of its files."
            );
            None
        }
        Err(error) => return Err(error),
    };
    let no_file = Arc::new(AtomicBool::new(file.is_none()));
    let (tx, rx) = bounded(config.channel_capacity);
    let writer = Writer {
        volume,
        notify,
        file,
        no_file: Arc::clone(&no_file),
        active_seq: config.first_seq,
        closed_seqs: config.closed_seqs.into(),
        channel_capacity: config.channel_capacity,
        stats: WalStats::default(),
        sealer,
        segment_cap_bytes: config.segment_cap_bytes,
        stream_buffer_bytes: config.stream_buffer_bytes,
        open: OpenFile {
            extents: config.carried,
            ..OpenFile::default()
        },
        last_tickets: Vec::new(),
        dedicated_count: 0,
    };
    let thread = std::thread::Builder::new()
        .name("wal-writer".to_owned())
        .spawn(move || {
            let result = writer.run(rx);
            // The node goes on serving reads, so nothing else tells an operator (D79).
            if let Err(error) = &result {
                eprintln!(
                    "the WAL writer stopped: {error}. This node refuses writes until it is \
                     restarted."
                );
            }
            result
        })?;
    Ok((WalSender { tx, no_file }, WalHandle { thread }))
}

struct Writer<V: WalVolume, N, S> {
    volume: V,
    notify: N,
    /// `None` while the file `active_seq` names can't be opened yet (D233, D234).
    file: Option<V::File>,
    no_file: Arc<AtomicBool>,
    active_seq: u64,
    closed_seqs: VecDeque<u64>,
    channel_capacity: usize,
    stats: WalStats,
    sealer: S,
    segment_cap_bytes: u64,
    stream_buffer_bytes: u64,
    open: OpenFile,
    /// Per core, the highest ticket written.
    last_tickets: Vec<u64>,
    dedicated_count: u64,
}

/// What the active file holds that no object has taken yet.
#[derive(Default)]
struct OpenFile {
    /// In the order each stream first appeared. A stream's records are in id order.
    extents: Vec<SealExtent>,
    streams: HashMap<Arc<str>, OpenStream>,
    entry_count: u64,
    record_bytes: u64,
    full_told: bool,
}

struct OpenStream {
    /// Where its records are in `extents`.
    at: usize,
    record_bytes: u64,
    /// Per core, the highest ticket among its entries.
    tickets: Vec<u64>,
}

fn raise(tickets: &mut Vec<u64>, core: usize, ticket: u64) {
    if tickets.len() <= core {
        tickets.resize(core + 1, 0);
    }
    tickets[core] = tickets[core].max(ticket);
}

impl<V: WalVolume, N: DurableNotify, S: Sealer> Writer<V, N, S> {
    fn run<R: AsRef<[u8]> + Send + 'static>(
        mut self,
        rx: Receiver<WalCommand<R>>,
    ) -> Result<WalStats, WalError> {
        let mut batch = Vec::new();
        let mut heads = Vec::new();
        while let Ok(first) = rx.recv() {
            let mut control = match first {
                WalCommand::Append(entry) => {
                    batch.push(entry);
                    None
                }
                other => Some(other),
            };
            if control.is_none() {
                // Bounded so that fast senders can't keep one group open forever.
                for command in rx.try_iter().take(self.channel_capacity) {
                    match command {
                        WalCommand::Append(entry) => batch.push(entry),
                        other => {
                            control = Some(other);
                            break;
                        }
                    }
                }
            }
            if !batch.is_empty() {
                self.commit(&batch, &mut heads)?;
                for entry in batch.drain(..) {
                    self.keep(entry);
                }
                if self.open.record_bytes >= self.segment_cap_bytes && !self.open.full_told {
                    self.open.full_told = true;
                    self.sealer.full();
                }
            }
            match control {
                Some(WalCommand::Seal { seal, done }) => {
                    let rotated = self.seal(seal).map_err(WalError::Rotate)?;
                    // A core that stopped waiting doesn't matter to the writer.
                    let _ = done.send(rotated);
                }
                Some(WalCommand::Remove { up_to_seq }) => {
                    while let Some(seq) = self
                        .closed_seqs
                        .front()
                        .copied()
                        .filter(|seq| *seq <= up_to_seq)
                    {
                        self.closed_seqs.pop_front();
                        // A file that can't be removed is harmless: it replays as entries already
                        // covered, and the next startup has it back in its closed list.
                        if let Err(error) = self.volume.remove(seq) {
                            eprintln!("removing WAL file {seq}: {error}");
                        }
                    }
                    if self.file.is_none() {
                        self.open_active().map_err(WalError::Rotate)?;
                    }
                }
                Some(WalCommand::Append(_)) | None => {}
            }
        }
        Ok(self.stats)
    }

    /// Keeps a synced entry's records for the seal, and hands over its stream's as a dedicated
    /// object once they fill its buffer.
    fn keep<R: AsRef<[u8]> + Send + 'static>(&mut self, entry: WalEntry<R>) {
        let (core, ticket) = (entry.core(), entry.ticket());
        let stream = Arc::clone(entry.stream_arc());
        let records = Bytes::from_owner(entry.into_records());
        raise(&mut self.last_tickets, core, ticket);
        let open = &mut self.open;
        open.entry_count += 1;
        open.record_bytes += records.len() as u64;
        let at = open.extents.len();
        let kept = open
            .streams
            .entry(Arc::clone(&stream))
            .or_insert_with(|| OpenStream {
                at,
                record_bytes: 0,
                tickets: Vec::new(),
            });
        if kept.at == at {
            open.extents.push(SealExtent {
                stream: Arc::clone(&stream),
                records: Vec::new(),
            });
        }
        kept.record_bytes += records.len() as u64;
        raise(&mut kept.tickets, core, ticket);
        open.extents[kept.at].records.push(records);
        if kept.record_bytes >= self.stream_buffer_bytes && stream_fits_dedicated_path(&stream) {
            self.hand_off_dedicated(stream);
        }
    }

    /// One that can't be taken now stays in the seal (D59, D268).
    fn hand_off_dedicated(&mut self, stream: Arc<str>) {
        let Some(kept) = self.open.streams.remove(&stream) else {
            return;
        };
        let records = std::mem::take(&mut self.open.extents[kept.at].records);
        let handoff = Handoff::Dedicated {
            stream: Arc::clone(&stream),
            number: self.dedicated_count,
            records,
            tickets: kept.tickets.clone(),
        };
        match self.sealer.dedicated(handoff) {
            Ok(()) => self.dedicated_count += 1,
            Err(handoff) => {
                if let Handoff::Dedicated { records, .. } = handoff {
                    self.open.extents[kept.at].records = records;
                }
                self.open.streams.insert(stream, kept);
            }
        }
    }

    /// Rotates unless the file is empty, and hands what it holds to the uploader, even when
    /// that is nothing, since the seal's checkpoint follows its upload event.
    fn seal(&mut self, seal: u64) -> io::Result<WalRotated> {
        let rotated = if self.file.is_some() && self.open.entry_count == 0 {
            WalRotated {
                closed_seq: self.active_seq - 1,
                new_seq: self.active_seq,
            }
        } else {
            self.rotate()?
        };
        let open = std::mem::take(&mut self.open);
        self.sealer.seal(Handoff::Seal {
            seal,
            extents: open
                .extents
                .into_iter()
                .filter(|extent| !extent.records.is_empty())
                .collect(),
            tickets: self.last_tickets.clone(),
        });
        Ok(rotated)
    }

    /// Tries again for the file `active_seq` names. Still none to be had isn't an error.
    fn open_active(&mut self) -> io::Result<()> {
        match self.volume.create(self.active_seq) {
            Ok(file) => {
                self.file = Some(file);
                self.no_file.store(false, Ordering::Release);
                Ok(())
            }
            Err(error) if can_pass(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Writes the group, syncs, and only then notifies. An error returns before
    /// any notification, and ends the thread.
    fn commit<R: AsRef<[u8]>>(
        &mut self,
        batch: &[WalEntry<R>],
        heads: &mut Vec<u8>,
    ) -> Result<(), WalError> {
        // Only an append that passed the flag just as a rotation found no room can get here. It
        // has no file to go in, and it isn't acked, so this ends as any full disk does.
        let Some(file) = &mut self.file else {
            return Err(WalError::Write(io::ErrorKind::StorageFull.into()));
        };
        heads.clear();
        let mut layout = Vec::with_capacity(batch.len());
        for entry in batch {
            let start = heads.len();
            let padding = entry.write_head(self.active_seq, heads);
            layout.push((start, heads.len(), padding));
        }
        let mut slices = Vec::with_capacity(batch.len() * 3);
        for (entry, (start, end, padding)) in batch.iter().zip(&layout) {
            slices.push(IoSlice::new(&heads[*start..*end]));
            slices.push(IoSlice::new(entry.records()));
            if *padding > 0 {
                slices.push(IoSlice::new(&ZERO_PADDING[..*padding]));
            }
        }
        wal_write_gate();
        crash_point("wal.before_write");
        write_all_vectored(file, &mut slices).map_err(WalError::Write)?;
        crash_point("wal.before_fsync");
        file.sync_data().map_err(WalError::Sync)?;
        crash_point("wal.after_fsync");

        self.stats.sync_count += 1;
        self.stats.entry_count += batch.len() as u64;
        self.stats.largest_group_count = self.stats.largest_group_count.max(batch.len() as u64);
        for entry in batch {
            self.stats.record_bytes += entry.records().len() as u64;
        }
        // A core's entries are in its send order, so the last one seen is its highest.
        let mut notified = Vec::new();
        for entry in batch.iter().rev() {
            if !notified.contains(&entry.core()) {
                notified.push(entry.core());
                self.notify.durable(entry.core(), entry.ticket());
            }
        }
        Ok(())
    }

    /// Everything before this command has been synced by `commit`, so the old file
    /// is complete when the new one takes over.
    fn rotate(&mut self) -> io::Result<WalRotated> {
        if self.file.is_none() {
            // Nothing is active, so what is closed is everything below it.
            self.open_active()?;
            return Ok(WalRotated {
                closed_seq: self.active_seq - 1,
                new_seq: self.active_seq,
            });
        }
        let closed_seq = self.active_seq;
        let new_seq = closed_seq + 1;
        self.file = match self.volume.create(new_seq) {
            Ok(file) => Some(file),
            Err(error) if can_pass(&error) => {
                eprintln!(
                    "the WAL can't open a new file: {error}. Writes wait for a later seal to \
                     open it."
                );
                self.no_file.store(true, Ordering::Release);
                None
            }
            Err(error) => return Err(error),
        };
        self.closed_seqs.push_back(closed_seq);
        self.active_seq = new_seq;
        Ok(WalRotated {
            closed_seq,
            new_seq,
        })
    }
}

/// An open that failed for want of room or descriptors, which pass: a seal frees a file, and a
/// connection's end frees a descriptor. Neither says anything is wrong with the disk (D233, D234).
fn can_pass(error: &io::Error) -> bool {
    const ENFILE: i32 = 23;
    const EMFILE: i32 = 24;
    error.kind() == io::ErrorKind::StorageFull
        || matches!(error.raw_os_error(), Some(ENFILE | EMFILE))
}

fn write_all_vectored<F: WalFile>(file: &mut F, mut slices: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !slices.is_empty() {
        match file.write_vectored(slices) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => IoSlice::advance_slices(&mut slices, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
