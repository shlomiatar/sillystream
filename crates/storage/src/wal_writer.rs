//! The WAL writer thread (SPEC section 6.3, D8, D9).
//!
//! The loop is the spec's: wait for one command, take whatever else has queued,
//! write it all, `sync_data`, and only then tell the cores. There is no timer: what
//! queues during a sync goes in the next one.

use std::collections::VecDeque;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use tokio::sync::{oneshot, watch};

use crate::wal_format::{WAL_FILE_HEADER_BYTES, WalEntry, ZERO_PADDING, encode_file_header};
use crate::wal_replay::wal_file_name;

/// An open WAL file. The real one is a `File`, and tests inject others to record calls.
pub trait WalFile: Send {
    /// One `writev`, which may write only part of what it is given.
    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize>;
    fn sync_data(&mut self) -> io::Result<()>;
}

/// Where WAL files come from. Both calls return a file positioned for its first
/// entry, with a durable header and a durable name.
pub trait WalVolume: Send {
    type File: WalFile;
    fn create(&mut self, seq: u64) -> io::Result<Self::File>;
    /// Renames a closed file to `new_seq` and rewrites its header, so that its old
    /// entries fail their CRC (D8).
    fn reuse(&mut self, old_seq: u64, new_seq: u64) -> io::Result<Self::File>;
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
}

impl DirVolume {
    pub fn new(dir: impl Into<PathBuf>, preallocate_bytes: u64) -> DirVolume {
        DirVolume {
            dir: dir.into(),
            preallocate_bytes,
        }
    }

    fn path(&self, seq: u64) -> PathBuf {
        self.dir.join(wal_file_name(seq))
    }

    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.dir)?.sync_all()
    }

    fn write_header(file: &mut File, seq: u64) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&encode_file_header(seq))?;
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
        file.set_len(self.preallocate_bytes.max(WAL_FILE_HEADER_BYTES as u64))?;
        DirVolume::write_header(&mut file, seq)?;
        self.sync_dir()?;
        Ok(file)
    }

    fn reuse(&mut self, old_seq: u64, new_seq: u64) -> io::Result<File> {
        let path = self.path(new_seq);
        std::fs::rename(self.path(old_seq), &path)?;
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        DirVolume::write_header(&mut file, new_seq)?;
        self.sync_dir()?;
        Ok(file)
    }
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    /// The active file's `seq`. After a restart, one above every file in `wal_dir`.
    pub first_seq: u64,
    /// Files left from before a restart, which become reusable through `try_recycle`.
    pub closed_seqs: Vec<u64>,
    /// Bound of the channel, in commands. A full channel is backpressure.
    pub channel_capacity: usize,
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

/// The files around a rotation. Every entry queued before the rotation command
/// is in `closed_seq` or earlier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRotated {
    pub closed_seq: u64,
    pub new_seq: u64,
}

enum WalCommand<R> {
    Append(WalEntry<R>),
    Rotate(oneshot::Sender<WalRotated>),
    Recycle { up_to_seq: u64 },
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
}

impl<R> Clone for WalSender<R> {
    fn clone(&self) -> Self {
        WalSender {
            tx: self.tx.clone(),
        }
    }
}

impl<R> WalSender<R> {
    pub fn try_append(&self, entry: WalEntry<R>) -> Result<(), WalSendError<WalEntry<R>>> {
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

    /// Asks for a new active file, as at a seal (D8). The receiver resolves once the
    /// old file is durable and the new one is ready.
    pub fn try_rotate(&self) -> Result<oneshot::Receiver<WalRotated>, WalSendError<()>> {
        let (done_tx, done_rx) = oneshot::channel();
        match self.tx.try_send(WalCommand::Rotate(done_tx)) {
            Ok(()) => Ok(done_rx),
            Err(TrySendError::Full(_)) => Err(WalSendError::Full(())),
            Err(TrySendError::Disconnected(_)) => Err(WalSendError::Stopped(())),
        }
    }

    /// Closed files up to `up_to_seq` are covered by uploaded segments and checkpoints,
    /// so the next rotations may reuse them (D16). The active file is never affected.
    pub fn try_recycle(&self, up_to_seq: u64) -> Result<(), WalSendError<u64>> {
        match self.tx.try_send(WalCommand::Recycle { up_to_seq }) {
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
pub fn start_wal_writer<R, V, N>(
    mut volume: V,
    notify: N,
    config: WalConfig,
) -> io::Result<(WalSender<R>, WalHandle)>
where
    R: AsRef<[u8]> + Send + 'static,
    V: WalVolume + 'static,
    N: DurableNotify + 'static,
{
    let file = volume.create(config.first_seq)?;
    let (tx, rx) = bounded(config.channel_capacity);
    let writer = Writer {
        volume,
        notify,
        file,
        active_seq: config.first_seq,
        closed_seqs: config.closed_seqs.into(),
        free_seqs: VecDeque::new(),
        channel_capacity: config.channel_capacity,
        stats: WalStats::default(),
    };
    let thread = std::thread::Builder::new()
        .name("wal-writer".to_owned())
        .spawn(move || writer.run(rx))?;
    Ok((WalSender { tx }, WalHandle { thread }))
}

struct Writer<V: WalVolume, N> {
    volume: V,
    notify: N,
    file: V::File,
    active_seq: u64,
    closed_seqs: VecDeque<u64>,
    free_seqs: VecDeque<u64>,
    channel_capacity: usize,
    stats: WalStats,
}

impl<V: WalVolume, N: DurableNotify> Writer<V, N> {
    fn run<R: AsRef<[u8]>>(mut self, rx: Receiver<WalCommand<R>>) -> Result<WalStats, WalError> {
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
                batch.clear();
            }
            match control {
                Some(WalCommand::Rotate(done)) => {
                    let rotated = self.rotate().map_err(WalError::Rotate)?;
                    // A core that stopped waiting doesn't matter to the writer.
                    let _ = done.send(rotated);
                }
                Some(WalCommand::Recycle { up_to_seq }) => {
                    while self
                        .closed_seqs
                        .front()
                        .is_some_and(|seq| *seq <= up_to_seq)
                    {
                        self.free_seqs.extend(self.closed_seqs.pop_front());
                    }
                }
                Some(WalCommand::Append(_)) | None => {}
            }
        }
        Ok(self.stats)
    }

    /// Writes the group, syncs, and only then notifies. An error returns before
    /// any notification, and ends the thread.
    fn commit<R: AsRef<[u8]>>(
        &mut self,
        batch: &[WalEntry<R>],
        heads: &mut Vec<u8>,
    ) -> Result<(), WalError> {
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
        write_all_vectored(&mut self.file, &mut slices).map_err(WalError::Write)?;
        self.file.sync_data().map_err(WalError::Sync)?;

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
        let closed_seq = self.active_seq;
        let new_seq = closed_seq + 1;
        self.file = match self.free_seqs.pop_front() {
            Some(old_seq) => self.volume.reuse(old_seq, new_seq)?,
            None => self.volume.create(new_seq)?,
        };
        self.closed_seqs.push_back(closed_seq);
        self.active_seq = new_seq;
        Ok(WalRotated {
            closed_seq,
            new_seq,
        })
    }
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
