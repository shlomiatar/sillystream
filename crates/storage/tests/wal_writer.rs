//! The WAL writer's ordering, group commit and backpressure, with injected files, and
//! on the real disk.

mod common;

use std::io::{self, IoSlice};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use bytes::Bytes;
use storage::{
    DirVolume, DurableNotify, Handoff, SealExtent, Sealer, WalConfig, WalEntry, WalError, WalFile,
    WalRotated, WalSendError, WalVolume, durable_watches, encode_file_header, encode_wal_entry,
    replay_dir, start_wal_writer,
};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Create(u64),
    Remove(u64),
    Write(usize),
    Sync,
    Durable(usize, u64),
}

/// Everything the writer does, in order, plus what it wrote.
#[derive(Default)]
struct Log {
    calls: Mutex<Vec<Call>>,
    written: Mutex<Vec<u8>>,
}

impl Log {
    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn push(&self, call: Call) {
        self.calls.lock().unwrap().push(call);
    }
}

/// Makes syncs wait until released, and can fail on chosen calls.
#[derive(Default)]
struct Gate {
    held: Mutex<bool>,
    changed: Condvar,
    entered: Mutex<u64>,
    entered_changed: Condvar,
}

impl Gate {
    fn hold(&self) {
        *self.held.lock().unwrap() = true;
    }

    fn release(&self) {
        *self.held.lock().unwrap() = false;
        self.changed.notify_all();
    }

    fn wait_if_held(&self) {
        *self.entered.lock().unwrap() += 1;
        self.entered_changed.notify_all();
        let mut held = self.held.lock().unwrap();
        while *held {
            held = self.changed.wait(held).unwrap();
        }
    }

    /// Waits until the writer has entered `count` syncs, without any timer.
    fn wait_for_entered(&self, count: u64) {
        let mut entered = self.entered.lock().unwrap();
        while *entered < count {
            entered = self.entered_changed.wait(entered).unwrap();
        }
    }
}

#[derive(Clone, Default)]
struct Script {
    log: Arc<Log>,
    gate: Arc<Gate>,
    /// Fail the n-th sync (1-based), or the n-th write.
    fail_sync_at: u64,
    fail_write_at: u64,
    /// At most this many bytes per `write_vectored`, when not 0.
    max_write_bytes: usize,
}

struct ScriptFile {
    script: Script,
    sync_count: u64,
    write_count: u64,
}

impl WalFile for ScriptFile {
    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        self.write_count += 1;
        if self.write_count == self.script.fail_write_at {
            return Err(io::Error::other("injected write failure"));
        }
        let mut left = match self.script.max_write_bytes {
            0 => usize::MAX,
            max => max,
        };
        let mut total = 0;
        let mut written = self.script.log.written.lock().unwrap();
        for slice in slices {
            let take = slice.len().min(left);
            written.extend_from_slice(&slice[..take]);
            total += take;
            left -= take;
            if left == 0 {
                break;
            }
        }
        self.script.log.push(Call::Write(total));
        Ok(total)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.script.gate.wait_if_held();
        self.sync_count += 1;
        if self.sync_count == self.script.fail_sync_at {
            return Err(io::Error::other("injected sync failure"));
        }
        self.script.log.push(Call::Sync);
        Ok(())
    }
}

impl WalVolume for Script {
    type File = ScriptFile;

    fn create(&mut self, seq: u64) -> io::Result<ScriptFile> {
        self.log.push(Call::Create(seq));
        Ok(self.open_file())
    }

    fn remove(&mut self, seq: u64) -> io::Result<()> {
        self.log.push(Call::Remove(seq));
        Ok(())
    }
}

impl Script {
    fn open_file(&self) -> ScriptFile {
        ScriptFile {
            script: self.clone(),
            sync_count: 0,
            write_count: 0,
        }
    }
}

struct LogNotify(Arc<Log>);

impl DurableNotify for LogNotify {
    fn durable(&mut self, core: usize, ticket: u64) {
        self.0.push(Call::Durable(core, ticket));
    }
}

fn config(channel_capacity: usize) -> WalConfig {
    WalConfig {
        first_seq: 1,
        closed_seqs: Vec::new(),
        channel_capacity,
        segment_cap_bytes: u64::MAX,
        carried: Vec::new(),
        carried_dedupe: Vec::new(),
    }
}

fn entry(core: usize, ticket: u64) -> WalEntry<Bytes> {
    let records = Bytes::from(vec![ticket as u8; 40]);
    WalEntry::new(key("stream"), 7, ticket, records, core, ticket).unwrap()
}

fn durable_calls(calls: &[Call]) -> Vec<Call> {
    calls
        .iter()
        .filter(|call| matches!(call, Call::Durable(..)))
        .cloned()
        .collect()
}

#[test]
fn no_notification_comes_before_the_sync_covering_it() {
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(64))
            .unwrap();
    for ticket in 1..=200 {
        let mut pending = entry((ticket % 3) as usize, ticket);
        loop {
            match sender.try_append(pending) {
                Ok(()) => break,
                Err(WalSendError::Full(back)) => {
                    pending = back;
                    std::thread::yield_now();
                }
                Err(WalSendError::Stopped(_)) => panic!("writer stopped"),
            }
        }
    }
    drop(sender);
    let stats = handle.join().unwrap();
    assert_eq!(stats.entry_count, 200);

    // Replay the call log: track bytes written, and which are synced, per group.
    let calls = log.calls();
    let mut unsynced_writes = false;
    let mut last_durable = [0u64; 3];
    for call in &calls {
        match call {
            Call::Write(_) => unsynced_writes = true,
            Call::Sync => unsynced_writes = false,
            Call::Durable(core, ticket) => {
                assert!(!unsynced_writes, "notified with unsynced writes: {calls:?}");
                assert!(*ticket > last_durable[*core]);
                last_durable[*core] = *ticket;
            }
            _ => {}
        }
    }
    // Every core ends knowing its last ticket.
    assert_eq!(last_durable, [198, 199, 200]);
    let sync_count = calls.iter().filter(|call| **call == Call::Sync).count() as u64;
    assert_eq!(sync_count, stats.sync_count);
}

#[test]
fn notification_is_held_back_exactly_while_the_sync_is_held() {
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let gate = Arc::clone(&script.gate);
    gate.hold();
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    gate.wait_for_entered(1);
    // The sync has started and can't finish: the entry is written and not durable.
    let calls = log.calls();
    assert!(matches!(calls.last(), Some(Call::Write(_))), "{calls:?}");
    assert!(durable_calls(&calls).is_empty());
    gate.release();
    drop(sender);
    handle.join().unwrap();
    assert_eq!(durable_calls(&log.calls()), [Call::Durable(0, 1)]);
}

#[test]
fn a_sync_error_stops_notifications_and_the_writer() {
    let script = Script {
        fail_sync_at: 2,
        ..Script::default()
    };
    let log = Arc::clone(&script.log);
    let gate = Arc::clone(&script.gate);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    gate.wait_for_entered(1);
    // Wait for the first notification, so that the next entry gets the second sync.
    while durable_calls(&log.calls()).is_empty() {
        std::thread::yield_now();
    }
    sender.try_append(entry(0, 2)).unwrap();
    let error = handle.join().unwrap_err();
    assert!(matches!(error, WalError::Sync(_)), "{error}");
    assert_eq!(durable_calls(&log.calls()), [Call::Durable(0, 1)]);
    // The sync isn't retried, and senders learn the writer stopped.
    assert!(matches!(
        sender.try_append(entry(0, 3)),
        Err(WalSendError::Stopped(_))
    ));
    assert_eq!(
        log.calls().iter().filter(|c| **c == Call::Sync).count(),
        1,
        "the failed sync is not logged, and nothing follows it"
    );
}

#[test]
fn a_write_error_stops_before_any_sync_or_notification() {
    let script = Script {
        fail_write_at: 1,
        ..Script::default()
    };
    let log = Arc::clone(&script.log);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    let error = handle.join().unwrap_err();
    assert!(matches!(error, WalError::Write(_)), "{error}");
    assert_eq!(log.calls(), [Call::Create(1)]);
}

#[tokio::test]
async fn a_sync_error_wakes_awaiting_cores_with_an_error() {
    let script = Script {
        fail_sync_at: 1,
        ..Script::default()
    };
    let (notify, mut receivers) = durable_watches(1);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, notify, (), config(8)).unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    let mut receiver = receivers.pop().unwrap();
    assert!(receiver.changed().await.is_err());
    assert_eq!(*receiver.borrow(), 0);
    assert!(handle.join().is_err());
}

#[test]
fn entries_queued_during_a_sync_land_together_in_the_next_one() {
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let gate = Arc::clone(&script.gate);
    gate.hold();
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(1024))
            .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    gate.wait_for_entered(1);
    // The first sync is in progress. All of these queue during it.
    for ticket in 2..=501 {
        sender.try_append(entry(0, ticket)).unwrap();
    }
    gate.release();
    drop(sender);
    let stats = handle.join().unwrap();
    assert_eq!(stats.entry_count, 501);
    assert_eq!(stats.sync_count, 2, "1 sync for the 500, not 500");
    assert_eq!(stats.largest_group_count, 500);
    let durable = durable_calls(&log.calls());
    assert_eq!(durable, [Call::Durable(0, 1), Call::Durable(0, 501)]);
}

#[test]
fn the_writer_loop_has_no_timer() {
    // The group commit's whole claim. This is a word scan of the writer's source.
    let source = include_str!("../src/wal_writer.rs");
    let code: String = source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect();
    for word in [
        "sleep",
        "recv_timeout",
        "recv_deadline",
        "Instant",
        "Duration",
        "after(",
        "tick(",
        "SystemTime",
    ] {
        assert!(!code.contains(word), "the writer uses {word}");
    }
}

#[test]
fn try_append_on_a_full_channel_returns_at_once_with_a_stalled_writer() {
    let script = Script::default();
    let gate = Arc::clone(&script.gate);
    gate.hold();
    let (notify, _receivers) = durable_watches(1);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, notify, (), config(16)).unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    gate.wait_for_entered(1);
    for ticket in 2..=17 {
        sender.try_append(entry(0, ticket)).unwrap();
    }
    // The gate opens only after this thread has finished, so if any call blocked,
    // the test would hang instead of passing. It runs under a deadline to report that.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let sender_thread = {
        let sender = sender.clone();
        std::thread::spawn(move || {
            let mut full_count = 0;
            for ticket in 18..100_018 {
                match sender.try_append(entry(0, ticket)) {
                    Err(WalSendError::Full(_)) => full_count += 1,
                    other => panic!("expected a full channel, got {other:?}"),
                }
            }
            done_tx.send(full_count).unwrap();
        })
    };
    let full_count = done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("try_append blocked on a stalled writer");
    assert_eq!(full_count, 100_000);
    sender_thread.join().unwrap();
    gate.release();
    drop(sender);
    assert_eq!(handle.join().unwrap().entry_count, 17);
}

#[test]
fn partial_writes_still_produce_exact_bytes() {
    let script = Script {
        max_write_bytes: 7,
        ..Script::default()
    };
    let log = Arc::clone(&script.log);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    sender.try_append(entry(0, 2)).unwrap();
    drop(sender);
    handle.join().unwrap();
    let mut expected = encode_wal_entry(1, key("stream"), 7, 1, &[1; 40]).unwrap();
    expected.extend(encode_wal_entry(1, key("stream"), 7, 2, &[2; 40]).unwrap());
    assert_eq!(*log.written.lock().unwrap(), expected);
}

#[test]
fn rotation_syncs_the_old_file_first_and_only_ever_creates_files() {
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    let first = sender.try_seal(0).unwrap().blocking_recv().unwrap();
    assert_eq!(
        first,
        WalRotated {
            closed_seq: 1,
            new_seq: 2
        }
    );
    // The active file can't be removed.
    sender.try_remove(2).unwrap();
    // An empty file isn't sealed, so this makes no file.
    let empty = sender.try_seal(0).unwrap().blocking_recv().unwrap();
    assert_eq!(
        empty,
        WalRotated {
            closed_seq: 1,
            new_seq: 2
        }
    );
    sender.try_append(entry(0, 2)).unwrap();
    sender.try_seal(0).unwrap().blocking_recv().unwrap();
    drop(sender);
    handle.join().unwrap();
    let calls = log.calls();
    assert_eq!(
        calls,
        [
            Call::Create(1),
            Call::Write(calls_write_len()),
            Call::Sync,
            Call::Durable(0, 1),
            Call::Create(2),
            Call::Remove(1),
            // 2 was active at the removal, so it stays.
            Call::Write(calls_write_len()),
            Call::Sync,
            Call::Durable(0, 2),
            Call::Create(3),
        ]
    );
}

fn calls_write_len() -> usize {
    encode_wal_entry(1, key("stream"), 7, 1, &[1; 40])
        .unwrap()
        .len()
}

#[tokio::test]
async fn a_real_file_end_to_end_wakes_the_core_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let (notify, mut receivers) = durable_watches(2);
    let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
        DirVolume::new(dir.path(), 1 << 20),
        notify,
        (),
        config(64),
    )
    .unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    sender.try_append(entry(1, 1)).unwrap();
    sender.try_append(entry(0, 2)).unwrap();
    let core_zero = &mut receivers[0];
    while *core_zero.borrow_and_update() < 2 {
        core_zero.changed().await.unwrap();
    }
    let core_one = &mut receivers[1];
    while *core_one.borrow_and_update() < 1 {
        core_one.changed().await.unwrap();
    }
    // Durable means replayable, now, with the writer still running.
    let entries = replay_dir(dir.path()).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[2].producer_seq, 2);
    assert_eq!(entries[2].records, Bytes::from(vec![2u8; 40]));
    drop(sender);
    handle.join().unwrap();
}

#[test]
fn entries_that_the_format_cannot_hold_are_refused() {
    use storage::WalEntryError;
    assert_eq!(
        WalEntry::new(key("s"), 0, 0, Bytes::new(), 0, 0).unwrap_err(),
        WalEntryError::EmptyRecords
    );
}

/// A `Script` whose next opens fail with this OS error, as a descriptor limit would make them.
struct FailingOpens {
    script: Script,
    os_error: Arc<std::sync::atomic::AtomicI32>,
}

impl FailingOpens {
    fn check(&self) -> io::Result<()> {
        match self.os_error.load(std::sync::atomic::Ordering::SeqCst) {
            0 => Ok(()),
            code => Err(io::Error::from_raw_os_error(code)),
        }
    }
}

impl WalVolume for FailingOpens {
    type File = ScriptFile;

    fn create(&mut self, seq: u64) -> io::Result<ScriptFile> {
        self.check()?;
        self.script.create(seq)
    }

    fn remove(&mut self, seq: u64) -> io::Result<()> {
        self.script.remove(seq)
    }
}

const EMFILE: i32 = 24;
const ENFILE: i32 = 23;
const EIO: i32 = 5;

fn start_failing_opens() -> (
    storage::WalSender<Bytes>,
    storage::WalHandle,
    Arc<std::sync::atomic::AtomicI32>,
    Arc<Log>,
) {
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let os_error = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let volume = FailingOpens {
        script,
        os_error: Arc::clone(&os_error),
    };
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(volume, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    (sender, handle, os_error, log)
}

/// Running out of descriptors passes when connections end, so it stops no writer for good.
#[test]
fn a_rotation_out_of_descriptors_waits_and_writes_come_back() {
    for code in [EMFILE, ENFILE] {
        let (sender, handle, os_error, log) = start_failing_opens();
        sender.try_append(entry(0, 1)).unwrap();
        os_error.store(code, std::sync::atomic::Ordering::SeqCst);
        // The old file is closed, as with a full volume, so the seal can still upload it.
        let rotated = sender.try_seal(0).unwrap().blocking_recv().unwrap();
        assert_eq!(rotated.closed_seq, 1);
        assert!(matches!(
            sender.try_append(entry(0, 2)),
            Err(WalSendError::Full(_))
        ));
        // Still out: another seal is no worse.
        sender.try_seal(0).unwrap().blocking_recv().unwrap();
        sender.try_remove(0).unwrap();

        os_error.store(0, std::sync::atomic::Ordering::SeqCst);
        sender.try_seal(0).unwrap().blocking_recv().unwrap();
        sender.try_append(entry(0, 2)).unwrap();
        drop(sender);
        handle.join().unwrap();
        assert!(log.calls().contains(&Call::Durable(0, 2)));
    }
}

/// An append can pass the flag just before a seal finds no room for the next file. It waits,
/// unacked, for the file a later removal opens, rather than ending the writer (D233).
#[test]
fn an_append_queued_behind_a_seal_that_finds_no_room_waits_for_the_next_file() {
    let script = Script::default();
    let (log, gate) = (Arc::clone(&script.log), Arc::clone(&script.gate));
    let os_error = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let volume = FailingOpens {
        script,
        os_error: Arc::clone(&os_error),
    };
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(volume, LogNotify(Arc::clone(&log)), (), config(8))
            .unwrap();
    gate.hold();
    sender.try_append(entry(0, 1)).unwrap();
    gate.wait_for_entered(1);
    let rotated = sender.try_seal(0).unwrap();
    os_error.store(EMFILE, std::sync::atomic::Ordering::SeqCst);
    // The writer is still syncing, so nothing has said there is no file.
    sender.try_append(entry(0, 2)).unwrap();
    gate.release();
    assert_eq!(rotated.blocking_recv().unwrap().closed_seq, 1);
    assert!(!log.calls().contains(&Call::Durable(0, 2)));

    os_error.store(0, std::sync::atomic::Ordering::SeqCst);
    sender.try_remove(0).unwrap();
    drop(sender);
    handle.join().unwrap();
    assert!(log.calls().contains(&Call::Durable(0, 2)));
}

/// Anything else is what it was: a disk that can't be trusted, which ends the writer.
#[test]
fn a_rotation_with_another_error_still_stops_the_writer() {
    let (sender, handle, os_error, _log) = start_failing_opens();
    sender.try_append(entry(0, 1)).unwrap();
    os_error.store(EIO, std::sync::atomic::Ordering::SeqCst);
    let done = sender.try_seal(0).unwrap();
    assert!(done.blocking_recv().is_err());
    drop(sender);
    assert!(matches!(handle.join(), Err(WalError::Rotate(_))));
}

/// A sender that never pauses can't hold one group open: a group is the entry that woke the
/// writer and at most one channel's worth more.
#[test]
fn a_flood_cannot_grow_one_group_past_the_channel() {
    const CAPACITY: usize = 4;
    const ENTRY_COUNT: u64 = 200_000;
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _, _>(script, LogNotify(log), (), config(CAPACITY)).unwrap();
    let mut sent = 0;
    let mut ticket = 0;
    while sent < ENTRY_COUNT {
        ticket += 1;
        match sender.try_append(entry(0, ticket)) {
            Ok(_) => sent += 1,
            Err(_) => {
                ticket -= 1;
                std::thread::yield_now();
            }
        }
    }
    drop(sender);
    let stats = handle.join().unwrap();
    assert_eq!(stats.entry_count, ENTRY_COUNT);
    assert!(
        stats.largest_group_count <= CAPACITY as u64 + 1,
        "{stats:?}"
    );
    assert!(stats.largest_group_count > 1, "nothing grouped: {stats:?}");
}

/// A removed file's blocks and old bytes go with it, so every file starts with nothing in it,
/// and a map from replay keeps reading what it has (D263, D265).
#[test]
fn a_removed_file_is_gone_and_the_next_one_is_new() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(storage::wal_file_name(1)), b"before").unwrap();
    let (notify, _receivers) = durable_watches(1);
    let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
        DirVolume::new(dir.path(), 1 << 16),
        notify,
        (),
        WalConfig {
            first_seq: 2,
            closed_seqs: vec![1],
            channel_capacity: 16,
            segment_cap_bytes: u64::MAX,
            carried: Vec::new(),
            carried_dedupe: Vec::new(),
        },
    )
    .unwrap();
    sender.try_remove(1).unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    sender.try_seal(0).unwrap().blocking_recv().unwrap();
    drop(sender);
    handle.join().unwrap();
    let files = storage::list_wal_files(dir.path()).unwrap();
    assert_eq!(
        files.iter().map(|file| file.seq).collect::<Vec<_>>(),
        [2, 3]
    );
    let raw = std::fs::read(&files[1].path).unwrap();
    assert_eq!(
        &raw[..storage::WAL_FILE_HEADER_BYTES],
        encode_file_header(3, 0)
    );
    assert!(
        raw[storage::WAL_FILE_HEADER_BYTES..]
            .iter()
            .all(|byte| *byte == 0)
    );
}

/// What the writer handed over and asked for, in order.
#[derive(Default)]
struct Sealed {
    handoffs: Mutex<Vec<Handoff>>,
    full_count: Mutex<u64>,
}

struct RecordingSealer(Arc<Sealed>);

impl Sealer for RecordingSealer {
    fn full(&mut self) {
        *self.0.full_count.lock().unwrap() += 1;
    }

    fn seal(&mut self, handoff: Handoff) {
        self.0.handoffs.lock().unwrap().push(handoff);
    }
}

fn entry_for(stream: &str, core: usize, ticket: u64) -> WalEntry<Bytes> {
    let records = Bytes::from(vec![ticket as u8; 40]);
    WalEntry::new(key(stream), 7, ticket, records, core, ticket).unwrap()
}

fn start_sealing(
    config: WalConfig,
) -> (storage::WalSender<Bytes>, storage::WalHandle, Arc<Sealed>) {
    let sealed = Arc::new(Sealed::default());
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
        script,
        LogNotify(log),
        RecordingSealer(Arc::clone(&sealed)),
        config,
    )
    .unwrap();
    (sender, handle, sealed)
}

/// What a handoff holds, as (stream, first byte of each record) per extent, and its tickets.
fn shape(handoff: &Handoff) -> (Vec<(format::KeyHash, Vec<u8>)>, Vec<u64>) {
    let Handoff::Seal {
        extents, tickets, ..
    } = handoff;
    let extents = extents
        .iter()
        .map(|extent| {
            (
                extent.stream,
                extent.records.iter().map(|records| records[0]).collect(),
            )
        })
        .collect();
    (extents, tickets.clone())
}

/// A seal is its file: every entry it holds, by stream in write order, with each core's highest
/// ticket. An empty file isn't rotated, and its seal still reaches the uploader (D268).
#[test]
fn a_seal_hands_over_exactly_what_its_file_holds() {
    let (sender, handle, sealed) = start_sealing(config(64));
    sender.try_append(entry_for("b", 0, 1)).unwrap();
    sender.try_append(entry_for("a", 1, 2)).unwrap();
    sender.try_append(entry_for("b", 1, 3)).unwrap();
    sender.try_append(entry_for("b", 0, 4)).unwrap();
    let rotated = sender.try_seal(5).unwrap().blocking_recv().unwrap();
    assert_eq!(rotated.closed_seq, 1);
    let empty = sender.try_seal(6).unwrap().blocking_recv().unwrap();
    assert_eq!(empty.new_seq, 2, "nothing was written, so nothing rotated");
    drop(sender);
    handle.join().unwrap();
    let handoffs = sealed.handoffs.lock().unwrap();
    assert!(matches!(handoffs[0], Handoff::Seal { seal: 5, .. }));
    assert_eq!(
        shape(&handoffs[0]),
        (
            vec![(key("b"), vec![1, 3, 4]), (key("a"), vec![2])],
            vec![4, 3]
        )
    );
    assert!(matches!(handoffs[1], Handoff::Seal { seal: 6, .. }));
    // Each core's ticket stays where it got to, so a core that wrote nothing since still knows
    // what is covered.
    assert_eq!(shape(&handoffs[1]), (vec![], vec![4, 3]));
}

/// The cap is one total for the file, whichever cores wrote it, and it asks once per file.
#[test]
fn the_size_cap_asks_for_a_seal_once_per_file() {
    let (sender, handle, sealed) = start_sealing(WalConfig {
        segment_cap_bytes: 100,
        ..config(64)
    });
    sender.try_append(entry_for("a", 0, 1)).unwrap();
    sender.try_append(entry_for("b", 1, 2)).unwrap();
    sender.try_seal(1).unwrap().blocking_recv().unwrap();
    assert_eq!(
        *sealed.full_count.lock().unwrap(),
        0,
        "80 bytes is under it"
    );
    for ticket in 3..=6 {
        sender
            .try_append(entry_for("a", (ticket % 2) as usize, ticket))
            .unwrap();
    }
    sender.try_seal(2).unwrap().blocking_recv().unwrap();
    assert_eq!(*sealed.full_count.lock().unwrap(), 1);
    sender.try_append(entry_for("a", 0, 7)).unwrap();
    sender.try_append(entry_for("a", 0, 8)).unwrap();
    sender.try_append(entry_for("a", 0, 9)).unwrap();
    drop(sender);
    handle.join().unwrap();
    assert_eq!(
        *sealed.full_count.lock().unwrap(),
        2,
        "the next file asks again"
    );
}

/// One stream writing past the cap on its own asks for a seal, and the seal holds every record
/// it wrote, so a hot stream stays bounded with nothing lost.
#[test]
fn one_stream_past_the_cap_asks_for_a_seal_and_loses_nothing() {
    let (sender, handle, sealed) = start_sealing(WalConfig {
        segment_cap_bytes: 100,
        ..config(64)
    });
    for ticket in 1..=5 {
        sender.try_append(entry_for("hot", 0, ticket)).unwrap();
    }
    sender.try_seal(1).unwrap().blocking_recv().unwrap();
    drop(sender);
    handle.join().unwrap();
    assert_eq!(*sealed.full_count.lock().unwrap(), 1);
    let handoffs = sealed.handoffs.lock().unwrap();
    assert_eq!(handoffs.len(), 1);
    assert_eq!(
        shape(&handoffs[0]),
        (vec![(key("hot"), vec![1, 2, 3, 4, 5])], vec![5])
    );
}

/// What a former process left goes into the first seal ahead of anything new, each extent as
/// it was given.
#[test]
fn carried_records_go_into_the_first_seal_first() {
    let carried = |byte: u8| SealExtent {
        stream: key("s"),
        records: vec![Bytes::from(vec![byte; 40])],
    };
    let (sender, handle, sealed) = start_sealing(WalConfig {
        carried: vec![carried(100), carried(101)],
        carried_dedupe: Vec::new(),
        ..config(64)
    });
    sender.try_append(entry_for("s", 0, 1)).unwrap();
    sender.try_seal(1).unwrap().blocking_recv().unwrap();
    sender.try_append(entry_for("s", 0, 2)).unwrap();
    sender.try_seal(2).unwrap().blocking_recv().unwrap();
    drop(sender);
    handle.join().unwrap();
    let handoffs = sealed.handoffs.lock().unwrap();
    assert_eq!(
        shape(&handoffs[0]).0,
        [
            (key("s"), vec![100]),
            (key("s"), vec![101]),
            (key("s"), vec![1])
        ]
    );
    assert_eq!(shape(&handoffs[1]).0, [(key("s"), vec![2])]);
}

/// `(stream, producer_id, producer_seq, first_id, count)` of each carried dedupe entry in a file.
fn carried_in(file: &storage::WalFileInfo) -> Vec<(format::KeyHash, u64, u64, u64, u32)> {
    let mut carried: Vec<_> = storage::replay_file(file)
        .unwrap()
        .entries
        .iter()
        .filter_map(|entry| {
            let record = format::Records::new(&entry.records).next()?.ok()?;
            let system = format::SystemRecord::parse(&record).ok()?;
            (system.entry_type == format::ENTRY_DEDUPE_CARRY).then(|| {
                let (first_id, count) = format::decode_dedupe_carry(system.doc).unwrap();
                (
                    entry.stream,
                    entry.producer_id,
                    entry.producer_seq,
                    first_id,
                    count,
                )
            })
        })
        .collect();
    carried.sort();
    carried
}

/// A new file begins with the latest dedupe entry of each `(stream, producer_id)` the closed one
/// wrote, once: the carried entries go in no object, and alone they don't make a file rotate
/// (section 3).
#[test]
fn a_new_file_carries_the_closed_files_dedupe_entries_once() {
    let dir = tempfile::tempdir().unwrap();
    let (notify, _receivers) = durable_watches(2);
    let sealed = Arc::new(Sealed::default());
    let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
        DirVolume::new(dir.path(), 1 << 16),
        notify,
        RecordingSealer(Arc::clone(&sealed)),
        config(16),
    )
    .unwrap();
    let publish = |stream: &str, seq: u64, ticket: u64| {
        let records = Bytes::from(vec![ticket as u8; 40]);
        WalEntry::new(key(stream), 7, seq, records, 0, ticket).unwrap()
    };
    sender
        .try_append(publish("a", 1, 1).carrying(100, 2))
        .unwrap();
    sender
        .try_append(publish("a", 2, 2).carrying(102, 1))
        .unwrap();
    sender
        .try_append(publish("b", 1, 3).carrying(200, 3))
        .unwrap();
    // No producer: nothing to carry.
    sender.try_append(publish("c", 0, 4)).unwrap();
    assert_eq!(
        sender.try_seal(1).unwrap().blocking_recv().unwrap().new_seq,
        2
    );
    let idle = sender.try_seal(2).unwrap().blocking_recv().unwrap();
    assert_eq!(idle.new_seq, 2, "carried entries alone don't rotate a file");
    sender
        .try_append(publish("b", 2, 5).carrying(203, 1))
        .unwrap();
    assert_eq!(
        sender.try_seal(3).unwrap().blocking_recv().unwrap().new_seq,
        3
    );
    drop(sender);
    handle.join().unwrap();

    let files = storage::list_wal_files(dir.path()).unwrap();
    assert_eq!(
        files.iter().map(|file| file.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(carried_in(&files[0]), []);
    let (a, b) = (key("a"), key("b"));
    let mut expected = vec![(a, 7, 2, 102, 1), (b, 7, 1, 200, 3)];
    expected.sort();
    assert_eq!(carried_in(&files[1]), expected);
    // The file before's own entries, not what it carried.
    assert_eq!(carried_in(&files[2]), [(b, 7, 2, 203, 1)]);
    let handoffs = sealed.handoffs.lock().unwrap();
    assert_eq!(
        shape(&handoffs[1]).0,
        [],
        "an idle seal holds no carried entry"
    );
    assert_eq!(shape(&handoffs[2]).0, [(b, vec![5])]);
}

/// The first file begins with what replay refilled (section 3).
#[test]
fn the_first_file_carries_what_replay_refilled() {
    let dir = tempfile::tempdir().unwrap();
    let (notify, _receivers) = durable_watches(1);
    let carry = storage::DedupeCarry {
        stream: key("a"),
        producer_id: 9,
        producer_seq: 4,
        first_id: 500,
        count: 2,
    };
    let (sender, handle) = start_wal_writer::<Bytes, _, _, _>(
        DirVolume::new(dir.path(), 1 << 16),
        notify,
        (),
        WalConfig {
            carried_dedupe: vec![carry],
            ..config(16)
        },
    )
    .unwrap();
    drop(sender);
    handle.join().unwrap();
    let files = storage::list_wal_files(dir.path()).unwrap();
    assert_eq!(carried_in(&files[0]), [(key("a"), 9, 4, 500, 2)]);
}
