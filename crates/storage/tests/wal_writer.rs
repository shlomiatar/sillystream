//! The WAL writer's ordering, group commit and backpressure, with injected files, and
//! on the real disk.

use std::io::{self, IoSlice};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use bytes::Bytes;
use storage::{
    DirVolume, DurableNotify, WalConfig, WalEntry, WalError, WalFile, WalRotated, WalSendError,
    WalVolume, durable_watches, encode_file_header, encode_wal_entry, replay_dir, start_wal_writer,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Create(u64),
    Reuse(u64, u64),
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

    fn reuse(&mut self, old_seq: u64, new_seq: u64) -> io::Result<ScriptFile> {
        self.log.push(Call::Reuse(old_seq, new_seq));
        Ok(self.open_file())
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
    }
}

fn entry(core: usize, ticket: u64) -> WalEntry<Bytes> {
    let records = Bytes::from(vec![ticket as u8; 40]);
    WalEntry::new(Arc::from("stream"), 7, ticket, records, core, ticket).unwrap()
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
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(64)).unwrap();
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
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(8)).unwrap();
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
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(8)).unwrap();
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
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(8)).unwrap();
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
    let (sender, handle) = start_wal_writer::<Bytes, _, _>(script, notify, config(8)).unwrap();
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
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(1024)).unwrap();
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
    let (sender, handle) = start_wal_writer::<Bytes, _, _>(script, notify, config(16)).unwrap();
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
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(8)).unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    sender.try_append(entry(0, 2)).unwrap();
    drop(sender);
    handle.join().unwrap();
    let mut expected = encode_wal_entry(1, "stream", 7, 1, &[1; 40]).unwrap();
    expected.extend(encode_wal_entry(1, "stream", 7, 2, &[2; 40]).unwrap());
    assert_eq!(*log.written.lock().unwrap(), expected);
}

#[test]
fn rotation_syncs_the_old_file_first_and_reuses_only_recycled_files() {
    let script = Script::default();
    let log = Arc::clone(&script.log);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _>(script, LogNotify(Arc::clone(&log)), config(8)).unwrap();
    sender.try_append(entry(0, 1)).unwrap();
    let first = sender.try_rotate().unwrap().blocking_recv().unwrap();
    assert_eq!(
        first,
        WalRotated {
            closed_seq: 1,
            new_seq: 2
        }
    );
    // Nothing recycled yet: a new file. The active file can't be recycled.
    sender.try_recycle(2).unwrap();
    sender.try_rotate().unwrap().blocking_recv().unwrap();
    sender.try_rotate().unwrap().blocking_recv().unwrap();
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
            Call::Reuse(1, 3),
            // 2 was active at the recycle, so it isn't reused.
            Call::Create(4),
        ]
    );
}

fn calls_write_len() -> usize {
    encode_wal_entry(1, "stream", 7, 1, &[1; 40]).unwrap().len()
}

#[tokio::test]
async fn a_real_file_end_to_end_wakes_the_core_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let (notify, mut receivers) = durable_watches(2);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _>(DirVolume::new(dir.path(), 1 << 20), notify, config(64))
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
fn a_real_reused_file_returns_none_of_its_stale_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (notify, _receivers) = durable_watches(1);
    let (sender, handle) =
        start_wal_writer::<Bytes, _, _>(DirVolume::new(dir.path(), 1 << 16), notify, config(64))
            .unwrap();
    for ticket in 1..=20 {
        sender.try_append(entry(0, ticket)).unwrap();
    }
    sender.try_rotate().unwrap().blocking_recv().unwrap();
    sender.try_recycle(1).unwrap();
    // File 3 is file 1 renamed, with its 20 old entries still in place after the first.
    sender.try_rotate().unwrap().blocking_recv().unwrap();
    sender.try_append(entry(0, 21)).unwrap();
    drop(sender);
    handle.join().unwrap();
    let names: Vec<u64> = storage::list_wal_files(dir.path())
        .unwrap()
        .iter()
        .map(|file| file.seq)
        .collect();
    assert_eq!(names, [2, 3]);
    let raw = std::fs::read(dir.path().join(storage::wal_file_name(3))).unwrap();
    assert_eq!(&raw[..16], encode_file_header(3));
    // The stale bytes really are there, past the new entry.
    let stale = encode_wal_entry(1, "stream", 7, 2, &[2; 40]).unwrap();
    assert!(raw.windows(stale.len()).any(|window| window == stale));
    let entries = replay_dir(dir.path()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].producer_seq, 21);
}

#[test]
fn entries_that_the_format_cannot_hold_are_refused() {
    use storage::WalEntryError;
    let records = Bytes::from_static(&[0; 8]);
    let long: Arc<str> = Arc::from("x".repeat(65_536));
    assert_eq!(
        WalEntry::new(Arc::from(""), 0, 0, records.clone(), 0, 0).unwrap_err(),
        WalEntryError::EmptyStream
    );
    assert_eq!(
        WalEntry::new(long, 0, 0, records, 0, 0).unwrap_err(),
        WalEntryError::StreamTooLong
    );
    assert_eq!(
        WalEntry::new(Arc::from("s"), 0, 0, Bytes::new(), 0, 0).unwrap_err(),
        WalEntryError::EmptyRecords
    );
}
