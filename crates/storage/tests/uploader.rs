//! The uploader: one PUT per seal, dedicated objects, and retries that lose nothing.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use common::{CountingStore, concat, record};
use format::id_ms;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use storage::{
    Handoff, ObjectId, SealExtent, SegmentCache, SegmentReader, UnfinishedUpload, UploadEvent,
    UploadKey, UploadSendError, Uploader, UploaderConfig, UploaderSender, sealed_path,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn stream_key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const FIRST_ID: u64 = 1_800_000_000_000 << 16;

/// A reference that shows when it is released, as a slab reference would.
#[derive(Clone, Debug)]
struct Tracked(Arc<Vec<u8>>);

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

fn config(store: Arc<dyn ObjectStore>) -> (UploaderConfig, UnboundedReceiver<UploadEvent>) {
    let (tx, rx) = unbounded_channel();
    (
        UploaderConfig {
            store,
            generation: 3,
            channel_capacity: 64,
            retry_initial_ms: 1,
            retry_max_ms: 4,
            put_timeout_min_ms: 60_000,
            cache: None,
            events: vec![tx],
            last_object_ms: 0,
        },
        rx,
    )
}

/// One extent per stream, for `stream_count` streams, sorted by name.
fn extents(stream_count: u64, base_id: u64) -> (Vec<SealExtent>, Vec<(String, Bytes)>) {
    let mut names: Vec<(String, u64)> = (0..stream_count)
        .map(|index| (format!("logs.{index:08}"), index))
        .collect();
    names.sort();
    let mut out = Vec::new();
    let mut expected = Vec::new();
    for (name, index) in names {
        let bytes = Bytes::from(record(base_id + index, index, 7, name.as_bytes()));
        out.push(SealExtent {
            stream: stream_key(name.as_str()),
            records: vec![bytes.clone()],
        });
        expected.push((name, bytes));
    }
    (out, expected)
}

fn seal(seal: u64, extents: Vec<SealExtent>) -> Handoff {
    Handoff::Seal {
        seal,
        extents,
        tickets: Vec::new(),
    }
}

fn one_stream_seal(number: u64, stream: &str, bytes: &Bytes) -> Handoff {
    seal(
        number,
        vec![SealExtent {
            stream: stream_key(stream),
            records: vec![bytes.clone()],
        }],
    )
}

fn dedicated(stream: &str, number: u64, records: Vec<Bytes>) -> Handoff {
    Handoff::Dedicated {
        stream: stream_key(stream),
        number,
        index: 1,
        records,
        tickets: Vec::new(),
    }
}

/// The next event that isn't `Sealed` or `Written`.
async fn next_event(events: &mut UnboundedReceiver<UploadEvent>) -> Option<UploadEvent> {
    loop {
        match events.recv().await {
            Some(UploadEvent::Sealed { .. } | UploadEvent::Written { .. }) => {}
            other => return other,
        }
    }
}

async fn next_uploaded(events: &mut UnboundedReceiver<UploadEvent>) -> (UploadKey, Path, u64) {
    loop {
        match events.recv().await.expect("the uploader stopped") {
            UploadEvent::Uploaded {
                key,
                path,
                len_bytes,
                ..
            } => return (key, path, len_bytes),
            UploadEvent::Retrying { .. }
            | UploadEvent::Sealed { .. }
            | UploadEvent::Written { .. } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
}

async fn one_put_per_seal(inner: Arc<dyn ObjectStore>) {
    let counting = Arc::new(CountingStore::new(inner));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config).unwrap();

    for (number, stream_count) in [(1u64, 10u64), (2, 1_000), (3, 20_000)] {
        let base_id = FIRST_ID + number * 1_000_000;
        let (extents, expected) = extents(stream_count, base_id);
        let before = counting.puts();
        sender.try_send(seal(number, extents)).unwrap();
        let (key, path, len_bytes) = next_uploaded(&mut events).await;
        assert_eq!(key, UploadKey::Seal(number));
        assert_eq!(counting.puts(), before + 1, "{stream_count} streams, 1 PUT");
        // The object's time is its newest record's, and the seal takes index 0.
        let id = ObjectId::of_path(&path).unwrap();
        assert_eq!((id.generation(), id.index()), (3, 0));
        assert!(id.time_ms() >= id_ms(base_id));
        assert_eq!(path, sealed_path(id));
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size as u64, len_bytes);

        let reader = SegmentReader::new(Arc::clone(&store), path);
        let top = reader.read_top_index().await.unwrap();
        for (name, bytes) in expected.iter().step_by(97) {
            let extents = reader.lookup(&top, stream_key(name)).await.unwrap();
            assert_eq!(extents.len(), 1);
            assert_eq!(&reader.read_extent(&extents[0]).await.unwrap(), bytes);
        }
    }
    drop(sender);
    assert!(uploader.finish().unfinished.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_put_per_seal_in_memory() {
    one_put_per_seal(Arc::new(InMemory::new())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_put_per_seal_on_the_local_file_system() {
    let dir = tempfile::tempdir().unwrap();
    one_put_per_seal(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ))
    .await;
}

/// Each extent is its own footer entry, in the order they came, even with another stream
/// between them.
#[tokio::test(flavor = "multi_thread")]
async fn one_stream_in_two_extents_of_a_seal_gets_one_region() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (config, mut events) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config).unwrap();
    let early = Bytes::from(record(FIRST_ID, 1, 7, b"early"));
    let other = Bytes::from(record(FIRST_ID + 1, 1, 7, b"other"));
    let late = Bytes::from(record(FIRST_ID + 2, 1, 7, b"late"));
    let extent = |stream: &str, bytes: &Bytes| SealExtent {
        stream: stream_key(stream),
        records: vec![bytes.clone()],
    };
    sender
        .try_send(seal(
            9,
            vec![
                extent("moved", &early),
                extent("a.other", &other),
                extent("moved", &late),
            ],
        ))
        .unwrap();
    let (_, path, _) = next_uploaded(&mut events).await;
    let reader = SegmentReader::new(Arc::clone(&store), path);
    let top = reader.read_top_index().await.unwrap();
    assert_eq!(
        reader
            .lookup(&top, stream_key("moved"))
            .await
            .unwrap()
            .len(),
        1
    );
    let both = [early, late].concat();
    assert_eq!(reader.read_stream(stream_key("moved")).await.unwrap(), both);
    assert_eq!(
        reader.read_stream(stream_key("a.other")).await.unwrap(),
        other
    );
    drop(sender);
    uploader.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_filling_its_buffer_gets_a_dedicated_object_at_the_spec_path() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config).unwrap();
    let first = Bytes::from(record(FIRST_ID, 1, 7, b"one"));
    let second = Bytes::from(record(FIRST_ID + 1, 1, 7, b"two"));
    sender
        .try_send(dedicated(
            "logs.busy",
            77,
            vec![first.clone(), second.clone()],
        ))
        .unwrap();
    let (key, path, _) = next_uploaded(&mut events).await;
    assert_eq!(
        key,
        UploadKey::Dedicated {
            stream: stream_key("logs.busy"),
            number: 77
        }
    );
    // Its own time, from its newest record, and the index the WAL writer gave it.
    assert_eq!(path, sealed_path(ObjectId::new(id_ms(FIRST_ID + 1), 3, 1)));
    assert_eq!(counting.puts(), 1);
    let reader = SegmentReader::new(store, path);
    let both = [first, second].concat();
    assert_eq!(
        reader.read_stream(stream_key("logs.busy")).await.unwrap(),
        both
    );
    drop(sender);
    uploader.finish();
}

/// The key an event is about.
fn key_of(event: &UploadEvent) -> &UploadKey {
    match event {
        UploadEvent::Sealed { key, .. }
        | UploadEvent::Written { key, .. }
        | UploadEvent::Uploaded { key, .. }
        | UploadEvent::Empty { key }
        | UploadEvent::Retrying { key, .. }
        | UploadEvent::Failed { key, .. } => key,
        UploadEvent::Restored { path } => panic!("no startup upload here: {path}"),
    }
}

/// Cores hear what an object covers before anything else about it, for seals and dedicated
/// objects alike.
#[tokio::test(flavor = "multi_thread")]
async fn the_sealed_event_comes_first_for_each_key_with_the_handoffs_tickets() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(2, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send(Handoff::Seal {
            seal: 4,
            extents: vec![SealExtent {
                stream: stream_key("s"),
                records: vec![bytes.clone()],
            }],
            tickets: vec![3, 0, 9],
        })
        .unwrap();
    sender
        .try_send(Handoff::Dedicated {
            stream: stream_key("d"),
            number: 8,
            index: 1,
            records: vec![bytes],
            tickets: vec![5, 1, 0],
        })
        .unwrap();
    let seal_key = UploadKey::Seal(4);
    let dedicated_key = UploadKey::Dedicated {
        stream: stream_key("d"),
        number: 8,
    };
    let mut seen = Vec::new();
    let mut uploaded = 0;
    while uploaded < 2 {
        let event = events.recv().await.expect("the uploader stopped");
        if matches!(event, UploadEvent::Uploaded { .. }) {
            uploaded += 1;
        }
        seen.push(event);
    }
    for (key, wanted) in [(&seal_key, [3, 0, 9]), (&dedicated_key, [5, 1, 0])] {
        let first = seen.iter().find(|event| key_of(event) == key).unwrap();
        match first {
            UploadEvent::Sealed { tickets, .. } => assert_eq!(**tickets, wanted),
            other => panic!("{key:?} began with {other:?}"),
        }
        let sealed_count = seen
            .iter()
            .filter(|event| key_of(event) == key && matches!(event, UploadEvent::Sealed { .. }))
            .count();
        assert_eq!(sealed_count, 1, "{seen:?}");
    }
    drop(sender);
    assert!(uploader.finish().unfinished.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn store_failures_retry_with_references_held_until_success() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(5, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config).unwrap();

    let wire = record(FIRST_ID, 1, 7, b"precious");
    let reference = Arc::new(wire.clone());
    let bytes = Bytes::from_owner(Tracked(Arc::clone(&reference)));
    sender.try_send(one_stream_seal(1, "s", &bytes)).unwrap();
    drop(bytes);

    let mut retries = 0;
    let path = loop {
        match events.recv().await.unwrap() {
            UploadEvent::Retrying { attempt, .. } => {
                retries += 1;
                assert_eq!(attempt, retries);
                assert_eq!(counting.puts(), 0);
                assert!(
                    Arc::strong_count(&reference) > 1,
                    "the reference must be held while retrying"
                );
            }
            UploadEvent::Uploaded { path, .. } => break path,
            UploadEvent::Sealed { .. } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    };
    assert_eq!(retries, 5);
    assert_eq!(counting.puts(), 1);
    assert_eq!(counting.put_attempt_count.load(Ordering::SeqCst), 6);
    let reader = SegmentReader::new(store, path);
    assert_eq!(reader.read_stream(stream_key("s")).await.unwrap(), wire);
    drop(sender);
    assert!(uploader.finish().unfinished.is_empty());
    assert_eq!(Arc::strong_count(&reference), 1, "released after success");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_an_outage_hands_every_reference_back() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let extent = |stream: &str| SealExtent {
        stream: stream_key(stream),
        records: vec![bytes.clone()],
    };
    sender
        .try_send(seal(1, vec![extent("a"), extent("b")]))
        .unwrap();
    // Wait until it is really retrying, then queue more behind it.
    assert!(matches!(
        next_event(&mut events).await,
        Some(UploadEvent::Retrying { .. })
    ));
    sender
        .try_send(dedicated("c", 1, vec![bytes.clone()]))
        .unwrap();
    sender.try_send(seal(2, vec![extent("d")])).unwrap();
    // The sender stays alive: shutdown must not depend on the channel closing.
    let exit = uploader.shutdown();
    assert_eq!(counting.puts(), 0);
    let mut seals = Vec::new();
    let mut dedicated_count = 0;
    for unfinished in exit.unfinished {
        match unfinished {
            UnfinishedUpload::Seal { seal, extents } => seals.push((seal, extents.len())),
            UnfinishedUpload::Dedicated {
                stream, records, ..
            } => {
                assert_eq!(stream, stream_key("c"));
                assert_eq!(records.len(), 1);
                dedicated_count += 1;
            }
        }
    }
    seals.sort();
    assert_eq!(seals, [(1, 2), (2, 1)]);
    assert_eq!(dedicated_count, 1);
    assert!(matches!(
        sender.try_send(dedicated("e", 2, Vec::new())),
        Err(UploadSendError::Stopped(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_nothing_to_do_returns_with_senders_alive() {
    let (config, _events) = config(Arc::new(InMemory::new()));
    let (sender, uploader) = Uploader::start(config).unwrap();
    assert!(uploader.shutdown().unfinished.is_empty());
    drop(sender);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_seal_with_no_records_writes_nothing() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    sender.try_send(seal(5, Vec::new())).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        Some(UploadEvent::Empty {
            key: UploadKey::Seal(5)
        })
    ));
    assert_eq!(counting.puts(), 0);
    drop(sender);
    uploader.finish();
}

/// Extents can come in any order. The object has them sorted, so every stream can be found.
#[tokio::test(flavor = "multi_thread")]
async fn unsorted_extents_are_uploaded_sorted_and_readable() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (config, mut events) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config).unwrap();
    let streams = ["c", "a", "b"];
    let mut extents = Vec::new();
    let mut expected = Vec::new();
    for (index, stream) in streams.iter().enumerate() {
        let bytes = Bytes::from(record(FIRST_ID + index as u64, 1, 7, stream.as_bytes()));
        extents.push(SealExtent {
            stream: stream_key(stream),
            records: vec![bytes.clone()],
        });
        expected.push((*stream, bytes));
    }
    sender.try_send(seal(1, extents)).unwrap();
    let (_, path, _) = next_uploaded(&mut events).await;
    let reader = SegmentReader::new(store, path);
    let top = reader.read_top_index().await.unwrap();
    for (stream, bytes) in expected {
        let found = reader.lookup(&top, stream_key(stream)).await.unwrap();
        assert_eq!(found.len(), 1, "{stream}");
        assert_eq!(reader.read_extent(&found[0]).await.unwrap(), bytes);
    }
    drop(sender);
    assert!(uploader.finish().unfinished.is_empty());
}

/// No two objects share an id, even with the same records. A PUT that succeeded with its
/// response lost finds its own bytes and counts as done, and other bytes under its id are left
/// alone and fail the upload.
#[tokio::test(flavor = "multi_thread")]
async fn ids_are_never_shared_and_only_the_same_bytes_count_as_done() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let (config_one, mut events_one) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config_one).unwrap();
    sender.try_send(one_stream_seal(1, "s", &bytes)).unwrap();
    let (_, path, _) = next_uploaded(&mut events_one).await;
    sender.try_send(one_stream_seal(2, "s", &bytes)).unwrap();
    let (_, next_path, _) = next_uploaded(&mut events_one).await;
    let (id, next_id) = (
        ObjectId::of_path(&path).unwrap(),
        ObjectId::of_path(&next_path).unwrap(),
    );
    assert_eq!(id, ObjectId::new(id_ms(FIRST_ID), 3, 0));
    assert_eq!(next_id, ObjectId::new(id_ms(FIRST_ID) + 1, 3, 0));
    drop(sender);
    assert_eq!(uploader.finish().unfinished.len(), 0);

    // The same first seal again: as if its response was lost.
    let (config_two, mut events_two) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config_two).unwrap();
    sender.try_send(one_stream_seal(1, "s", &bytes)).unwrap();
    assert_eq!(next_uploaded(&mut events_two).await.1, path);
    drop(sender);
    assert_eq!(uploader.finish().unfinished.len(), 0);

    // Other bytes for that id must not overwrite it.
    let other = Bytes::from(record(FIRST_ID, 1, 7, b"y"));
    let (config_three, mut events_three) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config_three).unwrap();
    sender.try_send(one_stream_seal(1, "s", &other)).unwrap();
    assert!(matches!(
        next_event(&mut events_three).await,
        Some(UploadEvent::Failed {
            key: UploadKey::Seal(1),
            ..
        })
    ));
    drop(sender);
    assert_eq!(uploader.finish().unfinished.len(), 1);
    let reader = SegmentReader::new(Arc::clone(&store), path);
    assert_eq!(reader.read_stream(stream_key("s")).await.unwrap(), bytes);
}

#[tokio::test(flavor = "multi_thread")]
async fn try_send_on_a_full_channel_returns_at_once() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (mut config, mut events) = config(store);
    config.channel_capacity = 2;
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send(dedicated("s", 0, vec![bytes.clone()]))
        .unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        Some(UploadEvent::Retrying { .. })
    ));
    let mut full_count = 0;
    for number in 1..1000 {
        match sender.try_send(dedicated("s", number, vec![bytes.clone()])) {
            Ok(()) => {}
            Err(UploadSendError::Full(_)) => full_count += 1,
            Err(UploadSendError::Stopped(_)) => panic!("stopped"),
        }
    }
    // At most 8 PUTs at once and 2 waiting in the channel, whatever was being retried.
    assert!(full_count >= 989, "{full_count}");
    let exit = uploader.shutdown();
    assert_eq!(exit.unfinished.len(), 1 + 999 - full_count);
}

#[tokio::test(flavor = "multi_thread")]
async fn uploaded_objects_reach_the_local_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    let local = cache.store();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (mut config, mut events) = config(Arc::clone(&inner));
    config.cache = Some(cache);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"cached"));
    sender
        .try_send(dedicated("s", 0, vec![bytes.clone()]))
        .unwrap();
    let (_, path, _) = next_uploaded(&mut events).await;
    // Same bytes from both tiers, through the same reader.
    let from_cache = SegmentReader::new(local.clone(), path.clone());
    assert_eq!(
        from_cache.read_stream(stream_key("s")).await.unwrap(),
        bytes
    );
    let cached = local.get(&path).await.unwrap().bytes().await.unwrap();
    let stored = inner.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(concat(&[cached]), concat(&[stored]));
    drop(sender);
    uploader.finish();
}

/// Events other than `Sealed` until `wanted` is `Uploaded`, failing if that takes longer than
/// 5 s. A wedged uploader never gets there.
async fn events_until_uploaded(
    events: &mut UnboundedReceiver<UploadEvent>,
    wanted: &UploadKey,
) -> Vec<UploadEvent> {
    let mut seen = Vec::new();
    let waited = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = next_event(events).await.expect("the uploader stopped");
            let done = matches!(&event, UploadEvent::Uploaded { key, .. } if key == wanted);
            seen.push(event);
            if done {
                return;
            }
        }
    })
    .await;
    assert!(
        waited.is_ok(),
        "{wanted:?} was not uploaded: {} events, the first {:?}",
        seen.len(),
        seen.first()
    );
    seen
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_error_that_retrying_cannot_change_fails_alone() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.reject_next_puts.store(1, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send(dedicated("rejected", 1, vec![bytes.clone()]))
        .unwrap();
    sender
        .try_send(one_stream_seal(1, "healthy", &bytes))
        .unwrap();
    let seen = events_until_uploaded(&mut events, &UploadKey::Seal(1)).await;
    assert!(matches!(seen[0], UploadEvent::Failed { .. }), "{seen:?}");
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(uploader.shutdown().unfinished.len(), 1);
}

/// Sends a new seal until told to stop or until the uploader has stopped. The store fails every
/// PUT, so none is ever uploaded, and an accepted one has to come back.
fn send_until_stopped(
    sender: &UploaderSender,
    give_up: &AtomicBool,
    spins_between_sends: u32,
) -> usize {
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let mut accepted = 0;
    let mut number = 0;
    while !give_up.load(Ordering::Relaxed) {
        match sender.try_send(one_stream_seal(number, "s", &bytes)) {
            Ok(()) => {
                accepted += 1;
                number += 1;
            }
            Err(UploadSendError::Full(_)) => {}
            Err(UploadSendError::Stopped(_)) => break,
        }
        for _ in 0..spins_between_sends {
            std::hint::spin_loop();
        }
    }
    accepted
}

fn failing_store() -> Arc<dyn ObjectStore> {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    counting
}

#[test]
fn every_handoff_accepted_around_shutdown_comes_back() {
    for run in 0..500 {
        let (mut config, _events) = config(failing_store());
        config.channel_capacity = 1024;
        let (sender, uploader) = Uploader::start(config).unwrap();
        let give_up = AtomicBool::new(false);
        let (accepted, returned) = std::thread::scope(|scope| {
            let sending = scope.spawn(|| send_until_stopped(&sender, &give_up, 200));
            std::thread::sleep(std::time::Duration::from_micros(50));
            let exit = uploader.shutdown();
            (sending.join().unwrap(), exit.unfinished.len())
        });
        assert_eq!(accepted, returned, "run {run}");
    }
}

#[test]
fn a_sender_that_never_pauses_cannot_hold_up_shutdown() {
    let (mut config, _events) = config(failing_store());
    config.channel_capacity = 1024;
    let (sender, uploader) = Uploader::start(config).unwrap();
    let give_up = AtomicBool::new(false);
    let (shut_down, returned_in_time, accepted) = std::thread::scope(|scope| {
        let sending = scope.spawn(|| send_until_stopped(&sender, &give_up, 0));
        std::thread::sleep(std::time::Duration::from_millis(20));
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let shutting_down = scope.spawn(move || {
            let exit = uploader.shutdown();
            let _ = done_tx.send(());
            exit
        });
        let returned_in_time = done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .is_ok();
        // Lets a stuck shutdown finish, so a failure here is a failure and not a hang.
        give_up.store(true, Ordering::Relaxed);
        (
            shutting_down.join().unwrap(),
            returned_in_time,
            sending.join().unwrap(),
        )
    });
    assert!(returned_in_time, "shutdown didn't return within 10 s");
    assert_eq!(shut_down.unfinished.len(), accepted);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_upload_leaves_room_for_the_seals_behind_it() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (mut config, mut events) = config(store);
    config.channel_capacity = 8;
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender.try_send(one_stream_seal(1, "s", &bytes)).unwrap();
    // It is now stuck on seal 1, reading nothing, as during a long PUT or an outage.
    assert!(matches!(
        next_event(&mut events).await,
        Some(UploadEvent::Retrying { .. })
    ));
    for number in 2..10 {
        sender
            .try_send(one_stream_seal(number, "s", &bytes))
            .unwrap_or_else(|error| panic!("seal {number} refused: {error:?}"));
    }
    assert_eq!(uploader.shutdown().unfinished.len(), 9);
}

/// The file is what a PUT sends, and it is on disk before the PUT starts. With the store away, it
/// waits in `pending/`, and once that reaches the cap, so do later seals.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_pending_directory_holds_later_seals_back() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let probe = {
        let mut builder = storage::SegmentBuilder::new();
        builder
            .add_extent(stream_key("s"), [bytes.clone()])
            .unwrap();
        builder.finish().unwrap().len_bytes
    };
    let cache = SegmentCache::open(dir.path(), probe).unwrap();
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting;
    let (mut config, mut events) = config(store);
    config.cache = Some(cache);
    let (sender, uploader) = Uploader::start(config).unwrap();
    sender
        .try_send(dedicated("s", 0, vec![bytes.clone()]))
        .unwrap();
    // The PUT is failing, so the file exists by now.
    assert!(matches!(
        next_event(&mut events).await,
        Some(UploadEvent::Retrying { .. })
    ));
    let mut files = Vec::new();
    list_files(&dir.path().join("pending"), &mut files);
    assert_eq!(files.len(), 1);
    assert_eq!(std::fs::metadata(&files[0]).unwrap().len(), probe);
    for number in 1..4 {
        let later = Bytes::from(record(FIRST_ID + (number << 16), 1, 7, b"x"));
        sender
            .try_send(dedicated("s", number, vec![later]))
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let mut files = Vec::new();
    list_files(&dir.path().join("pending"), &mut files);
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(uploader.shutdown().unfinished.len(), 4);
}

/// What a former process left in `pending/` is part of what `finish` waits for, even with
/// nothing new sent.
#[tokio::test(flavor = "multi_thread")]
async fn finish_waits_for_what_pending_held_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let failing = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    failing.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = failing;
    let (mut first, mut events) = config(store);
    first.cache = Some(SegmentCache::open(dir.path(), 1 << 20).unwrap());
    let (sender, uploader) = Uploader::start(first).unwrap();
    sender.try_send(dedicated("s", 0, vec![bytes])).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        Some(UploadEvent::Retrying { .. })
    ));
    drop(sender);
    assert_eq!(uploader.shutdown().unfinished.len(), 1);

    // A store that refuses a few times first, so the restore is still going when the last
    // sender goes.
    let inner = Arc::new(InMemory::new());
    let counting = Arc::new(CountingStore::new(Arc::clone(&inner) as Arc<dyn ObjectStore>));
    counting.fail_next_puts.store(20, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = Arc::clone(&counting) as Arc<dyn ObjectStore>;
    let (mut second, mut events) = config(store);
    second.cache = Some(SegmentCache::open(dir.path(), 1 << 20).unwrap());
    let (sender, uploader) = Uploader::start(second).unwrap();
    drop(sender);
    let exit = tokio::task::spawn_blocking(move || uploader.finish())
        .await
        .unwrap();
    assert!(exit.unfinished.is_empty());
    assert_eq!(counting.put_count.load(Ordering::SeqCst), 1);
    let mut restored = false;
    while let Ok(event) = events.try_recv() {
        restored |= matches!(event, UploadEvent::Restored { .. });
    }
    assert!(restored, "finish returned before the restore");
    let mut files = Vec::new();
    list_files(&dir.path().join("pending"), &mut files);
    assert!(files.is_empty(), "{files:?}");
}

fn list_files(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
    for item in std::fs::read_dir(dir).unwrap() {
        let item = item.unwrap();
        if item.metadata().unwrap().is_dir() {
            list_files(&item.path(), found);
        } else {
            found.push(item.path());
        }
    }
}

/// Runs `shutdown` on its own thread, so a stuck one is a failure and not a hung test.
/// The thread is left behind on failure, and ends with the test process.
fn shutdown_within_5_s(uploader: Uploader) -> Vec<UnfinishedUpload> {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(uploader.shutdown());
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("shutdown didn't return within 5 s")
        .unfinished
}

async fn wait_for_put_attempt(counting: &CountingStore) {
    while counting.put_attempt_count.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_interrupts_a_put_that_never_returns() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.hang_puts.store(true, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, _events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender.try_send(one_stream_seal(1, "s", &bytes)).unwrap();
    wait_for_put_attempt(&counting).await;
    let unfinished = shutdown_within_5_s(uploader);
    assert_eq!(unfinished.len(), 1);
    assert!(matches!(
        &unfinished[0],
        UnfinishedUpload::Seal { seal: 1, extents } if extents.len() == 1
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_does_not_wait_for_a_put_stuck_on_a_blocking_thread() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let (release, gate) = std::sync::mpsc::channel();
    *counting.block_next_put_on.lock().unwrap() = Some(gate);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, _events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender.try_send(dedicated("s", 7, vec![bytes])).unwrap();
    wait_for_put_attempt(&counting).await;
    let unfinished = shutdown_within_5_s(uploader);
    assert_eq!(unfinished.len(), 1);
    assert!(matches!(
        unfinished[0],
        UnfinishedUpload::Dedicated { number: 7, .. }
    ));
    drop(release);
}

/// A store whose first PUT never returns.
#[derive(Debug)]
struct HangsOnce {
    inner: InMemory,
    hung: AtomicBool,
}

impl std::fmt::Display for HangsOnce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HangsOnce")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HangsOnce {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if !self.hung.swap(true, Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// A PUT that doesn't return counts as a failed attempt, and doesn't hold up the node's uploads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_that_hangs_times_out_and_retries() {
    let store: Arc<dyn ObjectStore> = Arc::new(HangsOnce {
        inner: InMemory::new(),
        hung: AtomicBool::new(false),
    });
    let (mut config, mut events) = config(Arc::clone(&store));
    config.put_timeout_min_ms = 50;
    let (sender, uploader) = Uploader::start(config).unwrap();
    let (extents, _) = extents(4, FIRST_ID);
    sender.try_send(seal(1, extents)).unwrap();
    let mut retried = false;
    let path = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            match events.recv().await.expect("the uploader stopped") {
                UploadEvent::Uploaded { path, .. } => return path,
                UploadEvent::Retrying { error, .. } => {
                    assert!(error.contains("took over"), "{error}");
                    retried = true;
                }
                UploadEvent::Sealed { .. } => {}
                other => panic!("unexpected event: {other:?}"),
            }
        }
    })
    .await
    .expect("the upload stayed behind the hung PUT");
    assert!(retried);
    store.get(&path).await.unwrap();
    drop(sender);
    uploader.shutdown();
}

/// A store whose first PUT leaves a task on the caller's runtime, as an HTTP client leaves one to
/// drive a pooled connection.
#[derive(Debug)]
struct LeavesATask {
    inner: InMemory,
    ticks: Arc<std::sync::atomic::AtomicU64>,
    started: AtomicBool,
}

impl std::fmt::Display for LeavesATask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeavesATask")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LeavesATask {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if !self.started.swap(true, Ordering::SeqCst) {
            let ticks = Arc::clone(&self.ticks);
            tokio::spawn(async move {
                loop {
                    ticks.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            });
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// A shared client's other users wait on the connections this thread opened, so an idle uploader
/// has to keep running their tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_uploader_keeps_running_the_tasks_a_put_left() {
    let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(LeavesATask {
        inner: InMemory::new(),
        ticks: Arc::clone(&ticks),
        started: AtomicBool::new(false),
    });
    let (config, mut events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    let (extents, _) = extents(4, FIRST_ID);
    sender.try_send(seal(1, extents)).unwrap();
    loop {
        if let UploadEvent::Uploaded { .. } = events.recv().await.expect("the uploader stopped") {
            break;
        }
    }
    let before = ticks.load(Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while ticks.load(Ordering::SeqCst) < before + 5 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the task stopped running once the uploader went idle");
    drop(sender);
    uploader.shutdown();
}

/// Holds back PUTs of objects holding a stream in `held` until `release` is set, and counts the
/// PUTs waiting. Paths don't name streams, so it looks for the key hash in the bytes.
#[derive(Debug)]
struct HoldsPuts {
    inner: InMemory,
    /// Objects with any of these streams in them wait for `release`.
    held: Vec<format::KeyHash>,
    release: Arc<AtomicBool>,
    waiting: Arc<std::sync::atomic::AtomicU32>,
}

impl std::fmt::Display for HoldsPuts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HoldsPuts")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HoldsPuts {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let bytes: Vec<u8> = payload.iter().flat_map(|part| part.to_vec()).collect();
        if self.held.iter().any(|held| holds_key(&bytes, *held)) {
            self.waiting.fetch_add(1, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            self.waiting.fetch_sub(1, Ordering::SeqCst);
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn dedicated_records(stream: &str, number: u64, id: u64) -> Handoff {
    dedicated(
        stream,
        number,
        vec![Bytes::from(record(id, 0, 7, b"held back"))],
    )
}

async fn wait_for(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !done() {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// A slow PUT holds the store's next ones up no longer, and their events still come in order: a
/// seal's event lets its WAL files go, so it can't pass an earlier seal still on its way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn puts_overlap_and_their_events_stay_in_order() {
    let inner = InMemory::new();
    let release = Arc::new(AtomicBool::new(false));
    let waiting = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(HoldsPuts {
        inner,
        held: vec![stream_key("slow.stream")],
        release: Arc::clone(&release),
        waiting: Arc::clone(&waiting),
    });
    let (config, mut events) = config(Arc::clone(&store));
    let (sender, uploader) = Uploader::start(config).unwrap();
    sender
        .try_send(dedicated_records("slow.stream", 1, FIRST_ID))
        .unwrap();
    wait_for("the held PUT", || waiting.load(Ordering::SeqCst) == 1).await;
    sender
        .try_send(dedicated_records("fast.stream", 2, FIRST_ID + 1))
        .unwrap();

    let fast = sealed_path_of(&store, "fast.stream").await;
    assert!(fast.is_some(), "the second PUT waited for the first");
    // Only `Sealed` comes as an upload starts.
    while let Ok(event) = events.try_recv() {
        assert!(
            matches!(event, UploadEvent::Sealed { .. }),
            "an event passed an earlier PUT: {event:?}"
        );
    }

    release.store(true, Ordering::SeqCst);
    let numbers = [
        next_number(&mut events).await,
        next_number(&mut events).await,
    ];
    assert_eq!(numbers, [1, 2]);
    drop(sender);
    uploader.finish();
}

fn holds_key(bytes: &[u8], key: format::KeyHash) -> bool {
    bytes.windows(16).any(|window| window == key.0)
}

async fn sealed_path_of(store: &Arc<dyn ObjectStore>, stream: &str) -> Option<Path> {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let listed = storage::list_sealed_objects(store.as_ref()).await.unwrap();
            for object in listed {
                let bytes = store.get(&object.location).await.unwrap().bytes().await;
                if holds_key(&bytes.unwrap(), stream_key(stream)) {
                    return object.location;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .ok()
}

async fn next_number(events: &mut UnboundedReceiver<UploadEvent>) -> u64 {
    match next_event(events).await.expect("the uploader stopped") {
        UploadEvent::Uploaded {
            key: UploadKey::Dedicated { number, .. },
            ..
        } => number,
        other => panic!("unexpected event: {other:?}"),
    }
}

/// Every PUT under way when the uploader stops hands its references back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_returns_every_put_under_way() {
    let release = Arc::new(AtomicBool::new(false));
    let waiting = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(HoldsPuts {
        inner: InMemory::new(),
        held: (0..3)
            .map(|number| stream_key(&format!("held.{number}")))
            .collect(),
        release,
        waiting: Arc::clone(&waiting),
    });
    let (config, _events) = config(store);
    let (sender, uploader) = Uploader::start(config).unwrap();
    for number in 0..3 {
        let handoff = dedicated_records(&format!("held.{number}"), number, FIRST_ID + number);
        sender.try_send(handoff).unwrap();
    }
    wait_for("three held PUTs", || waiting.load(Ordering::SeqCst) == 3).await;
    drop(sender);
    let exit = uploader.shutdown();
    assert_eq!(exit.unfinished.len(), 3);
    assert!(
        exit.unfinished
            .iter()
            .all(|left| matches!(left, UnfinishedUpload::Dedicated { .. }))
    );
}

/// A store whose first PUTs fail with an error that can't be told from an outage.
#[derive(Debug)]
struct FailsAtFirst {
    inner: InMemory,
    left: std::sync::atomic::AtomicU32,
}

impl std::fmt::Display for FailsAtFirst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FailsAtFirst")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailsAtFirst {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self
            .left
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(object_store::Error::Generic {
                store: "FailsAtFirst",
                source: "down".into(),
            });
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Waits double up to `retry_max_ms`. The lower bounds are exact; the upper one is loose, since
/// a loaded machine can only make a wait longer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_back_off_exponentially_up_to_the_maximum() {
    let store: Arc<dyn ObjectStore> = Arc::new(FailsAtFirst {
        inner: InMemory::new(),
        left: std::sync::atomic::AtomicU32::new(7),
    });
    let (mut config, mut events) = config(store);
    config.retry_initial_ms = 25;
    config.retry_max_ms = 100;
    let (sender, uploader) = Uploader::start(config).unwrap();
    let (extents, _) = extents(4, FIRST_ID);
    sender.try_send(seal(1, extents)).unwrap();
    let mut times = Vec::new();
    loop {
        match events.recv().await.expect("the uploader stopped") {
            UploadEvent::Retrying { .. } => times.push(std::time::Instant::now()),
            UploadEvent::Uploaded { .. } => break,
            UploadEvent::Sealed { .. } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
    let gaps_ms: Vec<u128> = times
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).as_millis())
        .collect();
    assert_eq!(gaps_ms.len(), 6, "{gaps_ms:?}");
    // Event times can be a little closer than the waits, so these allow a few ms.
    for (gap_ms, least_ms) in gaps_ms.iter().zip([25u128, 50, 100, 100, 100, 100]) {
        assert!(*gap_ms + 5 >= least_ms, "{gaps_ms:?}");
    }
    // Uncapped, these would be 400, 800 and 1,600 ms.
    assert!(gaps_ms[5] < 800, "{gaps_ms:?}");
    drop(sender);
    uploader.shutdown();
}
