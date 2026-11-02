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
    Section, SectionExtent, SegmentCache, SegmentReader, UnfinishedUpload, UploadEvent, UploadKey,
    UploadSendError, Uploader, UploaderConfig, UploaderSender, dedicated_path, seal_path,
    stream_fits_dedicated_path,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

const FIRST_ID: u64 = 1_800_000_000_000 << 16;
const SECTIONS: usize = 4;

/// A reference that shows when it is released, as a slab reference would.
#[derive(Clone, Debug)]
struct Tracked(Arc<Vec<u8>>);

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

fn config(
    store: Arc<dyn ObjectStore>,
    section_count: usize,
) -> (UploaderConfig, UnboundedReceiver<UploadEvent>) {
    let (tx, rx) = unbounded_channel();
    (
        UploaderConfig {
            store,
            node: "node-1".to_owned(),
            generation: 3,
            section_count,
            channel_capacity: 64,
            retry_initial_ms: 1,
            retry_max_ms: 4,
            put_timeout_min_ms: 60_000,
            cache: None,
            events: vec![tx],
        },
        rx,
    )
}

/// `SECTIONS` sections over `stream_count` streams, spread by name.
fn sections(stream_count: u64, base_id: u64) -> (Vec<Section<Bytes>>, Vec<(String, Bytes)>) {
    let mut names: Vec<(String, u64)> = (0..stream_count)
        .map(|index| (format!("logs.{index:08}"), index))
        .collect();
    names.sort();
    let mut out: Vec<Section<Bytes>> = (0..SECTIONS)
        .map(|_| Section {
            extents: Vec::new(),
        })
        .collect();
    let mut expected = Vec::new();
    for (name, index) in names {
        let bytes = Bytes::from(record(base_id + index, index, 7, name.as_bytes()));
        out[(index % SECTIONS as u64) as usize]
            .extents
            .push(SectionExtent {
                stream: Arc::from(name.as_str()),
                records: vec![bytes.clone()],
            });
        expected.push((name, bytes));
    }
    (out, expected)
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
            UploadEvent::Retrying { .. } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
}

async fn one_put_per_seal(inner: Arc<dyn ObjectStore>) {
    let counting = Arc::new(CountingStore::new(inner));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(Arc::clone(&store), SECTIONS);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();

    for (seal, stream_count) in [(1u64, 10u64), (2, 1_000), (3, 20_000)] {
        let base_id = FIRST_ID + seal * 1_000_000;
        let (sections, expected) = sections(stream_count, base_id);
        let before = counting.puts();
        for (core, section) in sections.into_iter().enumerate() {
            // Nothing is uploaded until the last section arrives.
            assert_eq!(counting.puts(), before);
            sender.try_send_section(seal, core, section).unwrap();
        }
        let (key, path, len_bytes) = next_uploaded(&mut events).await;
        assert_eq!(key, UploadKey::Seal(seal));
        assert_eq!(counting.puts(), before + 1, "{stream_count} streams, 1 PUT");
        assert_eq!(path, seal_path("node-1", 3, base_id));
        let hour = id_ms(base_id) / 3_600_000;
        assert_eq!(
            path.to_string(),
            format!("sealed/{hour}/node-1/{:020}-{base_id:020}.seg", 3)
        );
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size as u64, len_bytes);

        let reader = SegmentReader::new(Arc::clone(&store), path);
        let top = reader.read_top_index().await.unwrap();
        for (name, bytes) in expected.iter().step_by(97) {
            let extents = reader.lookup(&top, name).await.unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_moved_cores_gets_two_entries_in_one_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (config, mut events) = config(Arc::clone(&store), 2);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let early = Bytes::from(record(FIRST_ID, 1, 7, b"early"));
    let late = Bytes::from(record(FIRST_ID + 1, 1, 7, b"late"));
    for (core, bytes) in [(1, late.clone()), (0, early.clone())] {
        let section = Section {
            extents: vec![SectionExtent {
                stream: Arc::from("moved"),
                records: vec![bytes],
            }],
        };
        sender.try_send_section(9, core, section).unwrap();
    }
    let (_, path, _) = next_uploaded(&mut events).await;
    let reader = SegmentReader::new(Arc::clone(&store), path);
    let top = reader.read_top_index().await.unwrap();
    assert_eq!(reader.lookup(&top, "moved").await.unwrap().len(), 2);
    let both = [early, late].concat();
    assert_eq!(reader.read_stream("moved").await.unwrap(), both);
    drop(sender);
    uploader.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_filling_its_buffer_gets_a_dedicated_object_at_the_spec_path() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(Arc::clone(&store), SECTIONS);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let first = Bytes::from(record(FIRST_ID, 1, 7, b"one"));
    let second = Bytes::from(record(FIRST_ID + 1, 1, 7, b"two"));
    sender
        .try_send_dedicated(
            Arc::from("logs.busy"),
            77,
            vec![first.clone(), second.clone()],
        )
        .unwrap();
    let (key, path, _) = next_uploaded(&mut events).await;
    assert_eq!(
        key,
        UploadKey::Dedicated {
            stream: Arc::from("logs.busy"),
            number: 77
        }
    );
    assert_eq!(path, dedicated_path("logs.busy", 3, FIRST_ID));
    let hour = id_ms(FIRST_ID) / 3_600_000;
    assert_eq!(
        path.to_string(),
        format!("sealed/{hour}/logs.busy/{:020}-{FIRST_ID:020}.seg", 3)
    );
    assert_eq!(counting.puts(), 1);
    let reader = SegmentReader::new(store, path);
    let both = [first, second].concat();
    assert_eq!(reader.read_stream("logs.busy").await.unwrap(), both);
    drop(sender);
    uploader.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn store_failures_retry_with_references_held_until_success() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(5, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(Arc::clone(&store), 1);
    let (sender, uploader) = Uploader::<Tracked>::start(config).unwrap();

    let wire = record(FIRST_ID, 1, 7, b"precious");
    let reference = Arc::new(wire.clone());
    let section = Section {
        extents: vec![SectionExtent {
            stream: Arc::from("s"),
            records: vec![Tracked(Arc::clone(&reference))],
        }],
    };
    sender.try_send_section(1, 0, section).unwrap();

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
            other => panic!("unexpected event: {other:?}"),
        }
    };
    assert_eq!(retries, 5);
    assert_eq!(counting.puts(), 1);
    assert_eq!(counting.put_attempt_count.load(Ordering::SeqCst), 6);
    let reader = SegmentReader::new(store, path);
    assert_eq!(reader.read_stream("s").await.unwrap(), wire);
    drop(sender);
    assert!(uploader.finish().unfinished.is_empty());
    assert_eq!(Arc::strong_count(&reference), 1, "released after success");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_an_outage_hands_every_reference_back() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 2);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let section = |stream: &str| Section {
        extents: vec![SectionExtent {
            stream: Arc::from(stream),
            records: vec![bytes.clone()],
        }],
    };
    sender.try_send_section(1, 0, section("a")).unwrap();
    sender.try_send_section(1, 1, section("b")).unwrap();
    // Wait until it is really retrying, then queue more behind it.
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Retrying { .. })
    ));
    sender
        .try_send_dedicated(Arc::from("c"), 1, vec![bytes.clone()])
        .unwrap();
    sender.try_send_section(2, 0, section("d")).unwrap();
    // The sender stays alive: shutdown must not depend on the channel closing.
    let exit = uploader.shutdown();
    assert_eq!(counting.puts(), 0);
    let mut seals = Vec::new();
    let mut dedicated = 0;
    for unfinished in exit.unfinished {
        match unfinished {
            UnfinishedUpload::Seal { seal, sections } => seals.push((seal, sections.len())),
            UnfinishedUpload::Dedicated {
                stream, records, ..
            } => {
                assert_eq!(&*stream, "c");
                assert_eq!(records.len(), 1);
                dedicated += 1;
            }
        }
    }
    seals.sort();
    assert_eq!(seals, [(1, 2), (2, 1)]);
    assert_eq!(dedicated, 1);
    assert!(matches!(
        sender.try_send_dedicated(Arc::from("e"), 2, Vec::new()),
        Err(UploadSendError::Stopped(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_nothing_to_do_returns_with_senders_alive() {
    let (config, _events) = config(Arc::new(InMemory::new()), 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    assert!(uploader.shutdown().unfinished.is_empty());
    drop(sender);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_seal_with_no_records_writes_nothing() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 2);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    for core in 0..2 {
        let section = Section {
            extents: Vec::new(),
        };
        sender.try_send_section(5, core, section).unwrap();
    }
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Empty {
            key: UploadKey::Seal(5)
        })
    ));
    assert_eq!(counting.puts(), 0);
    drop(sender);
    uploader.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unsorted_section_fails_and_keeps_its_references() {
    let (config, mut events) = config(Arc::new(InMemory::new()), 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let extent = |stream: &str| SectionExtent {
        stream: Arc::from(stream),
        records: vec![bytes.clone()],
    };
    let section = Section {
        extents: vec![extent("b"), extent("a")],
    };
    sender.try_send_section(1, 0, section).unwrap();
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Failed { .. })
    ));
    drop(sender);
    assert_eq!(uploader.finish().unfinished.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_after_a_lost_response_counts_as_done_and_a_collision_takes_the_next_name() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let section = |bytes: &Bytes| Section {
        extents: vec![SectionExtent {
            stream: Arc::from("s"),
            records: vec![bytes.clone()],
        }],
    };
    let (config_one, mut events_one) = config(Arc::clone(&store), 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config_one).unwrap();
    sender.try_send_section(1, 0, section(&bytes)).unwrap();
    let (_, path, _) = next_uploaded(&mut events_one).await;
    // The same bytes again, as after a PUT that succeeded with its response lost.
    sender.try_send_section(2, 0, section(&bytes)).unwrap();
    assert_eq!(next_uploaded(&mut events_one).await.1, path);
    // Different bytes for the same path must not overwrite the first object, and still have
    // to reach the store.
    let other = Bytes::from(record(FIRST_ID, 1, 7, b"y"));
    sender.try_send_section(3, 0, section(&other)).unwrap();
    let (_, other_path, _) = next_uploaded(&mut events_one).await;
    assert_eq!(other_path, seal_path("node-1", 3, FIRST_ID + 1));
    drop(sender);
    assert_eq!(uploader.finish().unfinished.len(), 0);
    let reader = SegmentReader::new(Arc::clone(&store), path);
    assert_eq!(reader.read_stream("s").await.unwrap(), bytes);
    let reader = SegmentReader::new(store, other_path);
    assert_eq!(reader.read_stream("s").await.unwrap(), other);
}

#[tokio::test(flavor = "multi_thread")]
async fn try_send_on_a_full_channel_returns_at_once() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (mut config, mut events) = config(store, 1);
    config.channel_capacity = 2;
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_dedicated(Arc::from("s"), 0, vec![bytes.clone()])
        .unwrap();
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Retrying { .. })
    ));
    let mut full_count = 0;
    for number in 1..1000 {
        match sender.try_send_dedicated(Arc::from("s"), number, vec![bytes.clone()]) {
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
    let (mut config, mut events) = config(Arc::clone(&inner), 1);
    config.cache = Some(cache);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"cached"));
    sender
        .try_send_dedicated(Arc::from("s"), 0, vec![bytes.clone()])
        .unwrap();
    let (_, path, _) = next_uploaded(&mut events).await;
    assert!(sender.try_cache_touch(path.clone()));
    // Same bytes from both tiers, through the same reader.
    let from_cache = SegmentReader::new(local.clone(), path.clone());
    assert_eq!(from_cache.read_stream("s").await.unwrap(), bytes);
    let cached = local.get(&path).await.unwrap().bytes().await.unwrap();
    let stored = inner.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(concat(&[cached]), concat(&[stored]));
    drop(sender);
    uploader.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_section_from_one_core_keeps_both_and_does_not_fire_the_seal() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 2);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let section = |id: u64| Section {
        extents: vec![SectionExtent {
            stream: Arc::from("s"),
            records: vec![Bytes::from(record(id, 1, 7, b"x"))],
        }],
    };
    sender.try_send_section(1, 0, section(FIRST_ID)).unwrap();
    sender
        .try_send_section(1, 0, section(FIRST_ID + 1))
        .unwrap();
    sender
        .try_send_section(1, 9, section(FIRST_ID + 2))
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            events.recv().await,
            Some(UploadEvent::Failed { .. })
        ));
    }
    assert_eq!(counting.puts(), 0);
    drop(sender);
    let unfinished = uploader.finish().unfinished;
    let held: usize = unfinished
        .iter()
        .map(|upload| match upload {
            UnfinishedUpload::Seal { sections, .. } => sections.len(),
            UnfinishedUpload::Dedicated { .. } => 0,
        })
        .sum();
    assert_eq!(held, 3, "every section comes back");
}

/// Events until `wanted` is `Uploaded`, failing if that takes longer than 5 s. A
/// wedged uploader never gets there.
async fn events_until_uploaded(
    events: &mut UnboundedReceiver<UploadEvent>,
    wanted: &UploadKey,
) -> Vec<UploadEvent> {
    let mut seen = Vec::new();
    let waited = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = events.recv().await.expect("the uploader stopped");
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

fn one_stream_section(stream: &str, bytes: &Bytes) -> Section<Bytes> {
    Section {
        extents: vec![SectionExtent {
            stream: Arc::from(stream),
            records: vec![bytes.clone()],
        }],
    }
}

async fn a_stream_id_too_long_for_a_path_fails_alone(store: Arc<dyn ObjectStore>) {
    let (config, mut events) = config(store, 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    // Over the limit as encoded in a path, including ids short enough as raw bytes.
    let long_ids = [
        "x".repeat(300),
        "x".repeat(256),
        "#".repeat(100),
        "é".repeat(60),
    ];
    for (number, id) in long_ids.iter().enumerate() {
        assert!(!stream_fits_dedicated_path(id));
        sender
            .try_send_dedicated(Arc::from(id.as_str()), number as u64, vec![bytes.clone()])
            .unwrap();
    }
    // The longest id that fits, and a long id in a seal, which puts it in no path.
    assert!(stream_fits_dedicated_path(&"x".repeat(255)));
    sender
        .try_send_dedicated(Arc::from("x".repeat(255)), 100, vec![bytes.clone()])
        .unwrap();
    sender
        .try_send_section(1, 0, one_stream_section(&"y".repeat(300), &bytes))
        .unwrap();
    let seen = events_until_uploaded(&mut events, &UploadKey::Seal(1)).await;
    let mut failed = Vec::new();
    for event in seen {
        match event {
            UploadEvent::Failed {
                key: UploadKey::Dedicated { number, .. },
                ..
            } => failed.push(number),
            UploadEvent::Uploaded {
                key: UploadKey::Dedicated { number, .. },
                ..
            } => assert_eq!(number, 100),
            UploadEvent::Uploaded { .. } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(failed, [0, 1, 2, 3]);
    let exit = uploader.shutdown();
    assert_eq!(exit.unfinished.len(), 4, "their references come back");
    for unfinished in exit.unfinished {
        assert!(matches!(
            unfinished,
            UnfinishedUpload::Dedicated { records, .. } if records.len() == 1
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_id_too_long_for_a_path_fails_alone_on_the_local_file_system() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    a_stream_id_too_long_for_a_path_fails_alone(store).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_id_too_long_for_a_path_fails_alone_in_memory() {
    // The same result on every store, whatever that store would have accepted.
    a_stream_id_too_long_for_a_path_fails_alone(Arc::new(InMemory::new())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_name_too_long_for_a_path_fails_each_seal_without_retrying() {
    let (mut config, mut events) = config(Arc::new(InMemory::new()), 1);
    config.node = "n".repeat(300);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_section(1, 0, one_stream_section("a", &bytes))
        .unwrap();
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Failed {
            key: UploadKey::Seal(1),
            ..
        })
    ));
    assert_eq!(uploader.shutdown().unfinished.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_error_that_retrying_cannot_change_fails_alone() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.reject_next_puts.store(1, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_dedicated(Arc::from("rejected"), 1, vec![bytes.clone()])
        .unwrap();
    sender
        .try_send_section(1, 0, one_stream_section("healthy", &bytes))
        .unwrap();
    let seen = events_until_uploaded(&mut events, &UploadKey::Seal(1)).await;
    assert!(matches!(seen[0], UploadEvent::Failed { .. }), "{seen:?}");
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(uploader.shutdown().unfinished.len(), 1);
}

/// Sends a new seal's section until told to stop or until the uploader has stopped.
/// With `section_count = 2` each stays pending, so an accepted one has to come back.
fn send_until_stopped(
    sender: &UploaderSender<Bytes>,
    give_up: &AtomicBool,
    spins_between_sends: u32,
) -> usize {
    let mut accepted = 0;
    let mut seal = 0;
    while !give_up.load(Ordering::Relaxed) {
        let section = Section {
            extents: Vec::new(),
        };
        match sender.try_send_section(seal, 0, section) {
            Ok(()) => {
                accepted += 1;
                seal += 1;
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

#[test]
fn every_section_accepted_around_shutdown_comes_back() {
    for run in 0..500 {
        let (mut config, _events) = config(Arc::new(InMemory::new()), 2);
        config.channel_capacity = 1024;
        let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
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
    let (mut config, _events) = config(Arc::new(InMemory::new()), 2);
    config.channel_capacity = 1024;
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
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
async fn cache_touches_cannot_take_the_room_sections_need() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    counting.fail_next_puts.store(u64::MAX, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (mut config, mut events) = config(store, 1);
    config.channel_capacity = 8;
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_section(1, 0, one_stream_section("s", &bytes))
        .unwrap();
    // It is now stuck on seal 1, reading nothing, as during a long PUT or an outage.
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Retrying { .. })
    ));
    let mut touch_count = 0;
    while sender.try_cache_touch(Path::from("sealed/1/node/a.seg")) {
        touch_count += 1;
        assert!(touch_count <= 1000, "touches have no bound");
    }
    assert!(touch_count >= 1, "a touch is accepted when there is room");
    for seal in 2..10 {
        sender
            .try_send_section(seal, 0, one_stream_section("s", &bytes))
            .unwrap_or_else(|error| panic!("seal {seal} refused: {error:?}"));
    }
    assert_eq!(uploader.shutdown().unfinished.len(), 9);
}

/// A touch sent before an upload counts before that upload's eviction, so a read
/// protects what it read.
#[tokio::test(flavor = "multi_thread")]
async fn a_cache_touch_still_reaches_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    // One record's object is well under 1 KiB. Measured below, then the cap fits two.
    let probe = {
        let mut builder = storage::SegmentBuilder::new();
        let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
        builder.add_extent("s", [bytes]).unwrap();
        builder.finish().unwrap().len_bytes
    };
    let cache = SegmentCache::open(dir.path(), probe * 2).unwrap();
    let local = cache.store();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (mut config, mut events) = config(inner, 1);
    config.cache = Some(cache);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let mut paths = Vec::new();
    for number in 0..2u64 {
        // One ms apart in ids, so the objects have different paths and the same size.
        let id = FIRST_ID + (number << 16);
        let bytes = Bytes::from(record(id, 1, 7, b"x"));
        sender
            .try_send_dedicated(Arc::from("s"), number, vec![bytes])
            .unwrap();
        paths.push(next_uploaded(&mut events).await.1);
    }
    assert!(sender.try_cache_touch(paths[0].clone()));
    let bytes = Bytes::from(record(FIRST_ID + (2 << 16), 1, 7, b"x"));
    sender
        .try_send_dedicated(Arc::from("s"), 2, vec![bytes])
        .unwrap();
    next_uploaded(&mut events).await;
    assert!(local.head(&paths[0]).await.is_ok(), "the touched one stays");
    assert!(local.head(&paths[1]).await.is_err(), "the other goes");
    drop(sender);
    uploader.finish();
}

fn seal_sections_held(unfinished: &[UnfinishedUpload<Bytes>]) -> Vec<(u64, usize)> {
    let mut held: Vec<(u64, usize)> = unfinished
        .iter()
        .filter_map(|upload| match upload {
            UnfinishedUpload::Seal { seal, sections } => Some((*seal, sections.len())),
            UnfinishedUpload::Dedicated { .. } => None,
        })
        .collect();
    held.sort();
    held
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_section_for_an_uploaded_seal_does_not_put_again() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let first = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    let second = Bytes::from(record(FIRST_ID + 5, 1, 7, b"y"));
    sender
        .try_send_section(1, 0, one_stream_section("s", &first))
        .unwrap();
    assert_eq!(next_uploaded(&mut events).await.0, UploadKey::Seal(1));
    sender
        .try_send_section(1, 0, one_stream_section("s", &second))
        .unwrap();
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Failed {
            key: UploadKey::Seal(1),
            ..
        })
    ));
    drop(sender);
    let exit = uploader.finish();
    assert_eq!(counting.puts(), 1, "one PUT per seal");
    assert_eq!(seal_sections_held(&exit.unfinished), [(1, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_section_with_more_cores_fails_at_once_and_does_not_sit_pending() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 2);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    for core in 0..2 {
        sender
            .try_send_section(1, core, one_stream_section(&format!("s{core}"), &bytes))
            .unwrap();
    }
    assert_eq!(next_uploaded(&mut events).await.0, UploadKey::Seal(1));
    sender
        .try_send_section(1, 1, one_stream_section("s", &bytes))
        .unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv()).await;
    assert!(
        matches!(event, Ok(Some(UploadEvent::Failed { .. }))),
        "{event:?}"
    );
    drop(sender);
    assert_eq!(seal_sections_held(&uploader.finish().unfinished), [(1, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_core_going_back_to_an_earlier_seal_is_refused() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, mut events) = config(store, 2);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_section(5, 0, one_stream_section("s", &bytes))
        .unwrap();
    sender
        .try_send_section(4, 0, one_stream_section("s", &bytes))
        .unwrap();
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Failed {
            key: UploadKey::Seal(4),
            ..
        })
    ));
    // Another core's numbers are its own, so seal 4 from it is still accepted.
    sender
        .try_send_section(4, 1, one_stream_section("s", &bytes))
        .unwrap();
    drop(sender);
    assert_eq!(
        seal_sections_held(&uploader.finish().unfinished),
        [(4, 1), (4, 1), (5, 1)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_section_from_a_core_out_of_range_leaves_no_empty_seal_behind() {
    let (config, mut events) = config(Arc::new(InMemory::new()), 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_section(9, 5, one_stream_section("s", &bytes))
        .unwrap();
    assert!(matches!(
        events.recv().await,
        Some(UploadEvent::Failed { .. })
    ));
    drop(sender);
    assert_eq!(seal_sections_held(&uploader.finish().unfinished), [(9, 1)]);
}

/// Runs `shutdown` on its own thread, so a stuck one is a failure and not a hung test.
/// The thread is left behind on failure, and ends with the test process.
fn shutdown_within_5_s(uploader: Uploader<Bytes>) -> Vec<UnfinishedUpload<Bytes>> {
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
    let (config, _events) = config(store, 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_section(1, 0, one_stream_section("s", &bytes))
        .unwrap();
    wait_for_put_attempt(&counting).await;
    let unfinished = shutdown_within_5_s(uploader);
    assert_eq!(seal_sections_held(&unfinished), [(1, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_does_not_wait_for_a_put_stuck_on_a_blocking_thread() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let (release, gate) = std::sync::mpsc::channel();
    *counting.block_next_put_on.lock().unwrap() = Some(gate);
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (config, _events) = config(store, 1);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    sender
        .try_send_dedicated(Arc::from("s"), 7, vec![bytes])
        .unwrap();
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
    let (mut config, mut events) = config(Arc::clone(&store), SECTIONS);
    config.put_timeout_min_ms = 50;
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let (sections, _) = sections(4, FIRST_ID);
    for (core, section) in sections.into_iter().enumerate() {
        sender.try_send_section(1, core, section).unwrap();
    }
    let mut retried = false;
    let path = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            match events.recv().await.expect("the uploader stopped") {
                UploadEvent::Uploaded { path, .. } => return path,
                UploadEvent::Retrying { error, .. } => {
                    assert!(error.contains("took over"), "{error}");
                    retried = true;
                }
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
    let (config, mut events) = config(store, SECTIONS);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let (sections, _) = sections(4, FIRST_ID);
    for (core, section) in sections.into_iter().enumerate() {
        sender.try_send_section(1, core, section).unwrap();
    }
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

/// Holds back PUTs of objects whose name has `held` in it until `release` is set, and counts the
/// PUTs waiting.
#[derive(Debug)]
struct HoldsPuts {
    inner: InMemory,
    held: String,
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
        if location.as_ref().contains(&self.held) {
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

fn dedicated_records(stream: &str, id: u64) -> (Arc<str>, Vec<Bytes>) {
    (
        Arc::from(stream),
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
        held: "slow.stream".to_owned(),
        release: Arc::clone(&release),
        waiting: Arc::clone(&waiting),
    });
    let (config, mut events) = config(Arc::clone(&store), SECTIONS);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let (stream, records) = dedicated_records("slow.stream", FIRST_ID);
    sender.try_send_dedicated(stream, 1, records).unwrap();
    wait_for("the held PUT", || waiting.load(Ordering::SeqCst) == 1).await;
    let (stream, records) = dedicated_records("fast.stream", FIRST_ID + 1);
    sender.try_send_dedicated(stream, 2, records).unwrap();

    let fast = sealed_path_of(&store, "fast.stream").await;
    assert!(fast.is_some(), "the second PUT waited for the first");
    assert!(events.try_recv().is_err(), "an event passed an earlier PUT");

    release.store(true, Ordering::SeqCst);
    let numbers = [
        next_number(&mut events).await,
        next_number(&mut events).await,
    ];
    assert_eq!(numbers, [1, 2]);
    drop(sender);
    uploader.finish();
}

async fn sealed_path_of(store: &Arc<dyn ObjectStore>, stream: &str) -> Option<Path> {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let listed = storage::list_sealed_objects(store.as_ref()).await.unwrap();
            if let Some(object) = listed
                .into_iter()
                .find(|object| object.location.as_ref().contains(stream))
            {
                return object.location;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .ok()
}

async fn next_number(events: &mut UnboundedReceiver<UploadEvent>) -> u64 {
    match events.recv().await.expect("the uploader stopped") {
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
        held: "held.".to_owned(),
        release,
        waiting: Arc::clone(&waiting),
    });
    let (config, _events) = config(store, SECTIONS);
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    for number in 0..3 {
        let (stream, records) = dedicated_records(&format!("held.{number}"), FIRST_ID + number);
        sender.try_send_dedicated(stream, number, records).unwrap();
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
    let (mut config, mut events) = config(store, SECTIONS);
    config.retry_initial_ms = 25;
    config.retry_max_ms = 100;
    let (sender, uploader) = Uploader::<Bytes>::start(config).unwrap();
    let (sections, _) = sections(4, FIRST_ID);
    for (core, section) in sections.into_iter().enumerate() {
        sender.try_send_section(1, core, section).unwrap();
    }
    let mut times = Vec::new();
    loop {
        match events.recv().await.expect("the uploader stopped") {
            UploadEvent::Retrying { .. } => times.push(std::time::Instant::now()),
            UploadEvent::Uploaded { .. } => break,
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
