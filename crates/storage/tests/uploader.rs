//! The uploader: one PUT per seal, dedicated objects, and retries that lose nothing.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use common::{CountingStore, concat, record};
use format::id_ms;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use storage::{
    Section, SectionExtent, SegmentCache, SegmentReader, UnfinishedUpload, UploadEvent, UploadKey,
    UploadSendError, Uploader, UploaderConfig, dedicated_path, seal_path,
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
async fn a_retry_after_a_lost_response_counts_as_done_and_a_collision_fails() {
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
    // Different bytes for the same path must not overwrite the first object.
    let other = Bytes::from(record(FIRST_ID, 1, 7, b"y"));
    sender.try_send_section(3, 0, section(&other)).unwrap();
    assert!(matches!(
        events_one.recv().await,
        Some(UploadEvent::Failed {
            key: UploadKey::Seal(3),
            ..
        })
    ));
    drop(sender);
    assert_eq!(uploader.finish().unfinished.len(), 1);
    let reader = SegmentReader::new(store, path);
    assert_eq!(reader.read_stream("s").await.unwrap(), bytes);
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
    assert!(full_count >= 997, "{full_count}");
    let exit = uploader.shutdown();
    assert_eq!(exit.unfinished.len(), 3);
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
