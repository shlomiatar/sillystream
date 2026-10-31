//! Retention and compaction (SPEC sections 7.4, 7.5), on real files, a store that records its
//! deletes, and an injected clock. The real binary, killed at each step, is in
//! `compaction_crash.rs`.

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use common::*;
use format::{ENTRY_SEAL_MANIFEST, Records, SystemRecord, decode_manifest_doc};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use proto::ErrorCode;
use server::{
    CompactionConfig, MANIFEST_STREAM, ManualClock, Node, NodeConfig, RecoverConfig, Reply,
    RetentionClass, SEALED_EXPIRY_MS, SealOutcome, StateOpRequest, StreamConfig, Subscription,
};
use storage::{
    SegmentBuilder, SegmentReader, compacted_path, list_hour_objects, list_hours, seal_path,
};

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

struct Disk {
    dir: tempfile::TempDir,
    store: Arc<ScriptStore>,
    clock: ManualClock,
}

impl Disk {
    fn new() -> Disk {
        Disk {
            dir: tempfile::tempdir().unwrap(),
            store: Arc::new(ScriptStore::new()),
            clock: ManualClock::new(START_MS),
        }
    }

    fn metadata_dir(&self) -> PathBuf {
        self.dir.path().join("metadata")
    }

    fn start(&self, config: NodeConfig) -> Node {
        let recover = RecoverConfig {
            wal_dir: self.dir.path().join("wal"),
            metadata_dir: self.metadata_dir(),
            wal_preallocate_bytes: 1024 * 1024,
        };
        Node::recover(
            config,
            recover,
            self.store.clone(),
            Arc::new(self.clock.clone()),
        )
        .unwrap()
        .0
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    fn deleted(&self) -> Vec<Path> {
        self.store.deleted.lock().unwrap().clone()
    }

    async fn objects(&self, root: &str) -> Vec<Path> {
        let mut out = Vec::new();
        for hour in list_hours(self.store.as_ref(), root).await.unwrap() {
            for object in list_hour_objects(self.store.as_ref(), root, hour)
                .await
                .unwrap()
            {
                out.push(object.location);
            }
        }
        out.sort();
        out
    }

    /// A stream's records as these objects hold them, in id order, byte for byte.
    async fn stream_bytes(&self, root: &str, stream: &str) -> Vec<u8> {
        let mut pieces: Vec<(u64, Bytes)> = Vec::new();
        for path in self.objects(root).await {
            let reader = SegmentReader::new(self.store(), path);
            let top = reader.read_top_index().await.unwrap();
            for extent in reader.lookup(&top, stream).await.unwrap() {
                pieces.push((extent.id_first, reader.read_extent(&extent).await.unwrap()));
            }
        }
        pieces.sort_by_key(|(id_first, _)| *id_first);
        pieces.into_iter().flat_map(|(_, bytes)| bytes).collect()
    }

    /// `(stream, id_first, id_last)` of what the objects under `root` hold.
    async fn extents(&self, root: &str) -> Vec<(String, u64, u64)> {
        let mut out = Vec::new();
        for path in self.objects(root).await {
            let reader = SegmentReader::new(self.store(), path);
            let top = reader.read_top_index().await.unwrap();
            for extent in reader.all_extents(&top).await.unwrap() {
                out.push((extent.stream.to_string(), extent.id_first, extent.id_last));
            }
        }
        out.sort();
        out
    }
}

fn config(core_count: usize) -> NodeConfig {
    let mut config = manual_config(core_count);
    config.compaction = Some(CompactionConfig {
        compact_period_ms: None,
        expire_period_ms: None,
        merge_wait_ms: 0,
        ..CompactionConfig::default()
    });
    config
}

fn class_config(retention_ms: u64) -> StreamConfig {
    StreamConfig {
        retention_ms,
        ..StreamConfig::default()
    }
}

async fn publish_one(client: &mut server::Client, stream: &str, seq: u64, data: &[u8]) -> u64 {
    let records = record_with(seq, data);
    client
        .publish(publish(stream, &records, seq, 2))
        .await
        .unwrap();
    expect_ack(client).await.1
}

async fn hear_all(node: &Node, stream: &str, from: u64, count: usize) -> Heard {
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, stream, from));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= count || heard.error_code(1).is_some()
    })
    .await;
    // Anything past `count` would be a duplicate.
    hear_for(&mut connection, &mut heard, 100).await;
    heard
}

/// `(id, data)` of every record a subscriber from 0 gets.
async fn read_all(node: &Node, stream: &str, count: usize) -> Vec<(u64, Vec<u8>)> {
    let heard = hear_all(node, stream, 0, count).await;
    assert_eq!(heard.error_code(1), None);
    heard
        .records(1)
        .into_iter()
        .map(|(id, _, _, data)| (id, data))
        .collect()
}

/// The object paths in the manifest stream, in order.
async fn manifests(node: &Node) -> Vec<String> {
    let mut connection = node.connection(0).await.unwrap();
    let mut subscription = Subscription::new(1, MANIFEST_STREAM, 0);
    subscription.include_system = true;
    connection.subscribe(subscription);
    let mut heard = Heard::default();
    hear_for(&mut connection, &mut heard, 150).await;
    let mut paths = Vec::new();
    for event in &heard.events {
        if let server::Event::Data { records, .. } = event {
            let bytes = records.to_vec();
            for record in Records::new(&bytes) {
                let record = record.unwrap();
                assert_eq!(record.header.author, 0);
                assert_eq!(record.header.options, ENTRY_SEAL_MANIFEST);
                let system = SystemRecord::parse(&record).unwrap();
                paths.push(decode_manifest_doc(system.doc).unwrap().1.to_owned());
            }
        }
    }
    paths
}

async fn seal_path_of(node: &Node) -> Path {
    match node.seal().await {
        SealOutcome::Uploaded { path, .. } => path,
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_seal_publishes_one_manifest_and_the_manifest_stream_none_for_itself() {
    let disk = Disk::new();
    let node = disk.start(config(2));
    let mut client = node.client(0).await;
    let mut user_objects = Vec::new();
    for round in 0..3u64 {
        publish_one(&mut client, "user.a", round + 1, b"a").await;
        publish_one(&mut client, "user.b", round + 1, b"b").await;
        user_objects.push(seal_path_of(&node).await.to_string());
    }
    assert_eq!(manifests(&node).await, user_objects);

    // What's left is only the manifest stream's, which gets none, and then there's nothing.
    let manifest_only = seal_path_of(&node).await;
    let reader = SegmentReader::new(disk.store(), manifest_only);
    let top = reader.read_top_index().await.unwrap();
    let extents = reader.all_extents(&top).await.unwrap();
    assert!(
        extents
            .iter()
            .all(|extent| &*extent.stream == MANIFEST_STREAM),
        "{extents:?}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(matches!(node.seal().await, SealOutcome::Empty { .. }));
    assert!(matches!(node.seal().await, SealOutcome::Empty { .. }));
    assert_eq!(manifests(&node).await, user_objects);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_without_compaction_has_no_manifest_stream() {
    let disk = Disk::new();
    let node = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "user.a", 1, b"a").await;
    seal_path_of(&node).await;
    assert!(node.compact_now(&RetentionClass::ALL).await.is_none());
    assert!(node.expire_now().await.is_none());
    assert_eq!(manifests(&node).await, Vec::<String>::new());
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clients_cannot_write_the_manifest_stream_or_touch_its_cursors() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    let records = record_with(0, b"forged");
    client
        .publish(publish(MANIFEST_STREAM, &records, 1, 2))
        .await
        .unwrap();
    match expect_reply(&mut client).await {
        Reply::Nack { code, .. } => assert_eq!(code, ErrorCode::Unauthorized),
        other => panic!("{other:?}"),
    }
    client
        .state_op(StateOpRequest {
            stream: MANIFEST_STREAM,
            req: 1,
            author: AUTHOR,
            expect_ver: 0,
            op: server::StateOp::Set,
            doc: cbor(&int(1)),
        })
        .await
        .unwrap();
    match expect_reply(&mut client).await {
        Reply::Error { code, .. } => assert_eq!(code, ErrorCode::Unauthorized),
        other => panic!("{other:?}"),
    }
    assert!(!client.delete_stream(MANIFEST_STREAM).await);

    // The delete job trusts these, so nobody else may move them.
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut named = Subscription::new(1, MANIFEST_STREAM, 0);
    named.name = Some("compact:7d");
    connection.subscribe(named);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
    })
    .await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::Malformed));
    connection.reset_cursor(MANIFEST_STREAM, "compact:7d", u64::MAX - 1);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(0).is_some()
    })
    .await;
    assert_eq!(heard.error_code(0), Some(ErrorCode::Malformed));
    drop((connection, client));
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_keeps_every_record_byte_for_byte_and_deletes_nothing() {
    let disk = Disk::new();
    let node = disk.start(config(2));
    let mut client = node.client(0).await;
    let streams = ["keep.a", "keep.b", "keep.c"];
    for stream in streams {
        client.configure(stream, class_config(7 * DAY_MS)).await;
    }
    let mut expected: Vec<Vec<(u64, Vec<u8>)>> = vec![Vec::new(); 3];
    let mut seq = 0;
    for round in 0..6u64 {
        for (at, stream) in streams.iter().enumerate() {
            seq += 1;
            // Records of many items, sizes and options all travel as they are.
            let mut records = Vec::new();
            let data = vec![at as u8 + round as u8; 100 + (seq as usize * 37) % 900];
            add_record(&mut records, seq, &data, 1 + (seq % 5) as u16);
            add_record(&mut records, seq + 1, b"second", 1);
            client
                .publish(publish(stream, &records, seq, 2))
                .await
                .unwrap();
            let (_, first_id, count) = expect_ack(&mut client).await;
            assert_eq!(count as u64, 1 + (seq % 5) + 1);
            expected[at].push((first_id, data));
            expected[at].push((first_id + u64::from(count) - 1, b"second".to_vec()));
        }
        seal_path_of(&node).await;
    }
    // A move partway makes a stream's extents come from two cores.
    let shard = server::shard_of("keep.a", node.virtual_shard_count());
    assert!(node.move_shard(shard, 1).await);
    let last = publish_one(&mut client, "keep.a", 1000, b"after the move").await;
    expected[0].push((last, b"after the move".to_vec()));
    seal_path_of(&node).await;

    let mut source_bytes = Vec::new();
    for stream in streams {
        source_bytes.push(disk.stream_bytes("sealed", stream).await);
    }
    let sources_before = disk.objects("sealed").await;

    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(
        report.outputs.len(),
        1,
        "one output for one hour: {report:?}"
    );
    assert!(report.outputs[0].as_ref().starts_with("7d/"));

    for (at, stream) in streams.iter().enumerate() {
        // Every byte of every record, in the same order.
        assert_eq!(
            disk.stream_bytes("7d", stream).await,
            source_bytes[at],
            "{stream}"
        );
        let got = read_all(&node, stream, expected[at].len()).await;
        assert_eq!(got, expected[at], "{stream}");
    }
    // Neighbours merged: a stream that sealed six times is one extent now.
    let compacted = disk.extents("7d").await;
    assert_eq!(
        compacted.iter().filter(|e| e.0 == "keep.b").count(),
        1,
        "{compacted:?}"
    );
    // The source is untouched, and the manifest stream's objects were never copied.
    assert!(disk.deleted().is_empty());
    let sources_after = disk.objects("sealed").await;
    assert!(
        sources_before
            .iter()
            .all(|path| sources_after.contains(path))
    );
    assert!(compacted.iter().all(|e| e.0 != MANIFEST_STREAM));

    // Nothing left to do, and no second copy for doing it again.
    let again = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(again.outputs, Vec::<Path>::new());
    drop(client);
    node.shutdown();

    // Readers still follow the new extents after a restart.
    let node = disk.start(config(2));
    for (at, stream) in streams.iter().enumerate() {
        assert_eq!(
            read_all(&node, stream, expected[at].len()).await,
            expected[at]
        );
    }
    node.shutdown();
}

/// Publishes as a read goes on, so each of a read's steps may meet the old or the new extents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_during_compaction_sees_no_gap_or_duplicate() {
    let disk = Disk::new();
    let mut settings = config(2);
    settings.connection_window_bytes = 16 * 1024;
    settings.catch_up_read_bytes = 8 * 1024;
    let node = disk.start(settings);
    let mut client = node.client(0).await;
    client.configure("during", class_config(7 * DAY_MS)).await;
    let mut ids = Vec::new();
    let data = vec![7u8; 4096];
    for round in 0..8u64 {
        for at in 0..5u64 {
            ids.push(publish_one(&mut client, "during", round * 5 + at + 1, &data).await);
        }
        seal_path_of(&node).await;
    }
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, "during", 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= 5
    })
    .await;
    // Compacts while the reader takes nothing, then reads on, publishing all the while.
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert!(report.replaced_count > 0);
    for at in 0..5u64 {
        ids.push(publish_one(&mut client, "during", 1000 + at, &data).await);
    }
    seal_path_of(&node).await;
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= ids.len()
    })
    .await;
    hear_for(&mut connection, &mut heard, 100).await;
    let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
    assert_eq!(got, ids);
    drop((connection, client));
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reader_holding_a_deleted_extent_recovers_through_the_compacted_one() {
    let disk = Disk::new();
    let mut settings = config(1);
    settings.connection_window_bytes = 16 * 1024;
    settings.catch_up_read_bytes = 4 * 1024;
    let node = disk.start(settings);
    let mut client = node.client(0).await;
    client.configure("stale", class_config(7 * DAY_MS)).await;
    let mut ids = Vec::new();
    let data = vec![9u8; 8 * 1024];
    // Past a block's size, so a read stops partway through an extent.
    for at in 0..40u64 {
        ids.push(publish_one(&mut client, "stale", at + 1, &data).await);
    }
    seal_path_of(&node).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, "stale", 0));
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty()
    })
    .await;
    let taken = heard.records(1).len();
    assert!(taken < ids.len(), "the read should still be going");

    // Its object goes while the reader isn't reading.
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let expired = node.expire_now().await.unwrap();
    assert_eq!(expired.errors, Vec::<String>::new());
    assert!(!expired.deleted_prefixes.is_empty(), "{expired:?}");

    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= ids.len() || heard.error_code(1).is_some()
    })
    .await;
    hear_for(&mut connection, &mut heard, 100).await;
    assert_eq!(heard.error_code(1), None);
    let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
    assert_eq!(got, ids);
    drop((connection, client));
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reader_of_an_extent_with_no_object_gets_error_7_and_the_stream_moves_on() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "lost", 1, b"old").await;
    let lost = seal_path_of(&node).await;
    let kept = publish_one(&mut client, "lost", 2, b"new").await;
    seal_path_of(&node).await;
    // By hand: nothing in the system does this.
    disk.store.as_ref().delete(&lost).await.unwrap();
    let heard = hear_all(&node, "lost", 0, 2).await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::RetentionExpired));
    // That extent is gone now, and a new read starts at what's left.
    let got = read_all(&node, "lost", 1).await;
    assert_eq!(got, vec![(kept, b"new".to_vec())]);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_1d_class_is_never_compacted_and_each_longer_class_has_its_own_prefix() {
    let disk = Disk::new();
    let node = disk.start(config(2));
    let mut client = node.client(0).await;
    let classes = [
        ("class.1d", DAY_MS, None),
        ("class.7d", 7 * DAY_MS, Some("7d")),
        ("class.30d", 30 * DAY_MS, Some("30d")),
        ("class.365d", 365 * DAY_MS, Some("365d")),
    ];
    let mut expected = Vec::new();
    for (seq, (stream, retention_ms, _)) in classes.iter().enumerate() {
        client.configure(stream, class_config(*retention_ms)).await;
        let id = publish_one(&mut client, stream, seq as u64 + 1, b"x").await;
        expected.push((*stream, id));
    }
    seal_path_of(&node).await;
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(report.outputs.len(), 3, "{report:?}");

    let everything = disk.store.as_ref().list_with_delimiter(None).await.unwrap();
    let roots: BTreeSet<String> = everything
        .common_prefixes
        .iter()
        .map(|prefix| prefix.to_string())
        .collect();
    assert!(!roots.contains("1d"), "{roots:?}");
    for (stream, _, prefix) in classes {
        match prefix {
            Some(prefix) => {
                let held = disk.extents(prefix).await;
                assert_eq!(held.len(), 1, "{prefix}: {held:?}");
                assert_eq!(held[0].0, stream);
            }
            None => {
                for root in ["7d", "30d", "365d"] {
                    assert!(disk.extents(root).await.iter().all(|e| e.0 != stream));
                }
            }
        }
    }
    for (stream, id) in expected {
        assert_eq!(read_all(&node, stream, 1).await, vec![(id, b"x".to_vec())]);
    }
    drop(client);
    node.shutdown();
}

/// A class change counts from when compaction gets there: what was copied stays where it is
/// (D31), and what wasn't follows the new class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_class_applies_to_what_is_compacted_from_then_on() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("moving", class_config(7 * DAY_MS)).await;
    let first = publish_one(&mut client, "moving", 1, b"a").await;
    seal_path_of(&node).await;
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    client.configure("moving", class_config(30 * DAY_MS)).await;
    let second = publish_one(&mut client, "moving", 2, b"b").await;
    seal_path_of(&node).await;
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(disk.extents("7d").await[0].1, first);
    assert_eq!(disk.extents("30d").await[0].1, second);
    assert_eq!(read_all(&node, "moving", 2).await.len(), 2);
    drop(client);
    node.shutdown();
}

/// Extents reached a checkpoint and the cursor didn't, which is the gap a crash can leave. The
/// second run finds nothing to do, and copies nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_that_lost_the_cursor_redoes_nothing_and_loses_nothing() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("again", class_config(7 * DAY_MS)).await;
    let mut ids = Vec::new();
    for at in 0..3u64 {
        ids.push(publish_one(&mut client, "again", at + 1, b"x").await);
        seal_path_of(&node).await;
    }
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    let outputs = disk.objects("7d").await;
    assert_eq!(outputs.len(), 1);
    // The cursor moved after the checkpoint, and nothing has saved it since.
    drop(client);
    node.shutdown();

    let node = disk.start(config(1));
    let connection = node.connection(0).await.unwrap();
    assert_eq!(
        connection.read_cursor(MANIFEST_STREAM, "compact:7d").await,
        None,
        "this test is about a cursor that wasn't saved"
    );
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(report.outputs, Vec::<Path>::new());
    assert_eq!(disk.objects("7d").await, outputs);
    let got: Vec<u64> = read_all(&node, "again", 3)
        .await
        .iter()
        .map(|record| record.0)
        .collect();
    assert_eq!(got, ids);
    drop(connection);
    node.shutdown();
}

/// A crash mid-merge leaves an output nobody refers to, here whole and truncated. Neither is
/// ever read, and the real run takes the next free name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_output_from_a_crash_is_never_readable() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("partial", class_config(7 * DAY_MS)).await;
    let mut ids = Vec::new();
    for at in 0..3u64 {
        ids.push(publish_one(&mut client, "partial", at + 1, b"x").await);
        seal_path_of(&node).await;
    }
    drop(client);
    node.shutdown();

    // What a compaction would have written, with the same records, at the name the real run
    // will want, and a torn one beside it.
    let source = disk.stream_bytes("sealed", "partial").await;
    let mut builder = SegmentBuilder::new();
    builder
        .add_extent("partial", vec![Bytes::from(source.clone())])
        .unwrap();
    let built = builder.finish().unwrap();
    let whole: Vec<u8> = built.parts.iter().flatten().copied().collect();
    let hour = format::id_ms(ids[0]) / HOUR_MS;
    let at = compacted_path("7d", hour, "node", 2, ids[0]);
    disk.store
        .put(&at, PutPayload::from(whole[..whole.len() / 2].to_vec()))
        .await
        .unwrap();
    let orphan = compacted_path("7d", hour, "node", 1, ids[0] + 100);
    disk.store
        .put(&orphan, PutPayload::from(whole))
        .await
        .unwrap();

    let node = disk.start(config(1));
    let check = |records: Vec<(u64, Vec<u8>)>| {
        let got: Vec<u64> = records.iter().map(|record| record.0).collect();
        assert_eq!(got, ids);
    };
    check(read_all(&node, "partial", 3).await);
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(report.replaced_count, 1);
    assert_ne!(report.outputs[0], at, "that name is taken by other bytes");
    assert_ne!(report.outputs[0], orphan);
    check(read_all(&node, "partial", 3).await);
    node.shutdown();
    let node = disk.start(config(1));
    check(read_all(&node, "partial", 3).await);
    node.shutdown();
}

fn stamped(id: u64, data: &[u8]) -> Bytes {
    let mut out = Vec::new();
    format::encode_record(
        &mut out,
        &format::RecordHeader {
            id,
            options: 0,
            author: 7,
            len: data.len() as u32,
            count: 1,
            codec: 0,
        },
        data,
    )
    .unwrap();
    out.into()
}

/// A former owner's late object: compaction copies what readers accept and no more (D17). Its
/// manifest comes from the startup that found it, which is also what makes up for a manifest
/// a crash lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_former_generations_records_above_its_limit_are_dropped_by_compaction() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "anchor", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();
    let node = disk.start(config(1));
    node.shutdown();
    let limit = storage::read_ownership(disk.store.as_ref())
        .await
        .unwrap()
        .limit_for(1)
        .unwrap();

    let mut builder = SegmentBuilder::new();
    let below = stamped(limit - 1, b"below");
    let at = stamped(limit, b"at");
    builder
        .add_extent(
            "late",
            vec![below.clone(), at.clone(), stamped(limit + 1, b"above")],
        )
        .unwrap();
    let body: Vec<u8> = builder
        .finish()
        .unwrap()
        .parts
        .iter()
        .flatten()
        .copied()
        .collect();
    let late_path = seal_path("former", 1, limit - 1);
    disk.store
        .put(&late_path, PutPayload::from(body))
        .await
        .unwrap();

    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("late", class_config(7 * DAY_MS)).await;
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert!(manifests(&node).await.contains(&late_path.to_string()));
    let mut want = below.to_vec();
    want.extend_from_slice(&at);
    assert_eq!(disk.stream_bytes("7d", "late").await, want);
    assert_eq!(
        read_all(&node, "late", 2).await,
        vec![(limit - 1, b"below".to_vec()), (limit, b"at".to_vec())]
    );
    drop(client);
    node.shutdown();
}

/// Records of a stream in each of `hours`, each hour in its own objects.
async fn publish_over_hours(
    disk: &Disk,
    node: &Node,
    client: &mut server::Client,
    stream: &str,
    hours: u64,
) -> Vec<u64> {
    let mut ids = Vec::new();
    for hour in 0..hours {
        for seq in 0..2 {
            ids.push(publish_one(client, stream, ids.len() as u64 + 1, &[seq]).await);
            seal_path_of(node).await;
        }
        if hour + 1 < hours {
            disk.clock.advance(HOUR_MS);
        }
    }
    ids
}

/// Only whole hours past their expiry, every object of them, and nothing else in the bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_whole_expired_hour_prefixes_are_ever_deleted() {
    let disk = Disk::new();
    let node = disk.start(config(2));
    let mut client = node.client(0).await;
    client.configure("week", class_config(7 * DAY_MS)).await;
    publish_over_hours(&disk, &node, &mut client, "week", 2).await;
    publish_over_hours(&disk, &node, &mut client, "day", 3).await;
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());

    let start_hour = START_MS / HOUR_MS;
    // Where `publish_over_hours` left the clock.
    let base_ms = START_MS + 3 * HOUR_MS;
    let mut elapsed_ms = 0;
    let mut seen_deleted = 0;
    // Walks past every expiry there is, an hour at a time, checking each run's deletes.
    for _ in 0..(8 * 24 + 8) {
        disk.clock.advance(HOUR_MS);
        elapsed_ms += HOUR_MS;
        let now_ms = base_ms + elapsed_ms;
        let mut before = Vec::new();
        for root in ["sealed", "7d", "30d", "365d"] {
            before.extend(disk.objects(root).await);
        }
        let report = node.expire_now().await.unwrap();
        assert_eq!(report.errors, Vec::<String>::new());
        assert_eq!(report.blocked_prefixes, Vec::<String>::new());
        let deleted = disk.deleted();
        let new = &deleted[seen_deleted..];
        seen_deleted = deleted.len();
        let mut new_prefixes = BTreeSet::new();
        for path in new {
            let parts: Vec<String> = path.parts().map(|p| p.as_ref().to_owned()).collect();
            let hour: u64 = parts[1].parse().unwrap();
            let expiry_ms = match parts[0].as_str() {
                "sealed" => SEALED_EXPIRY_MS,
                "7d" => 7 * DAY_MS + server::CLASS_GRACE_MS,
                other => panic!("deleted under {other}: {path}"),
            };
            assert!(
                now_ms >= (hour + 1) * HOUR_MS + expiry_ms,
                "{path} deleted early"
            );
            assert!(hour >= start_hour);
            new_prefixes.insert((parts[0].clone(), hour));
        }
        // Whole: what the prefix held before is all gone, and every delete was of something
        // in it.
        for (root, hour) in &new_prefixes {
            let prefix = format!("{root}/{hour}/");
            let had: BTreeSet<&Path> = before
                .iter()
                .filter(|path| path.as_ref().starts_with(&prefix))
                .collect();
            let went: BTreeSet<&Path> = new
                .iter()
                .filter(|path| path.as_ref().starts_with(&prefix))
                .collect();
            assert_eq!(had, went, "{prefix}");
            assert!(
                list_hour_objects(disk.store.as_ref(), root, *hour)
                    .await
                    .unwrap()
                    .is_empty(),
                "{prefix} still holds something"
            );
        }
        assert_eq!(
            report.deleted_prefixes.len(),
            new_prefixes.len(),
            "{report:?}"
        );
    }
    let deleted = disk.deleted();
    assert!(
        deleted
            .iter()
            .any(|path| path.as_ref().starts_with("sealed/"))
    );
    assert!(deleted.iter().any(|path| path.as_ref().starts_with("7d/")));
    // Ownership and checkpoints outlive records.
    assert!(deleted.iter().all(|path| {
        !path.as_ref().starts_with("ownership") && !path.as_ref().starts_with("checkpoints")
    }));
    drop(client);
    node.shutdown();
}

/// The key test of section 7.4.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_compactor_blocks_sealed_expiry_until_it_catches_up() {
    let disk = Disk::new();
    let node = disk.start(config(2));
    let mut client = node.client(0).await;
    client.configure("week", class_config(7 * DAY_MS)).await;
    client.configure("month", class_config(30 * DAY_MS)).await;
    let week = publish_one(&mut client, "week", 1, b"w").await;
    let month = publish_one(&mut client, "month", 1, b"m").await;
    let day = publish_one(&mut client, "day", 1, b"d").await;
    seal_path_of(&node).await;
    // So the manifests are read, and the cursors exist for the workers that do run.
    assert!(node.seal_and_checkpoint().await);
    let sealed = disk.objects("sealed").await;
    assert!(!sealed.is_empty());

    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    // Nothing has run at all.
    let report = node.expire_now().await.unwrap();
    assert!(!report.blocked_prefixes.is_empty(), "{report:?}");
    assert_eq!(report.deleted_prefixes, Vec::<String>::new());
    assert!(disk.deleted().is_empty());

    // One class has caught up and another hasn't: still none of it can go, or `month` is lost.
    node.compact_now(&[RetentionClass::Days7, RetentionClass::Days365])
        .await
        .unwrap();
    let report = node.expire_now().await.unwrap();
    assert!(!report.blocked_prefixes.is_empty(), "{report:?}");
    assert!(disk.deleted().is_empty());
    assert_eq!(disk.objects("sealed").await, sealed);
    assert_eq!(read_all(&node, "month", 1).await[0].0, month);

    // Stuck at the store itself, as a real stall would be, it stays blocked.
    disk.store.hold();
    {
        let stuck = node.compact_now(&[RetentionClass::Days30]);
        tokio::pin!(stuck);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut stuck)
                .await
                .is_err(),
            "the store is holding its PUTs"
        );
        disk.store.release();
        let report = stuck.await.unwrap();
        assert_eq!(report.errors, Vec::<String>::new());
    }

    // Caught up now.
    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(report.blocked_prefixes, Vec::<String>::new());
    assert!(!report.deleted_prefixes.is_empty());
    for path in sealed.iter().filter(|p| p.as_ref().contains("/node/")) {
        assert!(disk.deleted().contains(path), "{path}");
    }
    // What was copied is still there, from its own prefix, and `1d` is gone.
    assert_eq!(read_all(&node, "week", 1).await[0].0, week);
    assert_eq!(read_all(&node, "month", 1).await[0].0, month);
    let heard = hear_all(&node, "day", 0, 0).await;
    assert_eq!(heard.error_code(1), None);
    assert!(heard.records(1).is_empty());
    assert!(day > 0);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_replaced_by_a_later_generation_deletes_nothing() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "day", 1, b"d").await;
    seal_path_of(&node).await;
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let ownership = storage::read_ownership(disk.store.as_ref()).await.unwrap();
    storage::claim_generation(disk.store.as_ref(), &ownership, 1)
        .await
        .unwrap();
    let report = node.expire_now().await.unwrap();
    assert!(report.not_owner);
    assert!(disk.deleted().is_empty());
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_records_expire_state_cursors_ids_and_config_remain() {
    let disk = Disk::new();
    let settings = || {
        let mut settings = config(2);
        settings.full_checkpoint_every_count = 8;
        settings
    };
    let node = disk.start(settings());
    let mut client = node.client(0).await;
    let stream_config = StreamConfig {
        retention_ms: 7 * DAY_MS,
        default_ack_level: 2,
        min_ack_level: 1,
    };
    client.configure("life", stream_config).await;
    let mut old_ids = Vec::new();
    for at in 0..3u64 {
        old_ids.push(publish_one(&mut client, "life", at + 1, b"old").await);
        seal_path_of(&node).await;
    }
    client
        .state_op(StateOpRequest {
            stream: "life",
            req: 1,
            author: AUTHOR,
            expect_ver: 0,
            op: server::StateOp::Set,
            doc: cbor(&map(&[("kept", int(5))])),
        })
        .await
        .unwrap();
    let state_ver = match expect_reply(&mut client).await {
        Reply::StateResult { ver, ok: true, .. } => ver,
        other => panic!("{other:?}"),
    };
    let mut cursor_connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut named = Subscription::new(1, "life", 0);
    named.name = Some("worker");
    cursor_connection.subscribe(named);
    hear_until(&mut cursor_connection, &mut heard, |heard| {
        heard.records(1).len() >= 3
    })
    .await;
    cursor_connection.commit(1, old_ids[2]);
    assert_eq!(
        cursor_connection.read_cursor("life", "worker").await,
        Some(old_ids[2])
    );
    drop(cursor_connection);
    seal_path_of(&node).await;
    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());

    // Past the class's whole length, with a new record in a later hour.
    disk.clock.advance(7 * DAY_MS - 2 * HOUR_MS);
    let new_id = publish_one(&mut client, "life", 100, b"new").await;
    seal_path_of(&node).await;
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    disk.clock
        .advance(2 * HOUR_MS + server::CLASS_GRACE_MS + HOUR_MS);
    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert!(
        report.deleted_prefixes.iter().any(|p| p.starts_with("7d/")),
        "{report:?}"
    );

    let got = read_all(&node, "life", 1).await;
    assert_eq!(got, vec![(new_id, b"new".to_vec())], "from = 0");
    let snapshot = client.snapshot("life").await.unwrap();
    assert_eq!(snapshot.state_ver, state_ver);
    assert_eq!(from_cbor(&snapshot.state), map(&[("kept", int(5))]));
    let info = client.stream_info("life").await.unwrap();
    assert_eq!(info.config, stream_config);
    assert!(info.head >= new_id);
    let connection = node.connection(0).await.unwrap();
    assert_eq!(
        connection.read_cursor("life", "worker").await,
        Some(old_ids[2])
    );
    drop(connection);
    let next = publish_one(&mut client, "life", 101, b"next").await;
    assert!(next > new_id);
    // Saves the cursor and the rest, as a seal does.
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    // All of it comes back, from the local store and then from checkpoints alone.
    for rebuild in [false, true] {
        if rebuild {
            std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
        }
        let node = disk.start(settings());
        let mut client = node.client(0).await;
        let got = read_all(&node, "life", 2).await;
        assert_eq!(got[0], (new_id, b"new".to_vec()), "rebuild {rebuild}");
        assert_eq!(got[1], (next, b"next".to_vec()));
        let snapshot = client.snapshot("life").await.unwrap();
        assert_eq!(snapshot.state_ver, state_ver);
        let info = client.stream_info("life").await.unwrap();
        assert_eq!(info.config, stream_config);
        let connection = node.connection(0).await.unwrap();
        assert_eq!(
            connection.read_cursor("life", "worker").await,
            Some(old_ids[2])
        );
        assert!(publish_one(&mut client, "life", 200 + u64::from(rebuild), b"z").await > next);
        drop((connection, client));
        node.shutdown();
    }
}

/// The list startups use to know which objects to look in stops growing with what has gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_checkpoint_forgets_objects_that_have_expired() {
    let disk = Disk::new();
    let mut settings = config(1);
    settings.full_checkpoint_every_count = 3;
    let node = disk.start(settings.clone());
    let mut client = node.client(0).await;
    publish_one(&mut client, "day", 1, b"d").await;
    seal_path_of(&node).await;
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let report = node.expire_now().await.unwrap();
    assert!(!report.deleted_prefixes.is_empty(), "{report:?}");
    for at in 0..4u64 {
        publish_one(&mut client, "fresh", at + 1, b"f").await;
        assert!(node.seal_and_checkpoint().await);
    }
    drop(client);
    node.shutdown();
    let store: Arc<dyn ObjectStore> = disk.store();
    let ownership = storage::read_ownership(disk.store.as_ref()).await.unwrap();
    let set = storage::CheckpointSet::read(&store, ownership)
        .await
        .unwrap();
    let gone: Vec<String> = disk.deleted().iter().map(Path::to_string).collect();
    assert!(!gone.is_empty());
    assert!(
        set.recorded_objects()
            .all(|object| !gone.iter().any(|g| g == object))
    );
    assert!(set.recorded_objects().next().is_some());
}

/// After long downtime, restored records go up in the first seal with records from just after
/// the restart. Those are fresh, so the object mustn't count as old.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seal_mixing_restored_and_fresh_records_expires_by_its_newest() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "old", 1, b"old").await;
    drop(client);
    node.shutdown();

    disk.clock.advance(3 * DAY_MS);
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    let fresh = publish_one(&mut client, "fresh", 1, b"just now").await;
    seal_path_of(&node).await;
    assert!(node.seal_and_checkpoint().await);
    node.compact_now(&RetentionClass::ALL).await.unwrap();

    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(report.deleted_prefixes, Vec::<String>::new());
    assert_eq!(
        read_all(&node, "fresh", 1).await,
        vec![(fresh, b"just now".to_vec())]
    );

    // It goes when the fresh record's own time is up.
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    assert!(node.seal_and_checkpoint().await);
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert!(!report.deleted_prefixes.is_empty(), "{report:?}");
    drop(client);
    node.shutdown();
}

/// The same for a compacted class: the output's prefix is its sources' hour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compacted_mixed_seal_keeps_its_class_from_the_newest_record() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("old", class_config(7 * DAY_MS)).await;
    client.configure("fresh", class_config(7 * DAY_MS)).await;
    assert!(node.seal_and_checkpoint().await);
    publish_one(&mut client, "old", 1, b"old").await;
    drop(client);
    node.shutdown();

    disk.clock.advance(8 * DAY_MS);
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    let fresh = publish_one(&mut client, "fresh", 1, b"just now").await;
    seal_path_of(&node).await;
    assert!(node.seal_and_checkpoint().await);
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert!(!disk.objects("7d").await.is_empty());

    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert_eq!(report.deleted_prefixes, Vec::<String>::new());
    assert_eq!(
        read_all(&node, "fresh", 1).await,
        vec![(fresh, b"just now".to_vec())]
    );
    drop(client);
    node.shutdown();
}

/// Workers are at different manifests, so a class raised between them is seen by neither as
/// its own. The delete job finds the stream still listing the object and copies it first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_class_raised_between_workers_still_gets_its_records_copied() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("grow", class_config(7 * DAY_MS)).await;
    let id = publish_one(&mut client, "grow", 1, b"keep me").await;
    let source = seal_path_of(&node).await;
    assert!(node.seal_and_checkpoint().await);

    node.compact_now(&[RetentionClass::Days365, RetentionClass::Days30])
        .await
        .unwrap();
    client.configure("grow", class_config(365 * DAY_MS)).await;
    node.compact_now(&[RetentionClass::Days7]).await.unwrap();
    assert!(disk.objects("7d").await.is_empty());
    assert!(disk.objects("365d").await.is_empty());
    assert!(node.seal_and_checkpoint().await);

    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert!(
        !report
            .deleted_prefixes
            .contains(&"sealed/500000".to_owned())
    );
    assert_eq!(disk.extents("365d").await.len(), 1, "{report:?}");
    assert_eq!(
        read_all(&node, "grow", 1).await,
        vec![(id, b"keep me".to_vec())]
    );

    // With its copy made, the hour can go, and the record stays.
    let report = node.expire_now().await.unwrap();
    assert_eq!(report.errors, Vec::<String>::new());
    assert!(disk.deleted().contains(&source), "{report:?}");
    assert_eq!(
        read_all(&node, "grow", 1).await,
        vec![(id, b"keep me".to_vec())]
    );
    drop(client);
    node.shutdown();
}

/// The object of a stream with nothing to replace it goes while a reader is partway through.
async fn expire_under_a_paused_reader(publish_later_first: bool) {
    let disk = Disk::new();
    let mut settings = config(1);
    settings.connection_window_bytes = 16 * 1024;
    settings.catch_up_read_bytes = 4 * 1024;
    let node = disk.start(settings);
    let mut client = node.client(0).await;
    let data = vec![9u8; 8 * 1024];
    for at in 0..40u64 {
        publish_one(&mut client, "gap", at + 1, &data).await;
    }
    seal_path_of(&node).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, "gap", 0));
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty()
    })
    .await;
    let taken = heard.records(1).len();
    assert!(taken < 40, "the read should still be going");

    node.compact_now(&RetentionClass::ALL).await.unwrap();
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    if publish_later_first {
        publish_one(&mut client, "gap", 100, b"later").await;
        seal_path_of(&node).await;
    }
    let expired = node.expire_now().await.unwrap();
    assert!(!expired.deleted_prefixes.is_empty(), "{expired:?}");
    if !publish_later_first {
        publish_one(&mut client, "gap", 100, b"later").await;
        seal_path_of(&node).await;
    }

    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
            || heard
                .records(1)
                .iter()
                .any(|record| record.3 == b"later".to_vec())
    })
    .await;
    assert_eq!(
        heard.error_code(1),
        Some(ErrorCode::RetentionExpired),
        "taken {taken}, heard {}",
        heard.records(1).len()
    );
    drop((connection, client));
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_reader_whose_object_expired_gets_error_7() {
    expire_under_a_paused_reader(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reader_overtaken_by_the_delete_job_gets_error_7() {
    expire_under_a_paused_reader(true).await;
}

/// Records `a`, `b` expire, and `c` comes after them. `(a, b, c)`.
async fn expired_then_one_more(disk: &Disk, node: &Node) -> (u64, u64, u64) {
    let mut client = node.client(0).await;
    let a = publish_one(&mut client, "gone", 11, b"a").await;
    let b = publish_one(&mut client, "gone", 12, b"b").await;
    seal_path_of(node).await;
    node.compact_now(&RetentionClass::ALL).await.unwrap();
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let report = node.expire_now().await.unwrap();
    assert!(!report.deleted_prefixes.is_empty(), "{report:?}");
    let c = publish_one(&mut client, "gone", 13, b"c").await;
    seal_path_of(node).await;
    (a, b, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_position_with_expired_records_after_it_gets_error_7() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let (a, b, c) = expired_then_one_more(&disk, &node).await;

    let heard = hear_all(&node, "gone", a - 1, 1).await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::RetentionExpired));
    let heard = hear_all(&node, "gone", a, 1).await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::RetentionExpired));

    // Having read all that expired misses nothing, and neither does starting at retention.
    let heard = hear_all(&node, "gone", b, 1).await;
    assert_eq!(heard.error_code(1), None);
    assert_eq!(heard.records(1)[0].0, c);
    assert_eq!(read_all(&node, "gone", 1).await[0].0, c);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_cursor_with_expired_records_after_it_gets_error_7() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    let first = publish_one(&mut client, "gone", 1, b"first").await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut named = Subscription::new(1, "gone", 0);
    named.name = Some("worker");
    connection.subscribe(named);
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty()
    })
    .await;
    connection.commit(1, first);
    assert_eq!(connection.read_cursor("gone", "worker").await, Some(first));
    drop(connection);
    expired_then_one_more(&disk, &node).await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut named = Subscription::new(2, "gone", 0);
    named.name = Some("worker");
    connection.subscribe(named);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(2).is_some() || !heard.records(2).is_empty()
    })
    .await;
    assert_eq!(heard.error_code(2), Some(ErrorCode::RetentionExpired));
    drop((connection, client));
    node.shutdown();
}

/// **Q32, not fixed: this pins the limit and changes with the decision.** What expired is known
/// only while the stream is in memory, so after a restart this cursor is served with a gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn q32_after_a_restart_a_position_before_expired_records_still_resumes_silently() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let (a, _, c) = expired_then_one_more(&disk, &node).await;
    assert!(node.seal_and_checkpoint().await);
    node.shutdown();

    let node = disk.start(config(1));
    let heard = hear_all(&node, "gone", a, 1).await;
    assert_eq!(heard.error_code(1), None);
    assert_eq!(heard.records(1)[0].0, c);
    node.shutdown();
}

/// One flipped byte in a footer stops nothing else. Its own hour stays, since it can't be
/// shown safe to delete, and what the owner lists in it can still be read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_footer_holds_only_its_own_hour() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("mine", class_config(7 * DAY_MS)).await;
    client.configure("other", class_config(7 * DAY_MS)).await;
    assert!(node.seal_and_checkpoint().await);
    let bad_id = publish_one(&mut client, "mine", 1, b"bad").await;
    let bad = seal_path_of(&node).await;
    let good_id = publish_one(&mut client, "other", 1, b"good").await;
    seal_path_of(&node).await;
    assert!(node.seal_and_checkpoint().await);

    let bytes = disk.store.get(&bad).await.unwrap().bytes().await.unwrap();
    let mut flipped = bytes.to_vec();
    let at = flipped.len() - 3;
    flipped[at] ^= 0xFF;
    disk.store
        .put(&bad, PutPayload::from(flipped))
        .await
        .unwrap();

    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert!(!report.errors.is_empty(), "{report:?}");
    assert!(
        report.cursors.iter().all(|(_, cursor)| *cursor > 0),
        "{report:?}"
    );
    assert_eq!(disk.extents("7d").await[0].1, good_id, "{report:?}");
    assert_eq!(read_all(&node, "other", 1).await[0].0, good_id);
    assert_eq!(read_all(&node, "mine", 1).await[0].0, bad_id);

    // The record in that object was never copied, so it isn't deleted.
    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let report = node.expire_now().await.unwrap();
    assert!(!disk.deleted().contains(&bad), "{report:?}");
    assert!(
        report
            .blocked_prefixes
            .contains(&"sealed/500000".to_owned())
    );
    assert_eq!(read_all(&node, "mine", 1).await[0].0, bad_id);
    drop(client);
    node.shutdown();
}

/// A corrupt record range doesn't stop a worker either, and stays where it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_records_hold_only_their_own_hour() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("mine", class_config(7 * DAY_MS)).await;
    client.configure("other", class_config(7 * DAY_MS)).await;
    assert!(node.seal_and_checkpoint().await);
    publish_one(&mut client, "mine", 1, b"bad").await;
    let bad = seal_path_of(&node).await;
    let good_id = publish_one(&mut client, "other", 1, b"good").await;
    seal_path_of(&node).await;
    assert!(node.seal_and_checkpoint().await);

    let bytes = disk.store.get(&bad).await.unwrap().bytes().await.unwrap();
    let mut flipped = bytes.to_vec();
    flipped[40] ^= 0xFF;
    disk.store
        .put(&bad, PutPayload::from(flipped))
        .await
        .unwrap();

    let report = node.compact_now(&RetentionClass::ALL).await.unwrap();
    assert!(!report.errors.is_empty(), "{report:?}");
    assert_eq!(disk.extents("7d").await[0].1, good_id, "{report:?}");
    assert!(
        report.cursors.iter().all(|(_, cursor)| *cursor > 0),
        "{report:?}"
    );

    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    let report = node.expire_now().await.unwrap();
    assert!(!disk.deleted().contains(&bad), "{report:?}");
    assert!(
        report
            .blocked_prefixes
            .contains(&"sealed/500000".to_owned())
    );
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stray_huge_hour_prefix_is_left_alone_and_stops_nothing() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let stray =
        Path::from("sealed/18446744073709551615/n/00000000000000000001-00000000000000000001.seg");
    disk.store
        .put(&stray, PutPayload::from_static(b"stray"))
        .await
        .unwrap();
    for _ in 0..2 {
        let report = node.expire_now().await.expect("the compactor is running");
        assert_eq!(report.deleted_prefixes, Vec::<String>::new());
    }
    assert!(disk.store.head(&stray).await.is_ok());
    assert!(node.compact_now(&RetentionClass::ALL).await.is_some());
    node.shutdown();
}

/// At expiry the class now decides, whichever way it moved after the workers went past (D176).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_delete_job_copies_for_a_raised_class_and_not_for_a_lowered_one() {
    let disk = Disk::new();
    let node = disk.start(config(1));
    let mut client = node.client(0).await;
    client.configure("lowered", class_config(30 * DAY_MS)).await;
    let raised = publish_one(&mut client, "raised", 1, b"up").await;
    publish_one(&mut client, "lowered", 1, b"down").await;
    seal_path_of(&node).await;
    assert!(node.seal_and_checkpoint().await);
    node.compact_now(&[RetentionClass::Days7, RetentionClass::Days365])
        .await
        .unwrap();
    client.configure("lowered", class_config(DAY_MS)).await;
    client.configure("raised", class_config(7 * DAY_MS)).await;
    node.compact_now(&[RetentionClass::Days30]).await.unwrap();
    assert!(node.seal_and_checkpoint().await);

    disk.clock.advance(SEALED_EXPIRY_MS + 2 * HOUR_MS);
    node.expire_now().await.unwrap();
    let copied = disk.extents("7d").await;
    assert_eq!(copied.len(), 1, "{copied:?}");
    assert_eq!((copied[0].0.as_str(), copied[0].1), ("raised", raised));
    assert!(disk.extents("30d").await.is_empty());

    let report = node.expire_now().await.unwrap();
    assert!(!report.deleted_prefixes.is_empty(), "{report:?}");
    assert_eq!(read_all(&node, "raised", 1).await[0].0, raised);
    drop(client);
    node.shutdown();
}
