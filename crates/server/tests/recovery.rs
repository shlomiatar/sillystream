//! Startup, ownership and stream metadata (SPEC sections 7.6, 8.1), on real files and a store
//! the test can count. The crash matrix on the real binary is in `crash_matrix.rs`.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use format::{RecordHeader, encode_record};
use futures::StreamExt;
use object_store::{ObjectStore, PutPayload};
use server::{
    ManualClock, Node, NodeConfig, RecoverConfig, RecoveryReport, StateOpRequest, Subscription,
};
use storage::{ObjectId, Ownership, SegmentBuilder, claim_generation, read_ownership, sealed_path};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

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

    fn wal_dir(&self) -> PathBuf {
        self.dir.path().join("wal")
    }

    fn metadata_dir(&self) -> PathBuf {
        self.dir.path().join("metadata")
    }

    fn recover_config(&self) -> RecoverConfig {
        RecoverConfig {
            wal_dir: self.wal_dir(),
            metadata_dir: self.metadata_dir(),
            wal_preallocate_bytes: 1024 * 1024,
        }
    }

    fn try_start(&self, config: NodeConfig) -> std::io::Result<(Node, RecoveryReport)> {
        Node::recover(
            config,
            self.recover_config(),
            self.store.clone(),
            Arc::new(self.clock.clone()),
        )
    }

    fn start(&self, config: NodeConfig) -> (Node, RecoveryReport) {
        self.try_start(config).unwrap()
    }
}

/// `(first_id, payload)` of every record a subscriber from 0 gets, stopping at `count`.
async fn read_all(node: &Node, stream: &str, count: usize) -> Vec<(u64, Vec<u8>)> {
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(stream), 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= count
    })
    .await;
    // Anything past `count` would be a duplicate or an old record come back.
    hear_for(&mut connection, &mut heard, 100).await;
    heard
        .records(1)
        .into_iter()
        .map(|(id, _, _, data)| (id, data))
        .collect()
}

async fn publish_one(
    client: &mut server::Client,
    stream: &str,
    seq: u64,
    data: &[u8],
) -> (u64, u32) {
    let records = record_with(0, data);
    client
        .publish(publish(stream, &records, seq, 2))
        .await
        .unwrap();
    let (_, first_id, count) = expect_ack(client).await;
    (first_id, count)
}

async fn set_state(client: &mut server::Client, stream: &str, doc: &ciborium::value::Value) {
    client
        .state_op(StateOpRequest {
            stream: key(stream),
            req: 1,
            author: AUTHOR,
            expect_ver: 0,
            op: server::StateOp::Set,
            doc: cbor(doc),
        })
        .await
        .unwrap();
    match expect_reply(client).await {
        server::Reply::StateResult { ok: true, .. } => {}
        other => panic!("expected a state result, got {other:?}"),
    }
}

async fn total_stats(node: &Node) -> server::CoreStats {
    let mut total = server::CoreStats::default();
    for core in 0..node.core_count() {
        let stats = node.stats(core).await.unwrap();
        total.evicted_count += stats.evicted_count;
        total.loaded_count += stats.loaded_count;
        total.rebuilt_count += stats.rebuilt_count;
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_restores_state_dedupe_and_cursors() {
    let disk = Disk::new();
    let (node, report) = disk.start(manual_config(2));
    assert_eq!(report.generation, 1);
    assert!(!report.wal_lost);
    let mut client = node.client(0).await;
    let (first, _) = publish_one(&mut client, "a", 1, b"one").await;
    set_state(&mut client, "a", &map(&[("n", int(5))])).await;
    let (second, _) = publish_one(&mut client, "a", 2, b"two").await;

    let mut connection = node.connection(1).await.unwrap();
    let mut heard = Heard::default();
    let mut named = Subscription::new(1, key("a"), 0);
    named.name = Some(key("worker"));
    connection.subscribe(named);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= 2
    })
    .await;
    connection.commit(1, first);
    assert_eq!(
        connection.read_cursor(key("a"), key("worker")).await,
        Some(first)
    );
    assert!(node.seal_and_checkpoint().await);

    // Above the seal: only the WAL has these.
    let (third, count) = publish_one(&mut client, "a", 3, b"three").await;
    let before = client.snapshot(key("a")).await.unwrap();
    drop((client, connection));
    node.shutdown();

    let (node, report) = disk.start(manual_config(2));
    assert_eq!(report.generation, 2);
    assert!(report.last_valid_id >= third);
    let mut client = node.client(0).await;
    let after = client.snapshot(key("a")).await.unwrap();
    assert_eq!(after.state_ver, before.state_ver);
    assert_eq!(after.state, before.state);
    assert_eq!(after.head, before.head);

    // A retry of the latest publish gets its first ack back, and writes nothing.
    assert_eq!(
        publish_one(&mut client, "a", 3, b"three").await,
        (third, count)
    );

    let connection = node.connection(1).await.unwrap();
    assert_eq!(
        connection.read_cursor(key("a"), key("worker")).await,
        Some(first)
    );

    let records = read_all(&node, "a", 3).await;
    let ids: Vec<u64> = records.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids.len(), 3, "{ids:?}");
    assert_eq!((ids[0], ids[2]), (first, third));
    assert!(second < third);

    // New ids stay above everything earlier, including over another restart.
    let (fourth, _) = publish_one(&mut client, "a", 4, b"four").await;
    assert!(fourth > third);
    drop((client, connection));
    node.shutdown();
    let (node, report) = disk.start(manual_config(2));
    assert!(report.last_valid_id >= fourth);
    assert_eq!(read_all(&node, "a", 4).await.len(), 4);
    node.shutdown();
}

/// Dedupe entries are never saved (section 3): once a checkpoint lets the WAL file holding a
/// publish go, a restart can't dedupe a retry of it, and publishing is at-least-once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_its_wal_file_is_gone_is_a_new_record() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (first, _) = publish_one(&mut client, "a", 1, b"one").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    wait_for_wal_files(&disk, 1, "after the checkpoint").await;
    node.shutdown();

    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (again, _) = publish_one(&mut client, "a", 1, b"one").await;
    assert!(again > first);
    assert_eq!(read_all(&node, "a", 2).await.len(), 2);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_empty_wal_warning_is_only_for_a_node_that_had_a_generation() {
    let disk = Disk::new();
    let (node, report) = disk.start(manual_config(1));
    assert!(!report.wal_lost);
    assert_eq!(report.warning(), None);
    node.shutdown();

    // A normal restart has files.
    let (node, report) = disk.start(manual_config(1));
    assert!(!report.wal_lost);
    assert_eq!(report.warning(), None);
    node.shutdown();

    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    let (node, report) = disk.start(manual_config(1));
    assert!(report.wal_lost);
    assert!(report.warning().unwrap().contains("wal_dir is empty"));
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_records_survive_a_lost_wal_and_ids_stay_above_them() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (first, _) = publish_one(&mut client, "a", 1, b"one").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    let (node, report) = disk.start(manual_config(1));
    assert!(report.wal_lost);
    assert!(report.last_valid_id >= first);
    assert_eq!(read_all(&node, "a", 1).await[0].0, first);
    let mut client = node.client(0).await;
    assert!(publish_one(&mut client, "a", 2, b"two").await.0 > first);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_starts_at_once_have_one_winner() {
    // At the claim itself: both read the same ownership, and one conditional create succeeds.
    let store = ScriptStore::new();
    let known = read_ownership(&store).await.unwrap();
    assert_eq!(claim_generation(&store, &known, 5).await.unwrap(), (1, 5));
    assert!(matches!(
        claim_generation(&store, &known, 9).await,
        Err(storage::OwnershipError::Lost { generation: 1 })
    ));

    // Two whole startups, each with its own disks and one store, held at the claim.
    let (a, b) = (Disk::new(), Disk::new());
    let store = Arc::new(ScriptStore::new());
    store.hold();
    let start = |disk: Disk| {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            Node::recover(
                manual_config(1),
                disk.recover_config(),
                store,
                Arc::new(disk.clock.clone()),
            )
            .map(|(node, report)| {
                node.shutdown();
                report.generation
            })
            .map_err(|error| error.to_string())
        })
    };
    let (first, second) = (start(a), start(b));
    while store.put_begun_count.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    store.release();
    let (first, second) = (first.join().unwrap(), second.join().unwrap());
    let mut results = [first, second];
    results.sort_by_key(Result::is_err);
    assert_eq!(results[0], Ok(1), "{results:?}");
    let lost = results[1].as_ref().unwrap_err();
    assert!(lost.contains("generation 1"), "{lost}");
    let ownership = read_ownership(store.as_ref()).await.unwrap();
    assert_eq!(ownership.current_generation(), Some(1));

    // The same disks: the local store's lock turns the second away.
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    assert!(disk.try_start(manual_config(1)).is_err());
    node.shutdown();
}

fn stamped(id: u64, count: u16, data: &[u8]) -> bytes::Bytes {
    let mut out = Vec::new();
    encode_record(
        &mut out,
        &RecordHeader {
            id,
            options: 0,
            author: 7,
            len: data.len() as u32,
            count,
            codec: 0,
        },
        data,
    )
    .unwrap();
    out.into()
}

/// An object a former owner finished uploading after it lost the node.
async fn put_late_object(
    store: &ScriptStore,
    generation: u64,
    first_id: u64,
    records: Vec<bytes::Bytes>,
) {
    let mut builder = SegmentBuilder::new();
    builder.add_extent(key("late"), records).unwrap();
    let built = builder.finish().unwrap();
    let body: Vec<u8> = built.parts.iter().flatten().copied().collect();
    // Index 255, which no seal here takes, so it can't land on the former owner's own seal.
    store
        .put(
            &sealed_path(ObjectId::new(format::id_ms(first_id), generation, 255)),
            PutPayload::from(body),
        )
        .await
        .unwrap();
}

/// Generations 1 and 2 with a known limit on generation 1, and nothing of `late`.
async fn limit_on_first_generation(disk: &Disk) -> u64 {
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "anchor", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();
    let (node, report) = disk.start(manual_config(1));
    node.shutdown();
    assert_eq!(report.generation, 2);
    let ownership: Ownership = read_ownership(disk.store.as_ref()).await.unwrap();
    assert_eq!(ownership.limit_for(1), Some(report.last_valid_id));
    report.last_valid_id
}

/// A scan needs an object's indexes and any system records, so a publisher's data stays in the
/// store until something reads it (D264).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_doesnt_download_what_publishers_wrote() {
    let disk = Disk::new();
    let limit = limit_on_first_generation(&disk).await;
    let data = vec![7u8; 2 * 1024 * 1024];
    put_late_object(
        disk.store.as_ref(),
        2,
        limit + 1,
        vec![stamped(limit + 1, 1, &data)],
    )
    .await;
    disk.store.range_get_bytes.store(0, Ordering::SeqCst);
    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.adopted_extent_count, 1);
    let scanned = disk.store.range_get_bytes.load(Ordering::SeqCst);
    assert!(scanned < 64 * 1024, "the scan asked for {scanned} bytes");
    assert_eq!(read_all(&node, "late", 1).await, vec![(limit + 1, data)]);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_object_is_accepted_up_to_the_last_valid_id_and_no_further() {
    let disk = Disk::new();
    let limit = limit_on_first_generation(&disk).await;
    put_late_object(
        disk.store.as_ref(),
        1,
        limit - 1,
        vec![
            stamped(limit - 1, 1, b"below"),
            stamped(limit, 1, b"at"),
            stamped(limit + 1, 1, b"above"),
        ],
    )
    .await;
    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.adopted_extent_count, 1);
    assert_eq!(report.last_valid_id, limit);
    let records = read_all(&node, "late", 2).await;
    assert_eq!(
        records,
        vec![(limit - 1, b"below".to_vec()), (limit, b"at".to_vec())]
    );
    // New ids go where the ignored record was, so the two can never be confused.
    let mut client = node.client(0).await;
    let (next, _) = publish_one(&mut client, "late", 1, b"new").await;
    assert!(next > limit);
    assert_eq!(read_all(&node, "late", 3).await.len(), 3);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_record_across_the_last_valid_id_is_ignored_whole() {
    let disk = Disk::new();
    let limit = limit_on_first_generation(&disk).await;
    put_late_object(
        disk.store.as_ref(),
        1,
        limit - 1,
        vec![
            stamped(limit - 1, 1, b"below"),
            stamped(limit, 2, b"across"),
        ],
    )
    .await;
    let (node, _) = disk.start(manual_config(1));
    assert_eq!(
        read_all(&node, "late", 1).await,
        vec![(limit - 1, b"below".to_vec())]
    );
    node.shutdown();

    // Entirely above: nothing of it is taken, and the stream doesn't come to exist.
    let disk = Disk::new();
    let limit = limit_on_first_generation(&disk).await;
    put_late_object(
        disk.store.as_ref(),
        1,
        limit + 1,
        vec![stamped(limit + 1, 1, b"above")],
    )
    .await;
    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.adopted_extent_count, 0);
    assert_eq!(report.ignored_extent_count, 1);
    let mut client = node.client(0).await;
    assert_eq!(
        client.snapshot(key("late")).await.unwrap_err(),
        proto::ErrorCode::NotFound
    );
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_object_with_no_checkpoint_is_found_and_its_records_kept_without_the_wal() {
    // The crash between upload and checkpoint, with the WAL gone as well: section 8.1 scans.
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (id, _) = publish_one(&mut client, "a", 1, b"one").await;
    assert!(matches!(
        node.seal().await,
        server::SealOutcome::Uploaded { .. }
    ));
    drop(client);
    node.shutdown();
    // What the checkpoint held is forgotten too.
    let paths = storage::CheckpointSet::read(
        &(disk.store.clone() as Arc<dyn ObjectStore>),
        read_ownership(disk.store.as_ref()).await.unwrap(),
    )
    .await
    .unwrap();
    for file in &paths.files {
        disk.store
            .delete(&storage::checkpoint_path(file.generation, file.seq))
            .await
            .unwrap();
    }
    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.scanned_object_count, 1);
    assert_eq!(read_all(&node, "a", 1).await[0].0, id);
    node.shutdown();
}

fn evicting_config(limit: usize) -> NodeConfig {
    let mut config = manual_config(1);
    config.active_stream_limit = Some(limit);
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_streams_leave_oldest_first_and_a_live_subscriber_keeps_one_here() {
    let disk = Disk::new();
    let (node, _) = disk.start(evicting_config(2));
    let mut client = node.client(0).await;
    for (seq, stream) in ["watched", "s1", "s2", "s3", "s4"].iter().enumerate() {
        publish_one(&mut client, stream, seq as u64 + 1, b"x").await;
    }
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key("watched"), 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;

    assert!(node.seal_and_checkpoint().await);
    // The oldest three of what can go, which `watched` can't, though it is the oldest of all.
    assert_eq!(total_stats(&node).await.evicted_count, 3);
    assert_eq!(client.snapshot(key("watched")).await.unwrap().state_ver, 0);
    assert_eq!(client.snapshot(key("s4")).await.unwrap().state_ver, 0);
    assert_eq!(total_stats(&node).await.loaded_count, 0, "those two stayed");
    for stream in ["s1", "s2", "s3"] {
        client.snapshot(key(stream)).await.unwrap();
    }
    assert_eq!(total_stats(&node).await.loaded_count, 3, "those went");

    // Still live for a publish after everything else moved around it.
    let (id, _) = publish_one(&mut client, "watched", 9, b"y").await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).last().is_some_and(|record| record.0 == id)
    })
    .await;
    drop((client, connection));
    node.shutdown();
}

/// All a stream has that a reader can see.
struct Seen {
    snapshot: server::SnapshotReply,
    info: server::StreamInfo,
    cursor: Option<u64>,
    records: Vec<(u64, Vec<u8>)>,
}

async fn see(node: &Node, client: &mut server::Client, stream: &str, count: usize) -> Seen {
    let connection = node.connection(0).await.unwrap();
    let info = client.snapshot(key(stream)).await.unwrap();
    let records = read_all(node, stream, count).await;
    Seen {
        info: client.stream_info(key(stream)).await.unwrap(),
        snapshot: info,
        cursor: connection.read_cursor(key(stream), key("worker")).await,
        records,
    }
}

fn assert_same(one: &Seen, other: &Seen) {
    assert_eq!(one.snapshot.head, other.snapshot.head);
    assert_eq!(one.snapshot.state_ver, other.snapshot.state_ver);
    assert_eq!(one.snapshot.state, other.snapshot.state);
    assert_eq!(format!("{:?}", one.info), format!("{:?}", other.info));
    assert_eq!(one.cursor, other.cursor);
    assert_eq!(one.records, other.records);
}

/// A stream with one of everything in its metadata, sealed.
async fn make_full_stream(node: &Node, client: &mut server::Client, stream: &str) {
    client
        .configure(
            key(stream),
            server::StreamConfig {
                default_ack_level: 1,
                min_ack_level: 1,
                retention_ms: 5_000,
            },
        )
        .await;
    let (first, _) = publish_one(client, stream, 1, b"one").await;
    set_state(client, stream, &map(&[("n", int(5))])).await;
    publish_one(client, stream, 2, b"two").await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut named = Subscription::new(1, key(stream), 0);
    named.name = Some(key("worker"));
    connection.subscribe(named);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= 2
    })
    .await;
    connection.commit(1, first);
    assert_eq!(
        connection.read_cursor(key(stream), key("worker")).await,
        Some(first)
    );
    connection.unsubscribe(1);
    drop(connection);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_evicted_stream_comes_back_identical_on_first_access() {
    let disk = Disk::new();
    let (node, _) = disk.start(evicting_config(1));
    let mut client = node.client(0).await;
    make_full_stream(&node, &mut client, "cold").await;
    assert!(node.seal_and_checkpoint().await);
    let before = see(&node, &mut client, "cold", 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    publish_one(&mut client, "newer", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);
    let stats = total_stats(&node).await;
    assert_eq!(stats.evicted_count, 1);

    let after = see(&node, &mut client, "cold", 2).await;
    assert_same(&before, &after);
    let stats = total_stats(&node).await;
    assert_eq!(stats.loaded_count, 1);
    assert_eq!(stats.rebuilt_count, 0);

    // Dedupe entries live in memory only (section 3), so eviction dropped them: a retry after
    // the reload is a new record, after the ones it had.
    let retry_ack = publish_one(&mut client, "cold", 2, b"two").await;
    assert!(retry_ack.0 > before.records[1].0);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_the_local_store_gone_a_stream_is_rebuilt_from_checkpoints() {
    let disk = Disk::new();
    let mut config = manual_config(1);
    config.full_checkpoint_every_count = 4;
    let (node, _) = disk.start(config.clone());
    let mut client = node.client(0).await;
    make_full_stream(&node, &mut client, "old").await;
    assert!(node.seal_and_checkpoint().await);
    let before = see(&node, &mut client, "old", 2).await;

    // The long gap: many seals, past several full checkpoints, that don't touch it.
    let puts_before = disk.store.put_count.load(Ordering::SeqCst);
    for seq in 0..10 {
        publish_one(&mut client, "busy", seq + 1, b"x").await;
        assert!(node.seal_and_checkpoint().await);
    }
    assert!(disk.store.put_count.load(Ordering::SeqCst) >= puts_before + 20);
    drop(client);
    node.shutdown();

    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, _) = disk.start(config.clone());
    let mut client = node.client(0).await;
    let after = see(&node, &mut client, "old", 2).await;
    assert_same(&before, &after);
    assert_eq!(total_stats(&node).await.rebuilt_count, 1);
    assert_eq!(read_all(&node, "busy", 10).await.len(), 10);

    // Older files were let go of, and what's left still has it all, including the new id.
    let (next, _) = publish_one(&mut client, "old", 3, b"three").await;
    assert!(next > before.snapshot.head);
    drop(client);
    node.shutdown();

    // A name nobody ever used is still nobody's after a walk of the checkpoints.
    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, _) = disk.start(config);
    let mut client = node.client(0).await;
    assert_eq!(
        client.snapshot(key("never")).await.unwrap_err(),
        proto::ErrorCode::NotFound
    );
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_puts_per_seal_do_not_depend_on_how_many_streams_changed() {
    async fn puts_for_a_seal(stream_count: usize) -> u64 {
        let disk = Disk::new();
        let (node, _) = disk.start(manual_config(2));
        let mut client = node.client(0).await;
        let records = record_with(0, b"x");
        let mut waiting = 0usize;
        for number in 0..stream_count {
            let stream = format!("s.{number}");
            client
                .publish(publish(&stream, &records, 1, 1))
                .await
                .unwrap();
            waiting += 1;
            if waiting == 256 {
                while waiting > 0 {
                    expect_ack(&mut client).await;
                    waiting -= 1;
                }
            }
        }
        while waiting > 0 {
            expect_ack(&mut client).await;
            waiting -= 1;
        }
        let before = disk.store.put_count.load(Ordering::SeqCst);
        assert!(node.seal_and_checkpoint().await);
        let puts = disk.store.put_count.load(Ordering::SeqCst) - before;
        drop(client);
        node.shutdown();
        puts
    }
    let few = puts_for_a_seal(10).await;
    let many = puts_for_a_seal(100_000).await;
    assert_eq!(few, many);
    assert_eq!(few, 2, "one object and one checkpoint");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleted_streams_stay_deleted_and_a_new_one_cannot_read_the_old() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(2));
    let mut client = node.client(0).await;
    publish_one(&mut client, "gone", 1, b"sealed").await;
    assert!(node.seal_and_checkpoint().await);
    let (old_id, _) = publish_one(&mut client, "gone", 2, b"unsealed").await;
    // Answered once a checkpoint has it, so a crash right here can't bring it back.
    assert!(client.delete_stream(key("gone")).await);
    drop(client);
    node.shutdown();

    let (node, _) = disk.start(manual_config(2));
    let mut client = node.client(0).await;
    assert_eq!(
        client.snapshot(key("gone")).await.unwrap_err(),
        proto::ErrorCode::NotFound
    );
    drop(client);
    node.shutdown();

    // Rebuilt from checkpoints, with no local store and no WAL.
    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    let (node, _) = disk.start(manual_config(2));
    let mut client = node.client(0).await;
    assert_eq!(
        client.snapshot(key("gone")).await.unwrap_err(),
        proto::ErrorCode::NotFound
    );

    // Made again: higher ids, and none of the old records.
    let (new_id, _) = publish_one(&mut client, "gone", 1, b"new").await;
    assert!(new_id > old_id);
    assert_eq!(
        read_all(&node, "gone", 1).await,
        vec![(new_id, b"new".to_vec())]
    );
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    let (node, _) = disk.start(manual_config(2));
    assert_eq!(
        read_all(&node, "gone", 1).await,
        vec![(new_id, b"new".to_vec())]
    );
    node.shutdown();
    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, _) = disk.start(manual_config(2));
    assert_eq!(
        read_all(&node, "gone", 1).await,
        vec![(new_id, b"new".to_vec())]
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_with_only_the_wal_is_not_lost_nor_a_later_recreate() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (old_id, _) = publish_one(&mut client, "gone", 1, b"old").await;
    assert!(client.delete_stream(key("gone")).await);
    let (new_id, _) = publish_one(&mut client, "gone", 1, b"new").await;
    assert!(new_id > old_id);
    drop(client);
    node.shutdown();
    let (node, _) = disk.start(manual_config(1));
    assert_eq!(
        read_all(&node, "gone", 1).await,
        vec![(new_id, b"new".to_vec())]
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_evicted_stream_loads_on_the_core_its_shard_moved_to() {
    let disk = Disk::new();
    let mut config = manual_config(2);
    config.active_stream_limit = Some(2);
    let (node, _) = disk.start(config);
    let mut client = node.client(0).await;
    let (id, _) = publish_one(&mut client, "wanderer", 1, b"x").await;
    for seq in 0..16 {
        publish_one(&mut client, &format!("filler.{seq}"), 1, b"y").await;
    }
    assert!(node.seal_and_checkpoint().await);
    let shard = server::shard_of(key("wanderer"), node.virtual_shard_count());
    let at = (shard as usize) % 2;
    let before = node.stats(at).await.unwrap().evicted_count;
    assert!(before > 0);
    assert!(node.move_shard(shard, 1 - at).await);

    let mut fresh = node.client(1).await;
    assert_eq!(fresh.snapshot(key("wanderer")).await.unwrap().head, id);
    // Eviction dropped its dedupe entries (section 3), so the same seq is a new record.
    let again = publish_one(&mut fresh, "wanderer", 1, b"x").await.0;
    assert!(again > id);
    let ids: Vec<u64> = read_all(&node, "wanderer", 2)
        .await
        .iter()
        .map(|record| record.0)
        .collect();
    assert_eq!(ids, vec![id, again]);
    drop((client, fresh));
    node.shutdown();
}

// ---- Two owners on one bucket, local metadata ahead of S3, and objects no checkpoint records.

fn full_every(count: usize) -> NodeConfig {
    let mut config = manual_config(1);
    config.full_checkpoint_every_count = count;
    config
}

/// A second machine's disks, on the same bucket.
fn replacement_for(disk: &Disk) -> Disk {
    let mut replacement = Disk::new();
    replacement.store = disk.store.clone();
    replacement
}

fn fail_checkpoint_puts(disk: &Disk, budget: i64) {
    disk.store
        .checkpoint_put_budget
        .store(budget, Ordering::SeqCst);
}

async fn checkpoint_files(disk: &Disk) -> Vec<object_store::path::Path> {
    use futures::TryStreamExt;
    disk.store
        .list(Some(&object_store::path::Path::from("checkpoints")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap()
}

/// A few seals of another stream, each with a checkpoint.
async fn busy_seals(node: &Node, rounds: u64) {
    let mut client = node.client(0).await;
    for seq in 1..=rounds {
        publish_one(&mut client, "busy", seq + 1_000_000, b"busy").await;
        assert!(node.seal_and_checkpoint().await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_former_owners_full_file_leaves_what_the_next_owner_needs() {
    let old_disk = Disk::new();
    let new_disk = replacement_for(&old_disk);
    let (old, _) = old_disk.start(full_every(2));
    let mut old_client = old.client(0).await;
    let (first, _) = publish_one(&mut old_client, "s", 1, b"accepted").await;
    assert!(old.seal_and_checkpoint().await);

    let (new, report) = new_disk.start(full_every(1000));
    assert!(report.last_valid_id >= first);
    new.shutdown();

    // The old machine doesn't know. Its second file is a full one, with an id over the limit.
    old_disk.clock.advance(1000);
    let (second, _) = publish_one(&mut old_client, "s", 2, b"above").await;
    assert!(second > report.last_valid_id);
    assert!(old.seal_and_checkpoint().await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(checkpoint_files(&old_disk).await.len(), 2);
    drop(old_client);
    old.shutdown();

    std::fs::remove_dir_all(new_disk.metadata_dir()).unwrap();
    let (new, _) = new_disk.start(full_every(1000));
    assert_eq!(
        read_all(&new, "s", 1).await,
        vec![(first, b"accepted".to_vec())]
    );
    new.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_owners_load_survives_a_former_owners_full_file() {
    let old_disk = Disk::new();
    let new_disk = replacement_for(&old_disk);
    let (old, _) = old_disk.start(full_every(2));
    let mut old_client = old.client(0).await;
    let (first, _) = publish_one(&mut old_client, "s", 1, b"accepted").await;
    assert!(old.seal_and_checkpoint().await);

    let (new, report) = new_disk.start(full_every(1000));
    old_disk.clock.advance(1000);
    let (second, _) = publish_one(&mut old_client, "s", 2, b"above").await;
    assert!(second > report.last_valid_id);
    assert!(old.seal_and_checkpoint().await);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = new.client(0).await;
    let answer = tokio::time::timeout(Duration::from_secs(5), client.snapshot(key("s"))).await;
    drop((client, old_client));
    old.shutdown();
    let hung = answer.is_err();
    if hung {
        // Its threads would never let a shutdown finish.
        std::mem::forget(new);
    } else {
        assert_eq!(read_all(&new, "s", 1).await[0].0, first);
        new.shutdown();
    }
    assert!(!hung, "a running new owner's load never returned");
    assert!(answer.unwrap().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_startup_clears_files_a_full_one_has_replaced() {
    let disk = Disk::new();
    let (node, _) = disk.start(full_every(2));
    busy_seals(&node, 3).await;
    assert_eq!(checkpoint_files(&disk).await.len(), 3);
    node.shutdown();

    let (node, _) = disk.start(full_every(2));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while checkpoint_files(&disk).await.len() > 2 {
        assert!(std::time::Instant::now() < deadline, "older files stayed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_object_at_the_limit_stays_out_of_a_deleted_stream_and_its_successor() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (old_id, _) = publish_one(&mut client, "late", 1, b"old").await;
    disk.clock.advance(5);
    publish_one(&mut client, "anchor", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    let (node, report) = disk.start(manual_config(1));
    let limit = report.last_valid_id;
    assert!(limit > old_id);
    let mut client = node.client(0).await;
    assert!(client.delete_stream(key("late")).await);
    drop(client);
    // Generation 1's, finishing after the delete, with an id the limit accepts.
    let object_records = vec![stamped(limit, 1, b"old generation")];
    put_late_object(disk.store.as_ref(), 1, limit, object_records.clone()).await;
    node.shutdown();

    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    assert_eq!(
        client.snapshot(key("late")).await.err(),
        Some(proto::ErrorCode::NotFound),
        "deleted stream came back"
    );
    drop(client);
    node.shutdown();

    // The same, when the name is made again before the object is seen.
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "late", 1, b"old").await;
    disk.clock.advance(5);
    publish_one(&mut client, "anchor", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();
    let (node, report) = disk.start(manual_config(1));
    let limit = report.last_valid_id;
    let mut client = node.client(0).await;
    assert!(client.delete_stream(key("late")).await);
    let (new_id, _) = publish_one(&mut client, "late", 1, b"new").await;
    assert!(new_id > limit);
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    put_late_object(
        disk.store.as_ref(),
        1,
        limit,
        vec![stamped(limit, 1, b"old generation")],
    )
    .await;
    node.shutdown();
    let (node, _) = disk.start(manual_config(1));
    assert_eq!(
        read_all(&node, "late", 1).await,
        vec![(new_id, b"new".to_vec())]
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seal_saved_only_locally_reaches_the_checkpoints_after_a_restart() {
    let disk = Disk::new();
    let (node, _) = disk.start(full_every(2));
    fail_checkpoint_puts(&disk, 0);
    let mut client = node.client(0).await;
    let (id, _) = publish_one(&mut client, "victim", 1, b"one").await;
    node.seal().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    drop(client);
    // Stands in for a crash: the file was never written, and the local store has the extent.
    node.shutdown();

    fail_checkpoint_puts(&disk, -1);
    let (node, _) = disk.start(full_every(2));
    assert_eq!(
        read_all(&node, "victim", 1).await,
        vec![(id, b"one".to_vec())]
    );
    busy_seals(&node, 4).await;
    node.shutdown();

    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, _) = disk.start(full_every(2));
    assert_eq!(
        read_all(&node, "victim", 1).await,
        vec![(id, b"one".to_vec())],
        "acked record gone after rebuild"
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_saved_only_locally_reaches_the_checkpoints_after_a_restart() {
    let disk = Disk::new();
    let (node, _) = disk.start(full_every(2));
    let mut client = node.client(0).await;
    publish_one(&mut client, "gone", 1, b"old").await;
    assert!(node.seal_and_checkpoint().await);
    publish_one(&mut client, "other", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);

    fail_checkpoint_puts(&disk, 0);
    let mut deleter = node.client(0).await;
    let unanswered = tokio::spawn(async move { deleter.delete_stream(key("gone")).await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    drop(client);
    node.shutdown();
    let _ = unanswered.await;

    fail_checkpoint_puts(&disk, -1);
    let (node, _) = disk.start(full_every(2));
    let mut client = node.client(0).await;
    assert!(!client.delete_stream(key("gone")).await);
    assert_eq!(
        client.snapshot(key("gone")).await.err(),
        Some(proto::ErrorCode::NotFound)
    );
    drop(client);
    busy_seals(&node, 4).await;
    node.shutdown();

    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, _) = disk.start(full_every(2));
    let mut client = node.client(0).await;
    assert_eq!(
        client.snapshot(key("gone")).await.err(),
        Some(proto::ErrorCode::NotFound),
        "deleted stream is back"
    );
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_level_3_record_survives_when_an_older_seals_full_file_lands_after_its_object() {
    let disk = Disk::new();
    let (node, _) = disk.start(full_every(1));
    fail_checkpoint_puts(&disk, 0);
    let mut client = node.client(0).await;
    publish_one(&mut client, "first", 1, b"one").await;
    node.seal().await;
    disk.clock.advance(5);
    let records = record_with(0, b"victim");
    client
        .publish(publish("victim", &records, 1, 3))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    node.seal().await;
    let (_, acked, _) = expect_ack(&mut client).await;

    // The first seal's job gets its full file in. The second never does.
    fail_checkpoint_puts(&disk, 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(client);
    node.shutdown();
    fail_checkpoint_puts(&disk, -1);

    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let (node, report) = disk.start(full_every(1));
    assert!(report.last_valid_id >= acked, "{report:?} below {acked}");
    assert_eq!(
        read_all(&node, "victim", 1).await,
        vec![(acked, b"victim".to_vec())],
        "level 3 acked record lost"
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losing_every_checkpoint_of_a_seal_leaves_its_records_reachable() {
    let disk = Disk::new();
    let (node, _) = disk.start(full_every(64));
    let mut client = node.client(0).await;
    let (id, _) = publish_one(&mut client, "a", 1, b"one").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    // What a crash before the only PUT would leave.
    for path in checkpoint_files(&disk).await {
        disk.store.delete(&path).await.unwrap();
    }
    let (node, _) = disk.start(full_every(1));
    busy_seals(&node, 3).await;
    node.shutdown();

    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    let (node, _) = disk.start(full_every(1));
    assert_eq!(
        read_all(&node, "a", 1).await,
        vec![(id, b"one".to_vec())],
        "sealed record gone after machine loss"
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_object_is_still_found_after_a_full_file_and_only_looked_at_once() {
    let disk = Disk::new();
    let limit = limit_on_first_generation(&disk).await;
    let (node, _) = disk.start(full_every(1));
    put_late_object(
        disk.store.as_ref(),
        1,
        limit - 1,
        vec![stamped(limit - 1, 1, b"below")],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    busy_seals(&node, 1).await;
    node.shutdown();

    let (node, report) = disk.start(full_every(1));
    assert_eq!(report.adopted_extent_count, 1, "{report:?}");
    assert_eq!(
        read_all(&node, "late", 1).await,
        vec![(limit - 1, b"below".to_vec())]
    );
    busy_seals(&node, 2).await;
    node.shutdown();

    // Every object is recorded by now, through however many full files.
    let (node, report) = disk.start(full_every(1));
    assert_eq!(report.scanned_object_count, 0, "{report:?}");
    assert_eq!(
        read_all(&node, "late", 1).await,
        vec![(limit - 1, b"below".to_vec())]
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_checkpoint_over_the_limit_does_not_hide_its_object() {
    let disk = Disk::new();
    let limit = limit_on_first_generation(&disk).await;
    put_late_object(
        disk.store.as_ref(),
        1,
        limit - 1,
        vec![
            stamped(limit - 1, 1, b"below"),
            stamped(limit + 1, 1, b"above"),
        ],
    )
    .await;
    let meta = storage::StreamMeta {
        stream: key("late"),
        last_id: limit + 1,
        applied_id: limit + 1,
        ..Default::default()
    };
    let head = storage::CheckpointHead {
        full: false,
        max_id: limit + 1,
        rows: vec![storage::ObjectRow::new(
            storage::ObjectKey::sealed(ObjectId::new(format::id_ms(limit - 1), 1, 255)),
            storage::ObjectState::Remote,
        )],
        removed: Vec::new(),
        sink_streams: Vec::new(),
    };
    let built = storage::build_checkpoint(&head, &[meta]).unwrap();
    storage::put_checkpoint(
        disk.store.as_ref(),
        &storage::checkpoint_path(1, 999),
        &built,
    )
    .await
    .unwrap();

    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.adopted_extent_count, 1, "{report:?}");
    assert_eq!(
        read_all(&node, "late", 1).await,
        vec![(limit - 1, b"below".to_vec())]
    );
    node.shutdown();
}

/// An old disk coming back after another generation ran on the bucket: its local store is only
/// a cache of what S3 had then, and S3 has moved on (section 7.6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_store_left_from_before_a_replacement_is_not_trusted() {
    let disk_a = Disk::new();
    let (node, _) = disk_a.start(manual_config(2));
    let mut client = node.client(0).await;
    let (x1, _) = publish_one(&mut client, "x", 1, b"x1").await;
    publish_one(&mut client, "z", 1, b"z1").await;
    set_state(&mut client, "x", &map(&[("n", int(1))])).await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    let disk_b = replacement_for(&disk_a);
    disk_b.clock.advance(10_000);
    let (node, _) = disk_b.start(manual_config(2));
    let mut client = node.client(0).await;
    let (x2, _) = publish_one(&mut client, "x", 2, b"x2").await;
    set_state(&mut client, "x", &map(&[("n", int(2))])).await;
    publish_one(&mut client, "y", 1, b"y1").await;
    assert!(client.delete_stream(key("z")).await);
    assert!(node.seal_and_checkpoint().await);
    let before = client.snapshot(key("x")).await.unwrap();
    drop(client);
    node.shutdown();

    disk_a.clock.advance(20_000);
    let (node, _) = disk_a.start(manual_config(2));
    let mut client = node.client(0).await;
    let after = client.snapshot(key("x")).await.unwrap();
    assert_eq!(after.state_ver, before.state_ver, "x's state went back");
    assert_eq!(after.head, before.head, "x's head went back");
    assert!(client.snapshot(key("y")).await.is_ok(), "y is gone");
    assert!(
        client.snapshot(key("z")).await.is_err(),
        "deleted z is back"
    );
    let got: Vec<u64> = read_all(&node, "x", 2)
        .await
        .into_iter()
        .filter(|(_, data)| data == b"x1" || data == b"x2")
        .map(|(id, _)| id)
        .collect();
    assert_eq!(got, [x1, x2]);
    drop(client);
    node.shutdown();
}

/// A plain restart keeps its store, and reads nothing it doesn't have to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_with_no_other_generation_keeps_its_local_store() {
    let disk = Disk::new();
    let (node, _) = disk.start(evicting_config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "a", 1, b"a1").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    assert!(client.snapshot(key("a")).await.is_ok());
    assert_eq!(total_stats(&node).await.rebuilt_count, 0);
    drop(client);
    node.shutdown();
}

/// Section 8.1 holds for what a former generation left in its WAL, as it does for its objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wal_entry_above_the_former_generations_limit_is_ignored() {
    let disk_a = Disk::new();
    let (node, _) = disk_a.start(manual_config(1));
    let mut client = node.client(0).await;
    let (r1, _) = publish_one(&mut client, "s", 1, b"r1").await;
    assert!(node.seal_and_checkpoint().await);
    disk_a.clock.advance(1_000);
    let (r2, _) = publish_one(&mut client, "s", 2, b"r2").await;
    drop(client);
    node.shutdown();

    let disk_b = replacement_for(&disk_a);
    disk_b.clock.advance(5_000);
    let (node, report) = disk_b.start(manual_config(1));
    assert!(report.last_valid_id < r2, "the replacement never saw r2");
    let mut client = node.client(0).await;
    let (r3, _) = publish_one(&mut client, "s", 3, b"r3").await;
    assert!(node.seal_and_checkpoint().await);
    drop(client);
    node.shutdown();

    std::fs::remove_dir_all(disk_a.metadata_dir()).unwrap();
    disk_a.clock.advance(10_000);
    let (node, report) = disk_a.start(manual_config(1));
    // r1's file went once it was checkpointed, so all that is left is r2, which is above it.
    assert_eq!(report.replayed_entry_count, 0);
    let got: Vec<u64> = read_all(&node, "s", 2)
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(got, [r1, r3]);
    node.shutdown();
}

/// A crash of the same generation's process replays every entry: nothing has limited it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_entries_survive_two_restarts_on_their_own_disk() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (r1, _) = publish_one(&mut client, "s", 1, b"r1").await;
    drop(client);
    node.shutdown();
    let (node, _) = disk.start(manual_config(1));
    node.shutdown();
    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.replayed_entry_count, 1);
    let got = read_all(&node, "s", 1).await;
    assert_eq!(got[0].0, r1);
    node.shutdown();
}

/// A stream on `to`, published to level 2 in the upload window of a seal, on the core `from`
/// that is about to lose it, with enough other streams on `to` that it is due for eviction.
/// Ends with the seal done and the record on `from`, unsealed. Returns the two ids.
async fn move_in_upload_window(
    disk: &Disk,
    node: &Node,
    client: &mut server::Client,
    stream: &str,
) -> (u64, u64, usize) {
    let shard = server::shard_of(key(stream), node.virtual_shard_count());
    let from = shard as usize % 2;
    let to = 1 - from;
    let (first, _) = publish_one(client, stream, 1, b"one").await;
    disk.clock.advance(1);
    disk.store.hold();
    let begun = disk.store.put_begun_count.load(Ordering::SeqCst);
    let window = async {
        while disk.store.put_begun_count.load(Ordering::SeqCst) == begun {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        disk.clock.advance(1);
        let (second, _) = publish_one(client, stream, 2, b"two").await;
        assert!(node.move_shard(shard, to).await);
        let mut filler = 0;
        let mut made = 0;
        while made < 3 {
            filler += 1;
            let name = format!("filler.{filler}");
            if server::shard_of(key(&name), node.virtual_shard_count()) as usize % 2 == to {
                disk.clock.advance(1);
                publish_one(client, &name, 1, b"f").await;
                made += 1;
            }
        }
        disk.store.release();
        second
    };
    let (sealed, second) = tokio::join!(node.seal_and_checkpoint(), window);
    assert!(sealed);
    (first, second, to)
}

fn two_cores_evicting() -> NodeConfig {
    let mut config = manual_config(2);
    config.active_stream_limit = Some(2);
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_level_3_retry_waits_for_a_record_its_former_core_still_holds() {
    let disk = Disk::new();
    let (node, _) = disk.start(two_cores_evicting());
    let mut client = node.client(0).await;
    let (_, second, _) = move_in_upload_window(&disk, &node, &mut client, "m").await;

    let records = record_with(0, b"two");
    client.publish(publish("m", &records, 2, 3)).await.unwrap();
    // Only the WAL and the former core's memory have it.
    expect_quiet(&mut client).await;
    assert!(node.seal_and_checkpoint().await);
    let (_, first_id, _) = expect_ack(&mut client).await;
    assert_eq!(first_id, second);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_gets_a_record_its_former_core_still_holds() {
    let disk = Disk::new();
    let (node, _) = disk.start(two_cores_evicting());
    let mut client = node.client(0).await;
    let (first, second, to) = move_in_upload_window(&disk, &node, &mut client, "m").await;

    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key("m"), 0));
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.records(1).is_empty()
    })
    .await;
    assert!(node.seal_and_checkpoint().await);
    disk.clock.advance(1);
    let (third, _) = publish_one(&mut client, "m", 3, b"three").await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).last().is_some_and(|last| last.0 >= third)
    })
    .await;
    let got: Vec<u64> = heard.records(1).iter().map(|record| record.0).collect();
    assert_eq!(got, [first, second, third]);
    drop(connection);

    // Once the former core has let go, it can leave RAM after all.
    assert!(node.seal_and_checkpoint().await);
    assert!(node.seal_and_checkpoint().await);
    assert!(node.stats(to).await.unwrap().evicted_count >= 1);
    drop(client);
    node.shutdown();
}

fn wal_file_count(disk: &Disk) -> usize {
    storage::list_wal_files(&disk.wal_dir()).unwrap().len()
}

/// The writer hears of a removal by message, and doesn't answer it.
async fn wait_for_wal_files(disk: &Disk, count: usize, what: &str) {
    // A full suite's load can hold a removal back for seconds, so this goes by time.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if wal_file_count(disk) <= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("{what}: {} files", wal_file_count(disk));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarts_leave_no_wal_files_behind() {
    let disk = Disk::new();
    for round in 1..=8u64 {
        let (node, _) = disk.start(manual_config(1));
        let mut client = node.client(0).await;
        disk.clock.advance(1);
        publish_one(&mut client, "s", round, b"x").await;
        assert!(node.seal_and_checkpoint().await);
        drop(client);
        wait_for_wal_files(&disk, 2, &format!("round {round}")).await;
        node.shutdown();
    }
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    for seq in 100..150u64 {
        disk.clock.advance(1);
        publish_one(&mut client, "s", seq, b"x").await;
        assert!(node.seal_and_checkpoint().await);
        wait_for_wal_files(&disk, 2, &format!("seq {seq}")).await;
    }
    assert_eq!(read_all(&node, "s", 58).await.len(), 58);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_loop_with_nothing_written_does_not_grow_the_wal() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "s", 1, b"kept").await;
    drop(client);
    node.shutdown();
    for _ in 0..8 {
        let (node, report) = disk.start(manual_config(1));
        assert_eq!(
            report.replayed_entry_count, 1,
            "the file with an entry stays"
        );
        node.shutdown();
    }
    // That file, the file before the last start's, and the last start's own.
    assert_eq!(wal_file_count(&disk), 3);
    let (node, report) = disk.start(manual_config(1));
    assert!(!report.wal_lost);
    assert_eq!(read_all(&node, "s", 1).await.len(), 1);
    node.shutdown();
}

/// Ids repeat across streams, and on a clock that hasn't moved, two seals can have the same
/// lowest id. Nothing an owner acked may depend on that (Q16).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seals_with_the_same_lowest_id_lose_nothing_across_a_restart() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(2));
    let node = Arc::new(node);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sealer = {
        let (node, stop) = (Arc::clone(&node), Arc::clone(&stop));
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                node.seal_and_checkpoint().await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let mut client = node.client(0).await;
    let names: Vec<String> = (0..8).map(|at| format!("m.{at}")).collect();
    let mut acked: Vec<Vec<u64>> = vec![Vec::new(); names.len()];
    let start = std::time::Instant::now();
    let mut seq = 0;
    while start.elapsed() < Duration::from_secs(3) {
        seq += 1;
        for (at, name) in names.iter().enumerate() {
            acked[at].push(publish_one(&mut client, name, seq, b"payload").await.0);
        }
    }
    stop.store(true, Ordering::SeqCst);
    sealer.await.unwrap();
    drop(client);
    let node = Arc::into_inner(node).unwrap();
    node.shutdown();

    let (node, _) = disk.start(manual_config(2));
    for (at, name) in names.iter().enumerate() {
        let got: Vec<u64> = read_all(&node, name, acked[at].len())
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(got, acked[at], "{name} after a restart");
    }
    assert!(node.seal_and_checkpoint().await);
    node.shutdown();

    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    let (node, _) = disk.start(manual_config(2));
    for (at, name) in names.iter().enumerate() {
        let got: Vec<u64> = read_all(&node, name, acked[at].len())
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(got, acked[at], "{name} from S3 alone");
    }
    node.shutdown();
}

/// A dedicated object can land before the seal holding earlier ids, so a crash can leave records
/// to restore on both sides of an object. They can't go up as one extent around it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_restored_on_both_sides_of_an_object_stay_readable() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let (r1, _) = publish_one(&mut client, "s", 1, b"r1").await;
    disk.clock.advance(1);
    let (r2, _) = publish_one(&mut client, "s", 2, b"r2").await;
    disk.clock.advance(1);
    let (r3, _) = publish_one(&mut client, "s", 3, b"r3").await;
    drop(client);
    node.shutdown();

    let mut builder = SegmentBuilder::new();
    builder
        .add_extent(key("s"), vec![stamped(r2, 1, b"r2")])
        .unwrap();
    let body: Vec<u8> = builder
        .finish()
        .unwrap()
        .parts
        .iter()
        .flatten()
        .copied()
        .collect();
    disk.store
        .put(
            &storage::sealed_path(ObjectId::new(format::id_ms(r2), 1, 1)),
            PutPayload::from(body),
        )
        .await
        .unwrap();

    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.adopted_extent_count, 1);
    assert!(node.seal_and_checkpoint().await);
    assert!(node.seal_and_checkpoint().await);
    let want = [r1, r2, r3];
    let got: Vec<u64> = read_all(&node, "s", 3).await.iter().map(|r| r.0).collect();
    assert_eq!(got, want, "after the seals");
    node.shutdown();

    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    std::fs::remove_dir_all(disk.wal_dir()).unwrap();
    let (node, _) = disk.start(manual_config(1));
    let got: Vec<u64> = read_all(&node, "s", 3).await.iter().map(|r| r.0).collect();
    assert_eq!(got, want, "from S3 alone");
    node.shutdown();
}

/// A `DirVolume` on a volume with no room for a new file.
struct FullVolume {
    inner: storage::DirVolume,
    created_count: Arc<std::sync::atomic::AtomicU64>,
    full: Arc<std::sync::atomic::AtomicBool>,
}

impl storage::WalVolume for FullVolume {
    type File = std::fs::File;

    fn create(&mut self, seq: u64) -> std::io::Result<std::fs::File> {
        self.created_count.fetch_add(1, Ordering::SeqCst);
        if self.full.load(Ordering::SeqCst) {
            return Err(std::io::ErrorKind::StorageFull.into());
        }
        self.inner.create(seq)
    }

    fn remove(&mut self, seq: u64) -> std::io::Result<()> {
        // Removing a file is what makes room.
        self.full.store(false, Ordering::SeqCst);
        self.inner.remove(seq)
    }
}

/// With no room for a new WAL file, a restart still starts: it serves reads, holds writes back,
/// and uploads what the closed files hold, and removing those makes room for the file it then
/// writes to (D263). It was
/// stuck: nothing could be uploaded until the writer had a file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_with_no_room_for_a_wal_file_uploads_its_way_out() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    let mut ids = Vec::new();
    for seq in 1..=3 {
        ids.push(publish_one(&mut client, "full", seq, b"acked").await.0);
    }
    drop(client);
    node.shutdown();

    let created_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (wal_dir, attempts) = (disk.wal_dir(), Arc::clone(&created_count));
    let (node, report) = Node::recover_on(
        manual_config(1),
        disk.recover_config(),
        disk.store.clone(),
        Arc::new(disk.clock.clone()),
        move |generation| FullVolume {
            inner: storage::DirVolume::for_generation(wal_dir, 1024 * 1024, generation),
            created_count: attempts,
            full: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        },
    )
    .expect("a full volume doesn't stop a start");
    assert_eq!(report.generation, 2);
    assert!(created_count.load(Ordering::SeqCst) >= 1);

    // What is acked can be read at once, before any upload.
    let got: Vec<u64> = read_all(&node, "full", 3)
        .await
        .iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(got, ids);

    // A new write can't be made durable yet, so no ack comes.
    let mut client = node.client(0).await;
    let records = record_with(0, b"new");
    client
        .publish(publish("full", &records, 4, 2))
        .await
        .unwrap();
    assert!(
        client.recv_within(200).await.is_none(),
        "an answer with nowhere to have written it"
    );

    // One seal uploads it all and frees a file, and then the write goes through.
    assert!(node.seal_and_checkpoint().await);
    let (_, first_id, _) = expect_ack(&mut client).await;
    assert!(first_id > ids[2]);
    let files = storage::list_wal_files(&disk.wal_dir()).unwrap();
    assert_eq!(files.len(), 1, "{files:?}");
    let got = read_all(&node, "full", 4).await;
    assert_eq!(got.len(), 4);
    drop(client);
    node.shutdown();

    // And nothing was lost over another restart, on a volume with room again.
    let (node, report) = disk.start(manual_config(1));
    assert_eq!(report.generation, 3);
    assert_eq!(read_all(&node, "full", 4).await.len(), 4);
    node.shutdown();
}

/// The same at a rotation: the node had a file, the volume filled, and the seal goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rotation_with_no_room_still_seals_and_frees_a_file() {
    let disk = Disk::new();
    let full = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (wal_dir, full_flag) = (disk.wal_dir(), Arc::clone(&full));
    let (node, _) = Node::recover_on(
        manual_config(1),
        disk.recover_config(),
        disk.store.clone(),
        Arc::new(disk.clock.clone()),
        move |generation| FullVolume {
            inner: storage::DirVolume::for_generation(wal_dir, 1024 * 1024, generation),
            created_count: Arc::default(),
            full: full_flag,
        },
    )
    .unwrap();
    let mut client = node.client(0).await;
    let (first_id, _) = publish_one(&mut client, "full", 1, b"before").await;
    full.store(true, Ordering::SeqCst);
    assert!(node.seal_and_checkpoint().await);
    let (next_id, _) = publish_one(&mut client, "full", 2, b"after").await;
    assert!(next_id > first_id);
    let got = read_all(&node, "full", 2).await;
    assert_eq!(got.len(), 2);
    drop(client);
    node.shutdown();
}

/// With the local store gone and a checkpoint that stays bad, what needs the stream is refused
/// with the retryable code rather than waiting for ever, and other streams aren't held up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_that_stays_bad_refuses_what_waits_for_it() {
    let disk = Disk::new();
    let (node, _) = disk.start(manual_config(1));
    let mut client = node.client(0).await;
    publish_one(&mut client, "kept", 1, b"x").await;
    assert!(node.seal_and_checkpoint().await);
    // Long enough that the WAL no longer holds it, so that only a checkpoint can bring it back.
    for seq in 0..10 {
        publish_one(&mut client, "busy", seq + 1, b"x").await;
        assert!(node.seal_and_checkpoint().await);
    }
    // Not sealed, so the WAL holds this stream.
    publish_one(&mut client, "busy", 11, b"x").await;
    drop(client);
    node.shutdown();
    std::fs::remove_dir_all(disk.metadata_dir()).unwrap();
    let prefix = object_store::path::Path::from("checkpoints");
    let files: Vec<_> = disk
        .store
        .list(Some(&prefix))
        .map(|meta| meta.unwrap().location)
        .collect()
        .await;
    assert!(!files.is_empty());

    let (node, _) = disk.start(manual_config(1));
    // Removed only after the start has listed them, so that lookups find them gone for good.
    for file in &files {
        disk.store.inner_delete(file).await;
    }
    let mut client = node.client(0).await;
    let records = record_with(0, b"y");
    client
        .publish(publish("kept", &records, 2, 2))
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(30), client.recv()).await {
        Ok(Some(server::Reply::Nack { code, .. })) => {
            assert_eq!(code, proto::ErrorCode::RateLimited);
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    // Another stream the WAL does hold still answers.
    let busy = tokio::time::timeout(Duration::from_secs(30), client.stream_info(key("busy"))).await;
    assert!(busy.unwrap().is_ok());
    let info = tokio::time::timeout(Duration::from_secs(30), client.stream_info(key("kept"))).await;
    assert_eq!(info.unwrap().err(), Some(proto::ErrorCode::RateLimited));
    drop(client);
    node.shutdown();
}
