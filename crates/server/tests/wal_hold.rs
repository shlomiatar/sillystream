//! What a checkpoint saves while the WAL is behind it (D136, D154). These stall the WAL writer of
//! the whole process, so they live in a binary of their own and take turns.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use server::{ManualClock, Node, NodeConfig, RecoverConfig, RecoveryReport, Subscription};
use tokio::sync::Mutex;

static TURN: Mutex<()> = Mutex::const_new(());

fn start(
    dir: &Path,
    store: &Arc<ScriptStore>,
    clock: &ManualClock,
    config: NodeConfig,
) -> (Node, RecoveryReport) {
    Node::recover(
        config,
        RecoverConfig {
            wal_dir: dir.join("wal"),
            metadata_dir: dir.join("metadata"),
            wal_preallocate_bytes: 1024 * 1024,
        },
        store.clone(),
        Arc::new(clock.clone()),
    )
    .unwrap()
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for item in std::fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        let target = to.join(item.file_name());
        if item.file_type().unwrap().is_dir() {
            copy_dir(&item.path(), &target);
        } else {
            std::fs::copy(item.path(), target).unwrap();
        }
    }
}

async fn publish_at(client: &mut server::Client, seq: u64, data: &[u8], level: u8) -> (u64, u64) {
    let records = record_with(0, data);
    client
        .publish(publish("s", &records, seq, level))
        .await
        .unwrap();
    let (_, first_id, _) = expect_ack(client).await;
    (seq, first_id)
}

async fn read_ids(node: &Node, want: usize) -> Vec<u64> {
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, "s", 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= want
    })
    .await;
    hear_for(&mut connection, &mut heard, 100).await;
    heard.records(1).iter().map(|record| record.0).collect()
}

/// Publishes seq 2 at level 1 with the WAL stalled, during the upload of a seal, so that the
/// seal's checkpoint runs with that publish visible and not in the file. The WAL stays stalled.
async fn checkpoint_behind_the_wal(
    node: &Node,
    store: &ScriptStore,
    clock: &ManualClock,
    client: &mut server::Client,
) -> u64 {
    store.hold();
    let begun = store.put_begun_count.load(Ordering::SeqCst);
    let window = async {
        while store.put_begun_count.load(Ordering::SeqCst) == begun {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        storage::hold_wal_writes(true);
        clock.advance(1);
        let (_, second) = publish_at(client, 2, b"second", 1).await;
        store.release();
        second
    };
    let (sealed, second) = tokio::join!(node.seal_and_checkpoint(), window);
    assert!(sealed);
    second
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_a_crash_writes_what_a_checkpoint_saw_only_in_memory() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ScriptStore::new());
    let clock = ManualClock::new(START_MS);
    let (node, _) = start(dir.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    let (_, first) = publish_at(&mut client, 1, b"first", 2).await;
    checkpoint_behind_the_wal(&node, &store, &clock, &mut client).await;

    // What the disks hold at this moment is what a crash would leave.
    let image = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), &image.path().join("wal"));
    copy_dir(&dir.path().join("metadata"), &image.path().join("metadata"));
    let on_disk = storage::replay_dir(&image.path().join("wal")).unwrap();
    assert!(on_disk.iter().all(|entry| entry.producer_seq != 2));
    storage::hold_wal_writes(false);
    drop(client);
    node.shutdown();

    clock.advance(1_000);
    let (node, _) = start(image.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    let (_, retried) = publish_at(&mut client, 2, b"second", 2).await;
    assert_eq!(read_ids(&node, 2).await, [first, retried]);
    drop(client);
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_durable_after_the_checkpoint_still_dedupes_after_a_crash() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ScriptStore::new());
    let clock = ManualClock::new(START_MS);
    let (node, _) = start(dir.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    let (_, first) = publish_at(&mut client, 1, b"first", 2).await;
    let second = checkpoint_behind_the_wal(&node, &store, &clock, &mut client).await;
    storage::hold_wal_writes(false);
    // In the file now, with nothing since that saves metadata.
    clock.advance(1);
    let (_, third) = publish_at(&mut client, 3, b"third", 2).await;
    drop(client);
    node.shutdown();

    clock.advance(1_000);
    let (node, _) = start(dir.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    let (_, retried) = publish_at(&mut client, 3, b"third", 2).await;
    assert_eq!(retried, third, "the retry gets its original ack");
    let (_, older) = publish_at(&mut client, 2, b"second", 2).await;
    assert_eq!(older, 0, "an older retry is already committed");
    assert_eq!(read_ids(&node, 3).await, [first, second, third]);
    drop(client);
    node.shutdown();
}

/// Level 1 ids borrowed ahead of the clock, delivered, then erased by a crash. Section 2 says
/// ids are never reused, and a reader's cursor means "after this id", so a new record must land
/// above them whether or not anything durable shows they existed (D221).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ids_stay_above_what_a_crash_erased_after_being_delivered() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ScriptStore::new());
    let clock = ManualClock::new(START_MS);
    let (node, _) = start(dir.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    publish_at(&mut client, 1, b"durable", 2).await;

    storage::hold_wal_writes(true);
    let mut records = Vec::new();
    for _ in 0..3_000 {
        add_record(&mut records, 0, b"", u16::MAX);
    }
    client.publish(publish("s", &records, 2, 1)).await.unwrap();
    let (_, first_id, count) = expect_ack(&mut client).await;
    let erased_through = first_id + u64::from(count) - 1;
    assert!(format::id_ms(erased_through) > START_MS + 2_000, "ahead");
    // A reader has it, while no disk does.
    let delivered = read_ids(&node, 3_001).await;
    assert_eq!(delivered.last(), Some(&(erased_through - 65_534)));
    let image = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), &image.path().join("wal"));
    copy_dir(&dir.path().join("metadata"), &image.path().join("metadata"));
    storage::hold_wal_writes(false);
    drop(client);
    node.shutdown();

    // Back before the clock has caught up with those ids.
    clock.advance(100);
    let (node, _) = start(image.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    let (_, new_id) = publish_at(&mut client, 3, b"after", 2).await;
    assert!(
        new_id > erased_through,
        "new id {new_id} reuses what was delivered through {erased_through}"
    );
    drop(client);
    node.shutdown();
}
