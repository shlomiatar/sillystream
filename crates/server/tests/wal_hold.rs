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

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

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
    connection.subscribe(Subscription::new(1, key("s"), 0));
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

/// Q15: a flipped bit in a closed file loses that file's later entries while later files replay.
/// That can't be repaired, but the start says so, and a torn newest file doesn't (D252).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn damage_in_a_closed_wal_file_is_reported_and_a_torn_newest_file_is_not() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ScriptStore::new());
    let clock = ManualClock::new(START_MS);
    let (node, _) = start(dir.path(), &store, &clock, manual_config(1));
    let mut client = node.client(0).await;
    for seq in 1..=3 {
        publish_at(&mut client, seq, &[seq as u8; 64], 2).await;
    }
    // The upload is held, so the file the seal closed stays.
    store.hold();
    let seal = tokio::spawn({
        let wal = dir.path().join("wal");
        async move {
            while storage::list_wal_files(&wal).unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });
    let mut sealing = Box::pin(node.seal());
    tokio::select! {
        _ = sealing.as_mut() => panic!("the upload is held"),
        _ = seal => {}
    }
    publish_at(&mut client, 4, &[4; 64], 2).await;
    let image = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), &image.path().join("wal"));
    copy_dir(&dir.path().join("metadata"), &image.path().join("metadata"));
    let files = storage::list_wal_files(&image.path().join("wal")).unwrap();
    assert_eq!(files.len(), 2);

    // With the newest file torn, nothing is said.
    let torn_image = tempfile::tempdir().unwrap();
    copy_dir(&image.path().join("wal"), &torn_image.path().join("wal"));
    copy_dir(
        &image.path().join("metadata"),
        &torn_image.path().join("metadata"),
    );
    let newest = torn_image
        .path()
        .join("wal")
        .join(files[1].path.file_name().unwrap());
    let mut bytes = std::fs::read(&newest).unwrap();
    let end = storage::replay_file(&files[1]).unwrap().valid_end_bytes;
    bytes[end - 1] ^= 1;
    std::fs::write(&newest, &bytes).unwrap();

    // The middle of the closed file: the second entry's data.
    let closed = &files[0].path;
    let mut bytes = std::fs::read(closed).unwrap();
    let at = bytes.windows(64).position(|run| run == [2u8; 64]).unwrap();
    bytes[at] ^= 1;
    std::fs::write(closed, &bytes).unwrap();

    store.release();
    drop(client);
    let _ = sealing.await;
    node.shutdown();

    let isolated = Arc::new(ScriptStore::new());
    let (node, report) = start(image.path(), &isolated, &clock, manual_config(1));
    assert_eq!(report.damaged_wal_seqs, [files[0].seq]);
    let warning = report.damaged_warning().unwrap();
    assert!(warning.contains("gap"), "{warning}");
    // What the damage hid is gone, and the later file still replayed: the gap Q15 describes.
    assert_eq!(report.replayed_entry_count, 2);
    node.shutdown();

    let isolated = Arc::new(ScriptStore::new());
    let (node, report) = start(torn_image.path(), &isolated, &clock, manual_config(1));
    assert_eq!(report.damaged_wal_seqs, Vec::<u64>::new());
    assert_eq!(report.damaged_warning(), None);
    node.shutdown();
}

/// A stream whose hashed shard starts on `core`, as placement is `shard % core_count`.
fn name_on_core(core: usize, node: &Node, prefix: &str) -> String {
    (0..)
        .map(|n| format!("{prefix}{n}"))
        .find(|name| {
            server::shard_of(key(name), node.virtual_shard_count()) as usize % node.core_count()
                == core
        })
        .expect("names go on for ever")
}

/// A producer's write parked while its stream loads, on a core the held WAL has stalled, and its
/// next write, which waits behind the stall: when the shard moves, the parked one still goes
/// first. The other way round, the later write was taken first and the parked one was dropped
/// as old.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_passes_on_writes_parked_for_a_load_before_later_ones() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ScriptStore::new());
    let clock = ManualClock::new(START_MS);
    let mut config = manual_config(2);
    config.wal_channel_capacity = 2;
    let (node, _) = start(dir.path(), &store, &clock, config.clone());
    let name = name_on_core(1, &node, "m");
    let shard = server::shard_of(key(&name), node.virtual_shard_count());
    let records = record_with(0, b"x");
    let mut client = node.client(1).await;
    client
        .publish(publish(&name, &records, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    assert!(node.seal_and_checkpoint().await);
    // Long enough that the WAL no longer holds it, so that only a checkpoint can load it.
    let busy = name_on_core(1, &node, "busy");
    for seq in 1..=10 {
        client
            .publish(publish(&busy, &records, seq, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        assert!(node.seal_and_checkpoint().await);
    }
    drop(client);
    node.shutdown();
    std::fs::remove_dir_all(dir.path().join("metadata")).unwrap();

    let (node, _) = start(dir.path(), &store, &clock, config);
    // Its routes come from now, with the stream's shard on core 1, so core 1 passes its writes
    // on and core 0 reads them from its forwarded inbox.
    let mut producer = node.client(1).await;
    assert!(node.move_shard(shard, 0).await);
    let filler = name_on_core(0, &node, "filler");
    let mut stall = node.client(0).await;
    stall
        .publish(publish(&filler, &records, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut stall).await;

    // The stream's load waits on this, and the write parks.
    store
        .slow_get_once
        .lock()
        .unwrap()
        .push((".ckpt".to_owned(), Duration::from_millis(3_000)));
    producer
        .publish(publish(&name, &records, 2, 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    storage::hold_wal_writes(true);
    let mut seq = 2;
    while node.stats(0).await.unwrap().outbound_count == 0 {
        assert!(seq < 100, "the channel should have filled by now");
        stall
            .publish(publish(&filler, &records, seq, 1))
            .await
            .unwrap();
        seq += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Deferred, as core 0 is stalled. A sleep too short here or above only lets this pass
    // without testing anything, never fail.
    producer
        .publish(publish(&name, &records, 3, 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(node.move_shard(shard, 1).await);
    storage::hold_wal_writes(false);

    let mut acks = [
        expect_ack(&mut producer).await,
        expect_ack(&mut producer).await,
    ];
    acks.sort();
    let [(_, parked_id, _), (_, later_id, _)] = acks;
    assert!(parked_id > 0, "the parked write was dropped as old");
    assert!(parked_id < later_id, "{parked_id} after {later_id}");
    drop((producer, stall));
    node.shutdown();
}
