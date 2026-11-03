//! Reference counts change per slab per batch, not per record (SPEC section 6.5), and a
//! slab is freed once, when the last reference has gone.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use server::{SealOutcome, SlabHook};

#[derive(Default)]
struct Counting {
    changes: AtomicU64,
    live: Mutex<HashMap<u64, i64>>,
    freed: Mutex<HashMap<u64, u32>>,
    balance: AtomicI64,
}

impl SlabHook for Counting {
    fn ref_changed(&self, slab_id: u64, delta: i32) {
        self.changes.fetch_add(1, Ordering::SeqCst);
        self.balance.fetch_add(i64::from(delta), Ordering::SeqCst);
        *self.live.lock().unwrap().entry(slab_id).or_default() += i64::from(delta);
    }

    fn slab_freed(&self, slab_id: u64) {
        *self.freed.lock().unwrap().entry(slab_id).or_default() += 1;
    }
}

/// The reference count changes one level 3 publish of `record_count` records causes, from
/// its arrival to the slab being let go.
async fn changes_for_one_batch(record_count: usize) -> (u64, Arc<Counting>) {
    let hook = Arc::new(Counting::default());
    let mut config = manual_config(1);
    config.slab_bytes = 4 * 1024 * 1024;
    let dyn_hook: Arc<dyn SlabHook> = hook.clone();
    config.slab_hook = Some(dyn_hook);
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    let mut records = Vec::new();
    for index in 0..record_count {
        add_record(&mut records, index as u64, &[1; 16], 1);
    }
    client.publish(publish("s", &records, 1, 3)).await.unwrap();
    assert!(matches!(
        seal_published(harness.node()).await,
        SealOutcome::Uploaded { .. }
    ));
    expect_ack(&mut client).await;
    let exit = harness.finish();
    assert!(exit.wal.is_ok());
    (hook.changes.load(Ordering::SeqCst), hook)
}

#[tokio::test]
async fn count_changes_depend_on_batches_and_not_on_records() {
    let (one, _) = changes_for_one_batch(1).await;
    let (hundred, _) = changes_for_one_batch(100).await;
    let (many, _) = changes_for_one_batch(10_000).await;
    assert_eq!(one, hundred);
    assert_eq!(one, many);
    // The count itself is a small constant: the WAL's, the log's, the section's and the
    // uploader's references, each made and dropped.
    assert!(one <= 16, "{one}");
}

#[tokio::test]
async fn every_reference_is_dropped_and_each_slab_freed_exactly_once() {
    let hook = Arc::new(Counting::default());
    let mut config = manual_config(2);
    config.slab_bytes = 4096;
    let dyn_hook: Arc<dyn SlabHook> = hook.clone();
    config.slab_hook = Some(dyn_hook);
    let harness = Harness::new(config);
    let mut client = harness.node().client(0).await;
    let records = record_with(1, &[7; 1000]);
    let mut seq = 1;
    for stream in ["a", "b", "c", "d"] {
        for _ in 0..20 {
            client
                .publish(publish(stream, &records, seq, 2))
                .await
                .unwrap();
            expect_ack(&mut client).await;
            seq += 1;
        }
    }
    let live_before: usize = hook.freed.lock().unwrap().len();
    assert!(matches!(
        seal_published(harness.node()).await,
        SealOutcome::Uploaded { .. }
    ));
    client.retire_slab();

    // Slabs that aren't still being filled go without the node stopping: that is what
    // lets level 3 and the memory cap work.
    let wait_until = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while hook.freed.lock().unwrap().len() <= live_before {
        assert!(std::time::Instant::now() < wait_until, "no slab was freed");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let pinned_now = client.pinned_bytes();
    assert_eq!(pinned_now % 4096, 0);
    drop(client);
    harness.finish();

    assert_eq!(hook.balance.load(Ordering::SeqCst), 0);
    let live = hook.live.lock().unwrap();
    let freed = hook.freed.lock().unwrap();
    assert!(!freed.is_empty());
    for (slab_id, count) in live.iter() {
        assert_eq!(*count, 0, "slab {slab_id}");
        assert_eq!(freed.get(slab_id), Some(&1), "slab {slab_id}");
    }
    assert!(freed.values().all(|count| *count == 1));
}

#[tokio::test]
async fn nothing_in_the_write_path_copies_a_plain_publish() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let records = record_with(1, &[9; 500]);
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    expect_ack(&mut client).await;
    client.publish(publish("s", &records, 2, 1)).await.unwrap();
    expect_ack(&mut client).await;
    // Neighbours in one slab: the core kept the bytes where the client read them to.
    let batches = harness.node().read_memory(0, "s").await;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].slab_id(), batches[1].slab_id());
    let gap = batches[1].as_ptr() as usize - batches[0].as_ptr() as usize;
    assert_eq!(gap, batches[0].len());
    harness.finish();
}
