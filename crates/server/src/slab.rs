//! Slabs (SPEC section 6.5): large allocations that records live in, so a record is
//! allocated once on its way from the socket to S3.
//!
//! A slab is a `BytesMut` split into exclusive regions, which share one reference count
//! per allocation, and stay writable until they are frozen. So stamping in place needs
//! no `unsafe`. Everything downstream (WAL, uploader, the core's own log) holds a
//! `SlabBytes` per batch, and nothing here has a count per record.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};

/// Watches reference counts, for tests and the `write_path` bench. Off in normal use.
pub trait SlabHook: Send + Sync {
    /// A `SlabBytes` was made (+1) or dropped (-1).
    fn ref_changed(&self, slab_id: u64, delta: i32);
    /// The last reference to the slab has gone.
    fn slab_freed(&self, slab_id: u64);
}

/// How many bytes of slabs one pool has alive, wherever their references are. Shared
/// with the threads that drop references, and read for the memory cap (D7).
#[derive(Debug, Clone, Default)]
pub struct SlabAccount {
    pinned_bytes: Arc<AtomicU64>,
}

impl SlabAccount {
    pub fn new() -> SlabAccount {
        SlabAccount::default()
    }

    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_bytes.load(Ordering::Relaxed)
    }
}

struct SlabTracker {
    slab_id: u64,
    capacity_bytes: u64,
    account: SlabAccount,
    hook: Option<Arc<dyn SlabHook>>,
}

impl Drop for SlabTracker {
    fn drop(&mut self) {
        self.account
            .pinned_bytes
            .fetch_sub(self.capacity_bytes, Ordering::Relaxed);
        if let Some(hook) = &self.hook {
            hook.slab_freed(self.slab_id);
        }
    }
}

/// An exclusive, writable part of a slab.
pub struct SlabRegion {
    bytes: BytesMut,
    tracker: Arc<SlabTracker>,
}

impl SlabRegion {
    /// Makes it shareable. The memory doesn't move.
    pub fn freeze(self) -> SlabBytes {
        if let Some(hook) = &self.tracker.hook {
            hook.ref_changed(self.tracker.slab_id, 1);
        }
        SlabBytes {
            bytes: self.bytes.freeze(),
            tracker: self.tracker,
        }
    }
}

impl Deref for SlabRegion {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl DerefMut for SlabRegion {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl std::fmt::Debug for SlabRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlabRegion")
            .field("slab_id", &self.tracker.slab_id)
            .field("len_bytes", &self.bytes.len())
            .finish()
    }
}

/// `(slab, offset, len)` of frozen bytes. Cloning changes the slab's count, and is done
/// per batch.
pub struct SlabBytes {
    bytes: Bytes,
    tracker: Arc<SlabTracker>,
}

impl SlabBytes {
    pub fn slab_id(&self) -> u64 {
        self.tracker.slab_id
    }

    /// Which allocation this keeps alive, and how big it is. Unlike `slab_id`, the identity
    /// holds across pools.
    pub(crate) fn held(&self) -> (usize, u64) {
        (
            Arc::as_ptr(&self.tracker) as usize,
            self.tracker.capacity_bytes,
        )
    }
}

impl Clone for SlabBytes {
    fn clone(&self) -> SlabBytes {
        if let Some(hook) = &self.tracker.hook {
            hook.ref_changed(self.tracker.slab_id, 1);
        }
        SlabBytes {
            bytes: self.bytes.clone(),
            tracker: Arc::clone(&self.tracker),
        }
    }
}

impl Drop for SlabBytes {
    fn drop(&mut self) {
        if let Some(hook) = &self.tracker.hook {
            hook.ref_changed(self.tracker.slab_id, -1);
        }
    }
}

impl AsRef<[u8]> for SlabBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Deref for SlabBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for SlabBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlabBytes")
            .field("slab_id", &self.tracker.slab_id)
            .field("len_bytes", &self.bytes.len())
            .finish()
    }
}

struct CurrentSlab {
    rest: BytesMut,
    tracker: Arc<SlabTracker>,
}

/// Hands out regions from one slab until it is used up. Owned by one core or connection.
pub struct SlabPool {
    slab_bytes: usize,
    account: SlabAccount,
    hook: Option<Arc<dyn SlabHook>>,
    /// Slab ids only have to be unique for one hook.
    id_base: u64,
    slab_count: u64,
    current: Option<CurrentSlab>,
}

impl SlabPool {
    pub fn new(
        slab_bytes: usize,
        account: SlabAccount,
        hook: Option<Arc<dyn SlabHook>>,
        id_base: u64,
    ) -> SlabPool {
        SlabPool {
            slab_bytes,
            account,
            hook,
            id_base,
            slab_count: 0,
            current: None,
        }
    }

    fn new_slab(&mut self, capacity_bytes: usize) -> CurrentSlab {
        self.account
            .pinned_bytes
            .fetch_add(capacity_bytes as u64, Ordering::Relaxed);
        let slab_id = self.id_base + self.slab_count;
        self.slab_count += 1;
        CurrentSlab {
            // Zeroed from the allocator, so this doesn't touch the pages.
            rest: BytesMut::zeroed(capacity_bytes),
            tracker: Arc::new(SlabTracker {
                slab_id,
                capacity_bytes: capacity_bytes as u64,
                account: self.account.clone(),
                hook: self.hook.clone(),
            }),
        }
    }

    /// An exclusive region of exactly `len_bytes`, starting at a multiple of 8 from the
    /// slab's start. A publish bigger than a slab gets a slab of its own.
    pub fn alloc(&mut self, len_bytes: usize) -> SlabRegion {
        let take_bytes = len_bytes.next_multiple_of(8);
        if take_bytes > self.slab_bytes {
            let slab = self.new_slab(take_bytes);
            return SlabRegion {
                bytes: slab.rest,
                tracker: slab.tracker,
            }
            .truncated(len_bytes);
        }
        if self
            .current
            .as_ref()
            .is_none_or(|slab| slab.rest.len() < take_bytes)
        {
            self.current = Some(self.new_slab(self.slab_bytes));
        }
        let Some(slab) = self.current.as_mut() else {
            unreachable!("a slab was just made");
        };
        let bytes = slab.rest.split_to(take_bytes);
        SlabRegion {
            bytes,
            tracker: Arc::clone(&slab.tracker),
        }
        .truncated(len_bytes)
    }

    /// A region holding a copy of `bytes`: what reading from a socket does.
    pub fn copy_in(&mut self, bytes: &[u8]) -> SlabRegion {
        let mut region = self.alloc(bytes.len());
        region.copy_from_slice(bytes);
        region
    }

    /// Lets go of the slab being filled, so it can be freed once its references go.
    pub fn retire_current(&mut self) {
        self.current = None;
    }

    pub fn account(&self) -> &SlabAccount {
        &self.account
    }
}

impl SlabRegion {
    fn truncated(mut self, len_bytes: usize) -> SlabRegion {
        self.bytes.truncate(len_bytes);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct Recording {
        changes: Mutex<Vec<(u64, i32)>>,
        freed: Mutex<Vec<u64>>,
    }

    impl SlabHook for Recording {
        fn ref_changed(&self, slab_id: u64, delta: i32) {
            self.changes.lock().unwrap().push((slab_id, delta));
        }
        fn slab_freed(&self, slab_id: u64) {
            self.freed.lock().unwrap().push(slab_id);
        }
    }

    fn pool(slab_bytes: usize) -> (SlabPool, Arc<Recording>) {
        let hook = Arc::new(Recording::default());
        let dyn_hook: Arc<dyn SlabHook> = hook.clone();
        (
            SlabPool::new(slab_bytes, SlabAccount::new(), Some(dyn_hook), 0),
            hook,
        )
    }

    #[test]
    fn regions_are_exclusive_and_freezing_does_not_move_them() {
        let (mut pool, _hook) = pool(1024);
        let mut a = pool.alloc(100);
        let mut b = pool.alloc(100);
        a.fill(1);
        b.fill(2);
        let a_ptr = a.as_ptr();
        // Rounded up to 8 and contiguous, so these are one allocation.
        assert_eq!(b.as_ptr() as usize - a_ptr as usize, 104);
        let frozen = a.freeze();
        assert_eq!(frozen.as_ptr(), a_ptr);
        assert!(frozen.iter().all(|byte| *byte == 1));
        assert!(b.iter().all(|byte| *byte == 2));
    }

    #[test]
    fn a_slab_is_freed_once_after_the_last_reference() {
        let (mut pool, hook) = pool(1024);
        let account = pool.account().clone();
        let a = pool.copy_in(&[1; 64]).freeze();
        let b = pool.copy_in(&[2; 64]).freeze();
        let c = a.clone();
        assert_eq!(account.pinned_bytes(), 1024);
        pool.retire_current();
        drop(a);
        drop(b);
        assert!(hook.freed.lock().unwrap().is_empty());
        // The bytes stay good with the rest of the slab's references gone.
        assert_eq!(&c[..4], &[1; 4]);
        drop(c);
        assert_eq!(*hook.freed.lock().unwrap(), [0]);
        assert_eq!(account.pinned_bytes(), 0);
    }

    #[test]
    fn a_full_slab_moves_on_and_an_oversized_region_gets_its_own() {
        let (mut pool, hook) = pool(128);
        let a = pool.alloc(128).freeze();
        let b = pool.alloc(8).freeze();
        assert_ne!(a.slab_id(), b.slab_id());
        let big = pool.alloc(1000).freeze();
        assert_eq!(big.len(), 1000);
        let c = pool.alloc(8).freeze();
        // The oversized slab didn't take the place of the one being filled.
        assert_eq!(c.slab_id(), b.slab_id());
        assert_eq!(
            pool.account().pinned_bytes(),
            128 + 128 + 1000_u64.next_multiple_of(8)
        );
        drop(big);
        assert_eq!(hook.freed.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_rejected_region_frees_its_slab_too() {
        let (mut pool, hook) = pool(64);
        let region = pool.alloc(8);
        pool.retire_current();
        drop(region);
        assert_eq!(*hook.freed.lock().unwrap(), [0]);
    }

    #[test]
    fn an_empty_region_is_allowed() {
        let (mut pool, _hook) = pool(64);
        assert_eq!(pool.alloc(0).freeze().len(), 0);
    }
}
