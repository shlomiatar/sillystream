//! The top indexes and index pages readers have fetched, kept up to a byte cap and evicted
//! least recently used first (section 7.3).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use object_store::path::Path;
use storage::{IndexEntry, TopIndex};

/// What the cache holds by default.
pub(crate) const PAGE_CACHE_BYTES: u64 = 64 << 20;

/// About what a top index entry or a page entry costs in memory, and what any item costs.
const TOP_ENTRY_BYTES: u64 = 40;
const PAGE_ENTRY_BYTES: u64 = 24;
const ITEM_BYTES: u64 = 96;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Key {
    Top(Path),
    Page(Path, usize),
}

#[derive(Clone)]
enum Item {
    Top(Arc<TopIndex>),
    Page(Arc<Vec<IndexEntry>>),
}

#[derive(Default)]
struct Inner {
    items: HashMap<Key, (Item, u64, u64)>,
    /// Last use to key, oldest first.
    used: BTreeMap<u64, Key>,
    bytes: u64,
    tick: u64,
}

pub(crate) struct PageCache {
    cap_bytes: u64,
    inner: Mutex<Inner>,
}

impl PageCache {
    pub fn new(cap_bytes: u64) -> PageCache {
        PageCache {
            cap_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn top(&self, path: &Path) -> Option<Arc<TopIndex>> {
        match self.get(Key::Top(path.clone()))? {
            Item::Top(top) => Some(top),
            Item::Page(_) => None,
        }
    }

    pub fn page(&self, path: &Path, number: usize) -> Option<Arc<Vec<IndexEntry>>> {
        match self.get(Key::Page(path.clone(), number))? {
            Item::Page(page) => Some(page),
            Item::Top(_) => None,
        }
    }

    pub fn put_top(&self, path: &Path, top: Arc<TopIndex>) {
        let bytes = ITEM_BYTES + top.page_count() as u64 * TOP_ENTRY_BYTES;
        self.put(Key::Top(path.clone()), Item::Top(top), bytes);
    }

    pub fn put_page(&self, path: &Path, number: usize, page: Arc<Vec<IndexEntry>>) {
        let bytes = ITEM_BYTES + page.len() as u64 * PAGE_ENTRY_BYTES;
        self.put(Key::Page(path.clone(), number), Item::Page(page), bytes);
    }

    fn get(&self, key: Key) -> Option<Item> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.tick += 1;
        let tick = inner.tick;
        let (item, _, used) = inner.items.get_mut(&key)?;
        let item = item.clone();
        let before = std::mem::replace(used, tick);
        inner.used.remove(&before);
        inner.used.insert(tick, key);
        Some(item)
    }

    fn put(&self, key: Key, item: Item, bytes: u64) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.tick += 1;
        let tick = inner.tick;
        if let Some((_, old_bytes, used)) = inner.items.insert(key.clone(), (item, bytes, tick)) {
            inner.used.remove(&used);
            inner.bytes -= old_bytes;
        }
        inner.used.insert(tick, key);
        inner.bytes += bytes;
        while inner.bytes > self.cap_bytes {
            let Some((_, oldest)) = inner.used.pop_first() else {
                break;
            };
            if let Some((_, old_bytes, _)) = inner.items.remove(&oldest) {
                inner.bytes -= old_bytes;
            }
        }
    }
}
