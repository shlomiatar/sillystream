//! Dedupe entries (SPEC section 3): one per `(stream, producer_id)`, 28 bytes, holding
//! the highest `producer_seq` with its `first_id` and `count` (D6e).

use format::id_ms;

pub const DEDUPE_CAP_COUNT: usize = 1000;
pub const DEDUPE_IDLE_MS: u64 = 24 * 60 * 60 * 1000;
pub const DEDUPE_ENTRY_BYTES: usize = 28;

/// Packed so that `Vec<DedupeEntry>` really costs 28 bytes an entry. Fields are read by
/// value, so no reference to a packed field is ever made.
#[repr(C, packed(4))]
#[derive(Debug, Clone, Copy)]
pub struct DedupeEntry {
    producer_id: u64,
    producer_seq: u64,
    first_id: u64,
    count: u32,
}

impl DedupeEntry {
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }

    pub fn producer_seq(&self) -> u64 {
        self.producer_seq
    }

    pub fn first_id(&self) -> u64 {
        self.first_id
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    /// Idle age comes from `first_id`'s timestamp (D6e).
    fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= id_ms(self.first_id).saturating_add(DEDUPE_IDLE_MS)
    }
}

/// What to do with a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupeCheck {
    /// Not seen, or its entry has expired.
    New,
    /// A retry of the latest publish: send the original `ACK`.
    Latest { first_id: u64, count: u32 },
    /// A retry of an older publish: `ACK` with `first_id = 0`. `latest_last_id` is the
    /// last id of the producer's latest publish, which came after the older one, and
    /// `latest_seq` is that publish's seq.
    Older {
        latest_last_id: u64,
        latest_seq: u64,
    },
}

/// Sorted by `producer_id`, so a lookup is a binary search over at most 1,000 entries,
/// and only a new producer moves anything.
#[derive(Debug, Clone, Default)]
pub struct DedupeTable {
    entries: Vec<DedupeEntry>,
}

impl DedupeTable {
    pub fn new() -> DedupeTable {
        DedupeTable::default()
    }

    /// Entries as they were saved, in any order.
    pub fn from_entries(entries: impl IntoIterator<Item = (u64, u64, u64, u32)>) -> DedupeTable {
        let mut entries: Vec<DedupeEntry> = entries
            .into_iter()
            .map(|(producer_id, producer_seq, first_id, count)| DedupeEntry {
                producer_id,
                producer_seq,
                first_id,
                count,
            })
            .collect();
        entries.sort_by_key(DedupeEntry::producer_id);
        DedupeTable { entries }
    }

    pub fn entries(&self) -> &[DedupeEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn position(&self, producer_id: u64) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&producer_id, DedupeEntry::producer_id)
    }

    /// An expired entry is removed here, so it can't answer again.
    pub fn check(&mut self, producer_id: u64, producer_seq: u64, now_ms: u64) -> DedupeCheck {
        let Ok(at) = self.position(producer_id) else {
            return DedupeCheck::New;
        };
        let entry = self.entries[at];
        if entry.is_expired(now_ms) {
            self.entries.remove(at);
            return DedupeCheck::New;
        }
        if producer_seq > entry.producer_seq() {
            DedupeCheck::New
        } else if producer_seq == entry.producer_seq() {
            DedupeCheck::Latest {
                first_id: entry.first_id(),
                count: entry.count(),
            }
        } else {
            DedupeCheck::Older {
                latest_last_id: entry.first_id() + u64::from(entry.count()) - 1,
                latest_seq: entry.producer_seq(),
            }
        }
    }

    /// Records a committed publish. At the cap, expired entries go first and then the
    /// oldest, which is the lowest `first_id` since ids follow ingest time.
    pub fn record(
        &mut self,
        producer_id: u64,
        producer_seq: u64,
        first_id: u64,
        count: u32,
        now_ms: u64,
    ) {
        let entry = DedupeEntry {
            producer_id,
            producer_seq,
            first_id,
            count,
        };
        match self.position(producer_id) {
            Ok(at) => self.entries[at] = entry,
            Err(_) => {
                if self.entries.len() >= DEDUPE_CAP_COUNT {
                    self.entries.retain(|entry| !entry.is_expired(now_ms));
                }
                if self.entries.len() >= DEDUPE_CAP_COUNT
                    && let Some((oldest, _)) = self
                        .entries
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, entry)| entry.first_id())
                {
                    self.entries.remove(oldest);
                }
                // The removals may have moved its place.
                if let Err(at) = self.position(producer_id) {
                    self.entries.insert(at, entry);
                }
            }
        }
    }
}
