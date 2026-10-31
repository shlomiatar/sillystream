//! Ids: high 48 bits are ingest time in ms, low 16 bits a counter (SPEC section 2).

use crate::FormatError;

pub const COUNTER_BITS: u32 = 16;
/// The highest ms a 48-bit field holds.
pub const MAX_ID_MS: u64 = (1 << 48) - 1;

/// How far past the clock ids may go (D221). Above the 65,536 ms that the largest publish D34
/// allows spans, so that any publish can go through once the stream is near the clock.
pub const MAX_ID_LEAD_MS: u64 = 70_000;
/// What a restarted stream may borrow above its floor while the clock catches up to the floor.
pub const RESTART_ID_CREDIT_MS: u64 = 1_000;

pub fn make_id(ms: u64, counter: u16) -> u64 {
    (ms << COUNTER_BITS) | u64::from(counter)
}

pub fn id_ms(id: u64) -> u64 {
    id >> COUNTER_BITS
}

pub fn id_counter(id: u64) -> u16 {
    id as u16
}

/// Assigns ids for one stream. Owned by the stream's core.
///
/// The next id is `max(last_id + 1, now_ms << 16, last_valid_id + 1)` (D15).
/// Items in one allocation are consecutive, so running out of a ms's counter
/// values carries into the next ms: that is the borrowing the spec describes,
/// and the clock catches up later. A frozen or backward clock changes nothing.
/// `allocate` itself has no limit on borrowing: the owner asks `fits_lead` first (D221).
#[derive(Debug, Clone)]
pub struct IdAllocator {
    last_id: u64,
    last_valid_id: u64,
}

impl IdAllocator {
    /// `last_id` is the stream's highest assigned id, and `last_valid_id` the
    /// highest id a former generation could have issued. Both are 0 if none.
    pub fn new(last_id: u64, last_valid_id: u64) -> IdAllocator {
        IdAllocator {
            last_id,
            last_valid_id,
        }
    }

    pub fn last_id(&self) -> u64 {
        self.last_id
    }

    /// Whether `item_count` more ids stay within `MAX_ID_LEAD_MS` of `clock_ms`, or within
    /// `RESTART_ID_CREDIT_MS` of the floor a restart set. What `allocate` would refuse anyway
    /// counts as fitting, so that it gets `allocate`'s error.
    pub fn fits_lead(&self, clock_ms: u64, item_count: u64) -> bool {
        let clock_id = clock_ms.min(MAX_ID_MS) << COUNTER_BITS;
        let first_id = self
            .last_id
            .saturating_add(1)
            .max(clock_id)
            .max(self.last_valid_id.saturating_add(1));
        let Some(last_id) = first_id.checked_add(item_count.saturating_sub(1)) else {
            return true;
        };
        let limit_ms = clock_ms
            .saturating_add(MAX_ID_LEAD_MS)
            .max(id_ms(self.last_valid_id).saturating_add(RESTART_ID_CREDIT_MS));
        id_ms(last_id) <= limit_ms
    }

    /// Reserves `item_count` consecutive ids and returns the first.
    ///
    /// `FormatError::ZeroCount` for no items. `FormatError::TooManyItems` if the
    /// id space is exhausted: `u64::MAX` is the live-only cursor position
    /// (section 5.1), so no item may have that id.
    pub fn allocate(&mut self, now_ms: u64, item_count: u64) -> Result<u64, FormatError> {
        if item_count == 0 {
            return Err(FormatError::ZeroCount);
        }
        let clock_id = now_ms.min(MAX_ID_MS) << COUNTER_BITS;
        let (Some(after_last), Some(after_valid)) = (
            self.last_id.checked_add(1),
            self.last_valid_id.checked_add(1),
        ) else {
            return Err(FormatError::TooManyItems);
        };
        let first_id = after_last.max(clock_id).max(after_valid);
        let last_id = match first_id.checked_add(item_count - 1) {
            Some(last_id) if last_id < u64::MAX => last_id,
            _ => return Err(FormatError::TooManyItems),
        };
        self.last_id = last_id;
        Ok(first_id)
    }
}
