//! Id assignment (SPEC section 2, BUILD_NOTES D15).

use format::{FormatError, IdAllocator, MAX_ID_MS, id_counter, id_ms, make_id};
use proptest::prelude::*;

const ALLOCATIONS: u64 = 1_000_000;
const START_MS: u64 = 1_800_000_000_000;

/// Allocates `ALLOCATIONS` times with a scripted clock, checks that every item id
/// is above every earlier one, and returns the last id.
fn run(item_count: u64, clock: impl Fn(u64) -> u64) -> u64 {
    let mut allocator = IdAllocator::new(0, 0);
    let mut last_id = 0;
    for step in 0..ALLOCATIONS {
        let first_id = allocator.allocate(clock(step), item_count).unwrap();
        assert!(
            first_id > last_id,
            "step {step}: {first_id} after {last_id}"
        );
        last_id = first_id + item_count - 1;
        assert_eq!(allocator.last_id(), last_id);
    }
    last_id
}

#[test]
fn frozen_clock_single_items() {
    let last_id = run(1, |_| START_MS);
    assert_eq!(last_id, make_id(START_MS, 0) + ALLOCATIONS - 1);
    // 1M items don't fit in one ms's 65,536 counter values, so ms was borrowed.
    assert_eq!(id_ms(last_id), START_MS + (ALLOCATIONS - 1) / 65_536);
}

#[test]
fn backward_clock_single_items() {
    let last_id = run(1, |step| START_MS - step);
    assert_eq!(last_id, make_id(START_MS, 0) + ALLOCATIONS - 1);
}

#[test]
fn frozen_clock_full_counter_records() {
    let last_id = run(65_535, |_| START_MS);
    assert_eq!(last_id, make_id(START_MS, 0) + ALLOCATIONS * 65_535 - 1);
    assert!(id_ms(last_id) > START_MS + 900_000);
}

#[test]
fn backward_clock_full_counter_records() {
    let last_id = run(65_535, |step| START_MS - step);
    assert_eq!(last_id, make_id(START_MS, 0) + ALLOCATIONS * 65_535 - 1);
}

#[test]
fn high_bits_borrow_forward_at_exactly_the_counter_limit() {
    let mut allocator = IdAllocator::new(0, 0);
    for counter in 0..=u16::MAX {
        let id = allocator.allocate(START_MS, 1).unwrap();
        assert_eq!(id_ms(id), START_MS);
        assert_eq!(id_counter(id), counter);
    }
    // The ms is out of counter values, and the clock hasn't moved.
    let borrowed = allocator.allocate(START_MS, 1).unwrap();
    assert_eq!(borrowed, make_id(START_MS + 1, 0));
}

#[test]
fn a_record_can_span_a_borrowed_ms() {
    let mut allocator = IdAllocator::new(0, 0);
    let first_id = allocator.allocate(START_MS, 65_535).unwrap();
    assert_eq!(first_id, make_id(START_MS, 0));
    let second_id = allocator.allocate(START_MS, 65_535).unwrap();
    assert_eq!(second_id, make_id(START_MS, 65_535));
    assert_eq!(id_ms(allocator.last_id()), START_MS + 1);
    assert_eq!(id_counter(allocator.last_id()), 65_533);
}

#[test]
fn the_clock_takes_over_once_it_passes_the_borrowed_ms() {
    let mut allocator = IdAllocator::new(0, 0);
    allocator.allocate(START_MS, 200_000).unwrap();
    assert!(id_ms(allocator.last_id()) > START_MS);
    assert_eq!(
        allocator.allocate(START_MS + 100, 1).unwrap(),
        make_id(START_MS + 100, 0)
    );
}

#[test]
fn ids_stay_above_the_former_generation() {
    let last_valid_id = make_id(START_MS + 5_000, 77);
    let mut allocator = IdAllocator::new(0, last_valid_id);
    assert_eq!(allocator.allocate(START_MS, 1).unwrap(), last_valid_id + 1);
    assert_eq!(allocator.allocate(START_MS, 1).unwrap(), last_valid_id + 2);

    // A restarted stream's own last id also counts.
    let mut allocator = IdAllocator::new(make_id(START_MS + 9_000, 0), last_valid_id);
    assert_eq!(
        allocator.allocate(START_MS, 1).unwrap(),
        make_id(START_MS + 9_000, 1)
    );
}

#[test]
fn zero_items_are_refused_and_change_nothing() {
    let mut allocator = IdAllocator::new(5, 0);
    assert_eq!(allocator.allocate(START_MS, 0), Err(FormatError::ZeroCount));
    assert_eq!(allocator.last_id(), 5);
}

#[test]
fn id_zero_and_the_live_position_are_never_assigned() {
    // `first_id = 0` means "already committed" in an ACK, and id 0 can't be a cursor.
    let mut allocator = IdAllocator::new(0, 0);
    assert_eq!(allocator.allocate(0, 1).unwrap(), 1);

    // `u64::MAX` is the live-only cursor (section 5.1).
    let mut allocator = IdAllocator::new(u64::MAX - 2, 0);
    assert_eq!(allocator.allocate(0, 1).unwrap(), u64::MAX - 1);
    assert_eq!(allocator.allocate(0, 1), Err(FormatError::TooManyItems));
    assert_eq!(allocator.last_id(), u64::MAX - 1);

    let mut allocator = IdAllocator::new(u64::MAX - 2, 0);
    assert_eq!(allocator.allocate(0, 2), Err(FormatError::TooManyItems));
    assert_eq!(allocator.last_id(), u64::MAX - 2);
    assert_eq!(
        IdAllocator::new(u64::MAX, 0).allocate(0, 1),
        Err(FormatError::TooManyItems)
    );
    assert_eq!(
        IdAllocator::new(0, u64::MAX).allocate(0, 1),
        Err(FormatError::TooManyItems)
    );
}

#[test]
fn a_clock_past_48_bits_does_not_wrap() {
    let mut allocator = IdAllocator::new(0, 0);
    let id = allocator.allocate(u64::MAX, 1).unwrap();
    assert_eq!(id_ms(id), MAX_ID_MS);
    assert!(allocator.allocate(u64::MAX, 1).unwrap() > id);
}

#[test]
fn id_parts_round_trip() {
    let id = make_id(START_MS, 0xABCD);
    assert_eq!(id_ms(id), START_MS);
    assert_eq!(id_counter(id), 0xABCD);
}

proptest! {
    /// With any clock and any sizes, allocated ranges never overlap and only go up.
    #[test]
    fn ranges_only_go_up(
        steps in proptest::collection::vec((0u64..MAX_ID_MS, 1u64..200_000), 1..200),
    ) {
        let mut allocator = IdAllocator::new(0, 0);
        let mut last_id = 0;
        for (now_ms, item_count) in steps {
            let first_id = allocator.allocate(now_ms, item_count).unwrap();
            prop_assert!(first_id > last_id);
            prop_assert!(first_id >= now_ms << 16);
            last_id = first_id + item_count - 1;
        }
    }
}
