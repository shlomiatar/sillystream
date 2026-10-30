//! Filter clauses and the block skip rules (SPEC sections 4 and 4.1, D3, D33).

use format::{
    BlockAuthor, BlockSummary, Clause, Decision, Filter, FormatError, OP_ANY, OP_EQ, RecordHeader,
};
use proptest::prelude::*;

fn header(id: u64, options: u64, author: u64, count: u16) -> RecordHeader {
    RecordHeader {
        id,
        options,
        author,
        len: 0,
        count,
        codec: 0,
    }
}

fn summarize(records: &[RecordHeader]) -> BlockSummary {
    let mut summary = BlockSummary::new();
    for record in records {
        summary.add(record);
    }
    summary
}

/// Small bit patterns, so that clauses and records actually coincide.
fn arb_bits() -> impl Strategy<Value = u64> {
    prop_oneof![
        3 => 0u64..16,
        1 => (0u64..16).prop_map(|bits| bits << 60),
        1 => (0u64..16).prop_map(|bits| bits << 60 | 0x5),
        1 => any::<u64>(),
    ]
}

fn arb_clause() -> impl Strategy<Value = Clause> {
    prop_oneof![
        (arb_bits(), arb_bits()).prop_map(|(mask, value)| Clause::Eq { mask, value }),
        // Values inside the mask, the case real clients send.
        (arb_bits(), arb_bits()).prop_map(|(mask, value)| Clause::Eq {
            mask,
            value: value & mask
        }),
        arb_bits().prop_map(|mask| Clause::Any { mask }),
    ]
}

/// A block: increasing ids, a few authors (0 is a system record).
fn arb_block() -> impl Strategy<Value = Vec<RecordHeader>> {
    let authors = prop_oneof![
        // A block with one author, or a mix, with or without system records.
        Just(vec![7u64]),
        Just(vec![0u64]),
        Just(vec![7, 8]),
        Just(vec![0, 7]),
        Just(vec![0, 7, 8]),
    ];
    (
        authors,
        proptest::collection::vec((arb_bits(), any::<usize>(), 1u16..4), 1..12),
        1u64..1000,
    )
        .prop_map(|(authors, records, start)| {
            let mut next_id = start;
            records
                .into_iter()
                .map(|(options, pick, count)| {
                    let record = header(next_id, options, authors[pick % authors.len()], count);
                    next_id += u64::from(count);
                    record
                })
                .collect()
        })
}

fn arb_filter() -> impl Strategy<Value = Filter> {
    (
        proptest::collection::vec(arb_clause(), 0..=4),
        prop_oneof![
            Just(None),
            Just(Some(0u64)),
            Just(Some(7u64)),
            Just(Some(9u64))
        ],
        any::<bool>(),
        prop_oneof![Just(0u64), 0u64..1100],
        prop_oneof![Just(u64::MAX), 0u64..1100],
    )
        .prop_map(|(clauses, author, include_system, id_first, id_last)| {
            let mut filter = Filter::new(&clauses).unwrap();
            filter.author = author;
            filter.include_system = include_system;
            filter.id_first = id_first;
            filter.id_last = id_last;
            filter
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20_000))]

    /// `none` and `all` never disagree with evaluating each record.
    #[test]
    fn block_decision_agrees_with_records(records in arb_block(), filter in arb_filter()) {
        let summary = summarize(&records);
        let matches = records.iter().filter(|record| filter.matches(record)).count();
        match filter.decide_block(&summary) {
            Decision::None => prop_assert_eq!(matches, 0),
            Decision::All => prop_assert_eq!(matches, records.len()),
            Decision::Maybe => {}
        }
    }

    /// The same, for each clause's own section 4.1 rule.
    #[test]
    fn clause_decision_agrees_with_records(
        options in proptest::collection::vec(arb_bits(), 1..12),
        clause in arb_clause(),
    ) {
        let options_or = options.iter().fold(0, |acc, value| acc | value);
        let options_and = options.iter().fold(u64::MAX, |acc, value| acc & value);
        let matches = options.iter().filter(|value| clause.matches(**value)).count();
        match clause.decide(options_or, options_and) {
            Decision::None => prop_assert_eq!(matches, 0),
            Decision::All => prop_assert_eq!(matches, options.len()),
            Decision::Maybe => {}
        }
    }
}

/// The property tests prove nothing if decisions are always `Maybe`, so count them.
#[test]
fn the_rules_do_decide_blocks() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::deterministic();
    let (mut none, mut all, mut maybe) = (0, 0, 0);
    for _ in 0..5_000 {
        let records = arb_block().new_tree(&mut runner).unwrap().current();
        let filter = arb_filter().new_tree(&mut runner).unwrap().current();
        match filter.decide_block(&summarize(&records)) {
            Decision::None => none += 1,
            Decision::All => all += 1,
            Decision::Maybe => maybe += 1,
        }
    }
    assert!(none > 250, "none {none}");
    assert!(all > 100, "all {all}");
    assert!(maybe > 250, "maybe {maybe}");
}

#[test]
fn clause_evaluation() {
    let eq = Clause::Eq {
        mask: 0xF0,
        value: 0x30,
    };
    assert!(eq.matches(0x3F));
    assert!(!eq.matches(0x4F));
    let any = Clause::Any { mask: 0xF0 };
    assert!(any.matches(0x10));
    assert!(!any.matches(0x0F));
    assert!(!Clause::Any { mask: 0 }.matches(u64::MAX));
    assert!(Clause::Eq { mask: 0, value: 0 }.matches(u64::MAX));
}

#[test]
fn clause_wire_codes() {
    assert_eq!(
        Clause::from_wire(1, 2, OP_EQ),
        Ok(Clause::Eq { mask: 1, value: 2 })
    );
    assert_eq!(Clause::from_wire(1, 2, OP_ANY), Ok(Clause::Any { mask: 1 }));
    assert_eq!(
        Clause::from_wire(1, 2, 2),
        Err(FormatError::UnknownClauseOp)
    );
}

#[test]
fn eq_rules_from_the_spec() {
    let clause = Clause::Eq {
        mask: 0b1100,
        value: 0b0100,
    };
    // need1 = 0100, need0 = 1000.
    assert_eq!(
        clause.decide(0b0011, 0b0000),
        Decision::None,
        "need1 absent from OR"
    );
    assert_eq!(
        clause.decide(0b1100, 0b1000),
        Decision::None,
        "need0 present in AND"
    );
    assert_eq!(clause.decide(0b0100, 0b0100), Decision::All);
    assert_eq!(
        clause.decide(0b1100, 0b0100),
        Decision::Maybe,
        "need0 in OR, not AND"
    );
    assert_eq!(
        clause.decide(0b0100, 0b0000),
        Decision::Maybe,
        "need1 in OR, not AND"
    );
}

#[test]
fn any_rules_from_the_spec() {
    let clause = Clause::Any { mask: 0b1100 };
    assert_eq!(clause.decide(0b0011, 0b0000), Decision::None);
    assert_eq!(clause.decide(0b0100, 0b0100), Decision::All);
    assert_eq!(clause.decide(0b0100, 0b0000), Decision::Maybe);
}

/// D33: `(options & mask) == value` is never true if `value` has bits outside
/// `mask`, while the formula of section 4.1 alone would report "all match".
#[test]
fn eq_with_value_bits_outside_the_mask_matches_nothing() {
    let clause = Clause::Eq {
        mask: 0b01,
        value: 0b11,
    };
    assert!(!clause.matches(0b01));
    assert!(!clause.matches(0b11));
    assert_eq!(clause.decide(0b01, 0b01), Decision::None);
}

#[test]
fn clauses_are_ored_and_limited_to_four() {
    let hit = Clause::Eq { mask: 1, value: 1 };
    let miss = Clause::Eq { mask: 2, value: 2 };
    let filter = Filter::new(&[miss, hit]).unwrap();
    assert!(filter.matches(&header(1, 1, 7, 1)));
    assert!(!filter.matches(&header(1, 4, 7, 1)));

    assert_eq!(Filter::new(&[hit; 4]).unwrap().clauses().len(), 4);
    assert_eq!(Filter::new(&[hit; 5]), Err(FormatError::TooManyClauses));
}

#[test]
fn no_clauses_match_every_app_record() {
    let filter = Filter::new(&[]).unwrap();
    assert!(filter.matches(&header(1, 123, 7, 1)));
    assert_eq!(
        filter.decide_block(&summarize(&[header(1, 123, 7, 1)])),
        Decision::All
    );
}

#[test]
fn equal_filters_compare_equal_whatever_the_unused_slots_held() {
    let clause = Clause::Any { mask: 1 };
    assert_eq!(
        Filter::new(&[clause]).unwrap(),
        Filter::new(&[clause]).unwrap()
    );
    assert_ne!(Filter::new(&[clause]).unwrap(), Filter::new(&[]).unwrap());
}

#[test]
fn author_is_equality() {
    let mut filter = Filter::new(&[]).unwrap();
    filter.author = Some(7);
    assert!(filter.matches(&header(1, 0, 7, 1)));
    assert!(!filter.matches(&header(1, 0, 8, 1)));
    assert_eq!(
        filter.decide_block(&summarize(&[header(1, 0, 8, 1)])),
        Decision::None
    );
    assert_eq!(
        filter.decide_block(&summarize(&[header(1, 0, 7, 1)])),
        Decision::All
    );
    let mixed = summarize(&[header(1, 0, 7, 1), header(2, 0, 8, 1)]);
    assert_eq!(mixed.author, BlockAuthor::Mixed);
    assert_eq!(filter.decide_block(&mixed), Decision::Maybe);
}

/// A record matches if any of its items is in the range, and is sent whole.
#[test]
fn id_range_overlaps_records() {
    let record = header(100, 0, 7, 10); // Items 100..=109.
    let mut filter = Filter::new(&[]).unwrap();
    for (id_first, id_last, expected) in [
        (0, u64::MAX, true),
        (109, u64::MAX, true),
        (110, u64::MAX, false),
        (0, 100, true),
        (0, 99, false),
        (105, 105, true),
    ] {
        filter.id_first = id_first;
        filter.id_last = id_last;
        assert_eq!(filter.matches(&record), expected, "{id_first}..={id_last}");
    }
}

#[test]
fn block_outside_the_range_is_none_and_part_way_is_maybe() {
    let block = summarize(&[header(100, 0, 7, 10), header(110, 0, 7, 10)]);
    assert_eq!((block.id_first, block.id_last), (100, 119));
    let mut filter = Filter::new(&[]).unwrap();
    assert_eq!(filter.decide_block(&block), Decision::All);
    filter.id_first = 120;
    assert_eq!(filter.decide_block(&block), Decision::None);
    filter.id_first = 110;
    assert_eq!(filter.decide_block(&block), Decision::Maybe);
    filter.id_first = 100;
    filter.id_last = 99;
    assert_eq!(filter.decide_block(&block), Decision::None);
}

/// D3: app filters don't apply to system records, and `INCLUDE_SYSTEM` gets them all.
#[test]
fn system_records_are_decided_by_include_system_alone() {
    let system = header(1, 1, 0, 1);
    let miss = Clause::Eq {
        mask: 0xF0,
        value: 0xF0,
    };
    let mut filter = Filter::new(&[miss]).unwrap();
    filter.author = Some(7);
    assert!(!filter.matches(&system));
    filter.include_system = true;
    assert!(filter.matches(&system));
    // The id range still applies.
    filter.id_first = 2;
    assert!(!filter.matches(&system));
}

/// D3's skipping rule: an options "none" can't skip a block that may hold a system
/// record for a reader who wants them, only one whose author is a non-zero value.
#[test]
fn system_reader_may_only_skip_blocks_with_a_non_zero_author() {
    let miss = Clause::Eq {
        mask: 0xF0,
        value: 0xF0,
    };
    let mut reader = Filter::new(&[miss]).unwrap();
    reader.include_system = true;

    let mixed = summarize(&[header(1, 1, 0, 1), header(2, 1, 7, 1)]);
    assert_eq!(reader.decide_block(&mixed), Decision::Maybe);
    let only_system = summarize(&[header(1, 1, 0, 1), header(2, 2, 0, 1)]);
    assert_eq!(reader.decide_block(&only_system), Decision::All);
    let only_app = summarize(&[header(1, 1, 7, 1)]);
    assert_eq!(reader.decide_block(&only_app), Decision::None);

    // A reader without INCLUDE_SYSTEM skips all three.
    let mut plain = reader;
    plain.include_system = false;
    assert_eq!(plain.decide_block(&mixed), Decision::None);
    assert_eq!(plain.decide_block(&only_system), Decision::None);
    assert_eq!(plain.decide_block(&only_app), Decision::None);
}

/// A plain reader can't get "all" from a block that may hold a system record.
#[test]
fn plain_reader_gets_no_all_from_a_block_with_system_records() {
    let filter = Filter::new(&[]).unwrap();
    let mixed = summarize(&[header(1, 1, 0, 1), header(2, 1, 7, 1)]);
    assert_eq!(filter.decide_block(&mixed), Decision::Maybe);
}

#[test]
fn summary_fields() {
    let first = RecordHeader {
        id: 10,
        options: 0b0110,
        author: 7,
        len: 5,
        count: 3,
        codec: 1,
    };
    let second = RecordHeader {
        id: 13,
        options: 0b0011,
        author: 7,
        len: 8,
        count: 2,
        codec: 0,
    };
    let summary = summarize(&[first, second]);
    assert_eq!(summary.options_or, 0b0111);
    assert_eq!(summary.options_and, 0b0010);
    assert_eq!(summary.id_first, 10);
    assert_eq!(summary.id_last, 14);
    assert_eq!(summary.count, 5);
    assert_eq!(summary.bytes, 80);
    assert_eq!(summary.author, BlockAuthor::Single(7));
}

#[test]
fn an_empty_block_matches_nothing() {
    assert_eq!(
        Filter::new(&[]).unwrap().decide_block(&BlockSummary::new()),
        Decision::None
    );
}
