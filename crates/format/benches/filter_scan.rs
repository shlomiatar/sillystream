//! Records per second through a filter, by clause count and match rate, with and
//! without block skipping (SPEC section 4.1).
//!
//! `clustered` puts matching records in as few blocks as possible, as sorting by
//! options tends to. `scattered` spreads them evenly, the worst case for skipping.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use format::{BlockSummary, Clause, Decision, Filter, RecordHeader, Records, encode_record};

const BLOCK_RECORDS: usize = 1024;
const BLOCK_COUNT: usize = 64;
const TOTAL_RECORDS: usize = BLOCK_RECORDS * BLOCK_COUNT;
const DATA: [u8; 32] = [0xAB; 32];
const MATCH_OPTIONS: u64 = 0xA000_0000_0000_0001;
const OTHER_OPTIONS: u64 = 0x5000_0000_0000_0002;

struct Block {
    bytes: Vec<u8>,
    summary: BlockSummary,
}

/// Which records match: every nth when scattered, the first ones when clustered.
fn build_blocks(match_rate_per_mille: usize, clustered: bool) -> (Vec<Block>, usize) {
    let match_count = (TOTAL_RECORDS * match_rate_per_mille).div_ceil(1000);
    let mut blocks = Vec::with_capacity(BLOCK_COUNT);
    let mut next_id = 1_800_000_000_000u64 << 16;
    let mut matched = 0;
    for block_index in 0..BLOCK_COUNT {
        let mut bytes = Vec::with_capacity(BLOCK_RECORDS * (32 + DATA.len()));
        let mut summary = BlockSummary::new();
        for record_index in 0..BLOCK_RECORDS {
            let position = block_index * BLOCK_RECORDS + record_index;
            let is_match = if clustered {
                position < match_count
            } else {
                position * match_count / TOTAL_RECORDS
                    != (position + 1) * match_count / TOTAL_RECORDS
            };
            matched += usize::from(is_match);
            let header = RecordHeader {
                id: next_id,
                options: if is_match {
                    MATCH_OPTIONS
                } else {
                    OTHER_OPTIONS
                },
                author: 7,
                len: DATA.len() as u32,
                count: 1,
                codec: 0,
            };
            encode_record(&mut bytes, &header, &DATA).unwrap();
            summary.add(&header);
            next_id += 1;
        }
        blocks.push(Block { bytes, summary });
    }
    (blocks, matched)
}

/// One clause that matches, plus clauses that never match on these options.
fn build_filter(clause_count: usize) -> Filter {
    let mut clauses = vec![Clause::Eq {
        mask: 0xF000_0000_0000_0000,
        value: 0xA000_0000_0000_0000,
    }];
    let misses = [
        Clause::Eq {
            mask: 0x0F00_0000_0000_0000,
            value: 0x0100_0000_0000_0000,
        },
        Clause::Any {
            mask: 0x00F0_0000_0000_0000,
        },
        Clause::Eq {
            mask: 0x000F_0000_0000_0000,
            value: 0x0002_0000_0000_0000,
        },
    ];
    clauses.extend_from_slice(&misses[..clause_count - 1]);
    Filter::new(&clauses).unwrap()
}

fn scan_block(filter: &Filter, block: &Block) -> usize {
    Records::new(&block.bytes)
        .filter(|record| filter.matches(&record.as_ref().unwrap().header))
        .count()
}

fn scan_all(filter: &Filter, blocks: &[Block]) -> usize {
    blocks.iter().map(|block| scan_block(filter, block)).sum()
}

fn scan_skipping(filter: &Filter, blocks: &[Block]) -> usize {
    let mut matched = 0;
    for block in blocks {
        match filter.decide_block(&block.summary) {
            Decision::None => {}
            Decision::All => matched += block.summary.count as usize,
            Decision::Maybe => matched += scan_block(filter, block),
        }
    }
    matched
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_scan");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(600));
    group.throughput(Throughput::Elements(TOTAL_RECORDS as u64));
    for (layout, clustered) in [("clustered", true), ("scattered", false)] {
        for (rate, per_mille) in [
            ("0.1pct", 1),
            ("1pct", 10),
            ("10pct", 100),
            ("100pct", 1000),
        ] {
            let (blocks, expected) = build_blocks(per_mille, clustered);
            for clause_count in 1..=4 {
                let filter = build_filter(clause_count);
                // A benchmark of a wrong answer is worth nothing.
                assert_eq!(scan_all(&filter, &blocks), expected);
                assert_eq!(scan_skipping(&filter, &blocks), expected);
                let label = format!("{layout}/{rate}/{clause_count}_clauses");
                group.bench_function(BenchmarkId::new("full_scan", &label), |b| {
                    b.iter(|| black_box(scan_all(black_box(&filter), black_box(&blocks))))
                });
                group.bench_function(BenchmarkId::new("block_skip", &label), |b| {
                    b.iter(|| black_box(scan_skipping(black_box(&filter), black_box(&blocks))))
                });
            }
        }
    }
    group.finish();
}

/// One buffer of `count` records with options, authors and lengths from a small generator, so
/// that the branch in each filter isn't predictable. Ids count up by one.
fn kinds_buffer(count: usize) -> Vec<u8> {
    const FIRST_ID: u64 = 1_800_000_000_000 << 16;
    let mut bytes = Vec::with_capacity(count * (32 + DATA.len()));
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    for at in 0..count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let header = RecordHeader {
            id: FIRST_ID + at as u64,
            options: state,
            author: 1 + (state >> 60),
            len: DATA.len() as u32,
            count: 1,
            codec: 0,
        };
        encode_record(&mut bytes, &header, &DATA).unwrap();
    }
    bytes
}

/// Section 4's kinds, each scanned over 1,000 and 100,000 headers one at a time: nothing here
/// looks at a summary. Match rates come from random options and are printed, not chosen.
fn kinds(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_kinds");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));
    for count in [1_000usize, 100_000] {
        let bytes = kinds_buffer(count);
        let first_id = 1_800_000_000_000u64 << 16;
        let eq = Clause::Eq {
            mask: 0xF000_0000,
            value: 0x3000_0000,
        };
        let any = Clause::Any { mask: 0xF000_0000 };
        let miss = |bit: u32| Clause::Eq {
            mask: 0xFFFF << bit,
            value: 0x1234 << bit,
        };
        let all_records = Filter::new(&[]).unwrap();
        let mut author = all_records;
        author.author = Some(3);
        let mut time = all_records;
        time.id_first = first_id + count as u64 / 4;
        time.id_last = first_id + count as u64 / 2;
        let mut combined = Filter::new(&[eq, any]).unwrap();
        combined.author = Some(3);
        combined.id_first = time.id_first;
        combined.id_last = time.id_last;
        let cases = [
            ("no_clause", all_records),
            ("eq", Filter::new(&[eq]).unwrap()),
            ("any", Filter::new(&[any]).unwrap()),
            ("author", author),
            ("time_range", time),
            (
                "four_clauses",
                Filter::new(&[miss(0), miss(16), miss(32), eq]).unwrap(),
            ),
            ("all_kinds", combined),
        ];
        group.throughput(Throughput::Elements(count as u64));
        for (name, filter) in cases {
            let scan = |filter: &Filter, bytes: &[u8]| {
                Records::new(bytes)
                    .filter(|record| filter.matches(&record.as_ref().unwrap().header))
                    .count()
            };
            println!("MATCHES {name} over {count}: {}", scan(&filter, &bytes));
            group.bench_function(BenchmarkId::new(name, count), |b| {
                b.iter(|| black_box(scan(black_box(&filter), black_box(&bytes))))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench, kinds);
criterion_main!(benches);
