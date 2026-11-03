//! Stream → virtual shard → core (SPEC section 6.1), the per-shard counters and the
//! choice of which shards to move.

use format::KeyHash;

/// The shard a stream hashes to. The override table comes first, and lives on the cores.
pub fn shard_of(stream: KeyHash, virtual_shard_count: u32) -> u32 {
    (stream.top64() % u64::from(virtual_shard_count)) as u32
}

const AVERAGE_WEIGHT: f64 = 0.5;

fn fold(average: &mut f64, sample: u64) {
    *average = *average * (1.0 - AVERAGE_WEIGHT) + sample as f64 * AVERAGE_WEIGHT;
}

/// Section 6.1's counters: totals since the last sample, and their moving averages per
/// sample.
#[derive(Debug, Clone, Default)]
pub struct ShardCounters {
    pub ingest_bytes: u64,
    pub ingest_records: u64,
    pub fan_out_bytes: u64,
    pub catch_up_bytes: u64,
    pub subscriber_count: u64,
    pub ingest_bytes_avg: f64,
    pub ingest_records_avg: f64,
    pub fan_out_bytes_avg: f64,
    pub catch_up_bytes_avg: f64,
    pub subscriber_count_avg: f64,
}

impl ShardCounters {
    /// Folds the totals into the averages, and starts the next sample.
    pub fn sample(&mut self) {
        fold(
            &mut self.ingest_bytes_avg,
            std::mem::take(&mut self.ingest_bytes),
        );
        fold(
            &mut self.ingest_records_avg,
            std::mem::take(&mut self.ingest_records),
        );
        fold(
            &mut self.fan_out_bytes_avg,
            std::mem::take(&mut self.fan_out_bytes),
        );
        fold(
            &mut self.catch_up_bytes_avg,
            std::mem::take(&mut self.catch_up_bytes),
        );
        // A level, not a total: it stays when sampled.
        fold(&mut self.subscriber_count_avg, self.subscriber_count);
    }

    /// Bytes a core has to move for this shard, which is what the check balances.
    pub fn load(&self) -> f64 {
        self.ingest_bytes_avg + self.fan_out_bytes_avg + self.catch_up_bytes_avg
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMove {
    pub shard: u32,
    pub from: usize,
    pub to: usize,
}

/// The moves that take load off overloaded cores, given `(shard, core, load)`.
///
/// A core is overloaded above `overload_ratio` × the mean. Each move takes the largest
/// shard smaller than the gap to the least loaded core, so it always lowers the higher
/// of the two, and the check can't make a shard bounce. Deterministic for a given input.
pub fn plan_moves(
    shards: &[(u32, usize, f64)],
    core_count: usize,
    overload_ratio: f64,
    max_moves: usize,
) -> Vec<ShardMove> {
    let mut totals = vec![0.0; core_count];
    for (_, core, load) in shards {
        totals[*core] += *load;
    }
    let mean = totals.iter().sum::<f64>() / core_count as f64;
    let mut placed: Vec<(u32, usize, f64)> = shards.to_vec();
    let mut moves = Vec::new();
    while moves.len() < max_moves {
        let (mut hot, mut cold) = (0, 0);
        for core in 0..core_count {
            if totals[core] > totals[hot] {
                hot = core;
            }
            if totals[core] < totals[cold] {
                cold = core;
            }
        }
        if mean <= 0.0 || totals[hot] <= mean * overload_ratio {
            break;
        }
        let gap = totals[hot] - totals[cold];
        let candidate = placed
            .iter_mut()
            .filter(|(_, core, load)| *core == hot && *load > 0.0 && *load < gap)
            .max_by(|a, b| a.2.total_cmp(&b.2).then(b.0.cmp(&a.0)));
        let Some(shard) = candidate else {
            break;
        };
        shard.1 = cold;
        totals[hot] -= shard.2;
        totals[cold] += shard.2;
        moves.push(ShardMove {
            shard: shard.0,
            from: hot,
            to: cold,
        });
    }
    moves
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn a_balanced_node_moves_nothing() {
        let shards = [(0, 0, 10.0), (1, 1, 10.0), (2, 2, 11.0), (3, 3, 9.0)];
        assert!(plan_moves(&shards, 4, 1.25, 8).is_empty());
        assert!(plan_moves(&[], 4, 1.25, 8).is_empty());
    }

    #[test]
    fn shards_leave_the_overloaded_core_for_the_coldest() {
        let shards = [(0, 0, 40.0), (1, 0, 30.0), (2, 0, 20.0), (3, 1, 10.0)];
        let moves = plan_moves(&shards, 2, 1.25, 8);
        assert_eq!(
            moves,
            [ShardMove {
                shard: 0,
                from: 0,
                to: 1
            }]
        );
    }

    #[test]
    fn one_shard_bigger_than_the_gap_stays_where_it_is() {
        // Moving it would only overload the other core the same way.
        assert!(plan_moves(&[(0, 0, 100.0)], 2, 1.25, 8).is_empty());
    }

    #[test]
    fn moves_are_capped() {
        let shards: Vec<_> = (0..20).map(|shard| (shard, 0, 1.0)).collect();
        assert_eq!(plan_moves(&shards, 4, 1.1, 3).len(), 3);
    }

    #[test]
    fn counters_average_and_reset_their_totals() {
        let mut counters = ShardCounters {
            ingest_bytes: 100,
            subscriber_count: 2,
            ..ShardCounters::default()
        };
        counters.sample();
        assert_eq!(counters.ingest_bytes, 0);
        assert_eq!(counters.ingest_bytes_avg, 50.0);
        assert_eq!(counters.subscriber_count, 2);
        counters.sample();
        assert_eq!(counters.ingest_bytes_avg, 25.0);
        assert_eq!(counters.subscriber_count_avg, 1.5);
    }

    proptest! {
        /// However the load is spread, a plan never raises the highest core's load, and
        /// every move starts where the shard is.
        #[test]
        fn a_plan_never_makes_the_peak_worse(
            loads in proptest::collection::vec((0usize..4, 0.0f64..1000.0), 0..60),
        ) {
            let shards: Vec<_> = loads
                .iter()
                .enumerate()
                .map(|(shard, (core, load))| (shard as u32, *core, *load))
                .collect();
            let peak = |shards: &[(u32, usize, f64)]| {
                let mut totals = [0.0; 4];
                for (_, core, load) in shards {
                    totals[*core] += load;
                }
                totals.iter().cloned().fold(0.0, f64::max)
            };
            let before = peak(&shards);
            let mut after = shards.clone();
            for planned in plan_moves(&shards, 4, 1.25, 16) {
                let shard = after.iter_mut().find(|s| s.0 == planned.shard).unwrap();
                prop_assert_eq!(shard.1, planned.from);
                shard.1 = planned.to;
            }
            prop_assert!(peak(&after) <= before + 1e-6);
        }
    }
}
