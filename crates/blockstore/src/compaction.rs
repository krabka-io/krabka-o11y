//! Level-based compaction planning, shared by every signal.
//!
//! A block carries a [`BlockLevel`]: zero when a block builder wrote it from
//! ingested data, one more than its inputs when a compaction produced it.
//! [`plan_compactions`] groups blocks of the same tenant, level and time
//! bucket into [`CompactionJob`]s under a [`CompactionPolicy`], and the policy
//! is what makes the planning terminate rather than rewrite the same rows for
//! as long as the process runs.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn candidate(
        tenant: &str,
        object_key: &str,
        min_ts: i64,
        max_ts: i64,
        row_count: usize,
        level: u32,
    ) -> CompactionCandidate {
        CompactionCandidate {
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            min_ts,
            max_ts,
            row_count,
            level: BlockLevel(level),
        }
    }

    /// A policy with a wide window and no row target in the way, so a test can
    /// isolate one rule at a time.
    fn policy(max_blocks_per_job: usize, target_rows: usize, max_level: u32) -> CompactionPolicy {
        CompactionPolicy::new(
            max_blocks_per_job,
            target_rows,
            BlockLevel(max_level),
            i64::MAX,
        )
    }

    fn input_keys(jobs: &[CompactionJob]) -> Vec<Vec<String>> {
        jobs.iter().map(|job| job.input_keys.clone()).collect()
    }

    #[test]
    fn a_single_block_is_never_a_job() {
        let candidates = vec![candidate("t", "a", 0, 10, 1, 0)];
        check!(plan_compactions(&candidates, policy(4, 1_000, 4)).is_empty());
    }

    #[test]
    fn blocks_are_grouped_by_tenant_and_taken_in_time_order() {
        let candidates = vec![
            candidate("b", "b2", 30, 40, 1, 0),
            candidate("a", "a2", 10, 20, 1, 0),
            candidate("b", "b1", 0, 10, 1, 0),
            candidate("a", "a1", 0, 10, 1, 0),
        ];
        let jobs = plan_compactions(&candidates, policy(4, 1_000, 4));
        check!(
            jobs == vec![
                CompactionJob {
                    tenant: "a".to_string(),
                    input_keys: vec!["a1".to_string(), "a2".to_string()],
                    output_level: BlockLevel(1),
                    min_ts: 0,
                    max_ts: 20,
                    row_count: 2,
                },
                CompactionJob {
                    tenant: "b".to_string(),
                    input_keys: vec!["b1".to_string(), "b2".to_string()],
                    output_level: BlockLevel(1),
                    min_ts: 0,
                    max_ts: 40,
                    row_count: 2,
                },
            ]
        );
    }

    #[test]
    fn a_job_never_mixes_levels() {
        let candidates = vec![
            candidate("t", "l0-a", 0, 10, 1, 0),
            candidate("t", "l1-a", 1, 11, 1, 1),
            candidate("t", "l0-b", 2, 12, 1, 0),
            candidate("t", "l1-b", 3, 13, 1, 1),
        ];
        let jobs = plan_compactions(&candidates, policy(4, 1_000, 4));
        check!(
            input_keys(&jobs)
                == vec![
                    vec!["l0-a".to_string(), "l0-b".to_string()],
                    vec!["l1-a".to_string(), "l1-b".to_string()],
                ]
        );
        check!(
            jobs.iter().map(|job| job.output_level).collect::<Vec<_>>()
                == vec![BlockLevel(1), BlockLevel(2)]
        );
    }

    #[test]
    fn a_job_never_spans_two_windows_and_the_window_widens_with_the_level() {
        // Window 100ns at level 0, 200ns at level 1. The level-0 pair straddles
        // the 100ns boundary and so cannot merge; the level-1 pair sits inside
        // one 200ns bucket and does.
        let policy = CompactionPolicy::new(4, 1_000, BlockLevel(4), 100);
        let candidates = vec![
            candidate("t", "l0-early", 10, 20, 1, 0),
            candidate("t", "l0-late", 110, 120, 1, 0),
            candidate("t", "l1-early", 10, 20, 1, 1),
            candidate("t", "l1-late", 110, 120, 1, 1),
        ];
        check!(
            input_keys(&plan_compactions(&candidates, policy))
                == vec![vec!["l1-early".to_string(), "l1-late".to_string()]]
        );
    }

    #[test]
    fn a_run_closes_at_the_fan_in_cap_and_the_remainder_needs_two_blocks() {
        let candidates = (0..5)
            .map(|n| candidate("t", &format!("b{n}"), n, n + 1, 1, 0))
            .collect::<Vec<_>>();
        // Two of two, then one left over, which is not a job.
        check!(
            input_keys(&plan_compactions(&candidates, policy(2, 1_000, 4)))
                == vec![
                    vec!["b0".to_string(), "b1".to_string()],
                    vec!["b2".to_string(), "b3".to_string()],
                ]
        );
    }

    #[test]
    fn a_run_closes_once_it_covers_the_row_target() {
        let candidates = vec![
            candidate("t", "b0", 0, 1, 60, 0),
            candidate("t", "b1", 1, 2, 60, 0),
            candidate("t", "b2", 2, 3, 60, 0),
            candidate("t", "b3", 3, 4, 60, 0),
        ];
        // 60 + 60 reaches the 100-row target, so the run closes at two even
        // though the fan-in cap would allow eight.
        check!(
            input_keys(&plan_compactions(&candidates, policy(8, 100, 4)))
                == vec![
                    vec!["b0".to_string(), "b1".to_string()],
                    vec!["b2".to_string(), "b3".to_string()],
                ]
        );
    }

    #[test]
    fn a_block_at_the_top_of_the_ladder_is_never_an_input_again() {
        let candidates = vec![
            candidate("t", "top-a", 0, 10, 1, 2),
            candidate("t", "top-b", 1, 11, 1, 2),
            candidate("t", "below-a", 2, 12, 1, 1),
            candidate("t", "below-b", 3, 13, 1, 1),
        ];
        check!(
            input_keys(&plan_compactions(&candidates, policy(4, 1_000, 2)))
                == vec![vec!["below-a".to_string(), "below-b".to_string()]]
        );
    }

    #[test]
    fn a_block_that_already_covers_the_row_target_is_never_an_input() {
        let candidates = vec![
            candidate("t", "full", 0, 10, 100, 0),
            candidate("t", "small-a", 1, 11, 1, 0),
            candidate("t", "small-b", 2, 12, 1, 0),
        ];
        check!(
            input_keys(&plan_compactions(&candidates, policy(4, 100, 4)))
                == vec![vec!["small-a".to_string(), "small-b".to_string()]]
        );
    }

    /// An unknown row count reads as "small", not as "empty": the block is
    /// still compactable and contributes nothing to the run's row budget.
    #[test]
    fn an_unknown_row_count_does_not_seal_a_block() {
        let candidates = vec![
            candidate("t", "a", 0, 10, 0, 0),
            candidate("t", "b", 1, 11, 0, 0),
            candidate("t", "c", 2, 12, 0, 0),
        ];
        check!(
            input_keys(&plan_compactions(&candidates, policy(8, 100, 4)))
                == vec![vec!["a".to_string(), "b".to_string(), "c".to_string()]]
        );
    }

    /// Applying a plan and replanning must reach a fixed point. This runs the
    /// loop the compactor runs, with no new ingest, and pins both that it
    /// stops and how far it climbs.
    #[test]
    fn repeated_planning_reaches_an_empty_plan() {
        for (_name, block_count, policy) in [
            ("fan-in two", 64_usize, policy(2, usize::MAX, 8)),
            ("fan-in eight", 64, policy(8, usize::MAX, 8)),
            ("short ladder", 64, policy(2, usize::MAX, 2)),
            ("odd count", 37, policy(4, usize::MAX, 6)),
        ] {
            let mut live = (0..block_count)
                .map(|n| {
                    candidate(
                        "t",
                        &format!("b{n:04}"),
                        i64::try_from(n).unwrap_or(0),
                        0,
                        1,
                        0,
                    )
                })
                .collect::<Vec<_>>();

            let mut rounds = 0_usize;
            loop {
                let jobs = plan_compactions(&live, policy);
                if jobs.is_empty() {
                    break;
                }
                rounds += 1;
                check!(rounds <= 64, "planning did not converge");
                for job in jobs {
                    check!(job.input_keys.len() >= 2, "a job of one rewrites a block");
                    let merged = CompactionCandidate {
                        tenant: job.tenant.clone(),
                        object_key: format!("c{rounds}-{}", job.input_keys.join("+")),
                        min_ts: job.min_ts,
                        max_ts: job.max_ts,
                        row_count: job.row_count,
                        level: job.output_level,
                    };
                    live.retain(|block| !job.input_keys.contains(&block.object_key));
                    live.push(merged);
                }
            }

            // A block is rewritten at most once per level, so the number of
            // rounds cannot exceed the ladder height.
            check!(rounds <= usize::try_from(policy.max_level().get()).unwrap_or(usize::MAX));
            check!(
                live.iter().all(|block| block.level <= policy.max_level()),
                "no block climbs past the cap"
            );
        }
    }

    /// The other half of termination: with the row target biting, the planner
    /// stops even though the ladder still has room.
    #[test]
    fn the_row_target_stops_planning_below_the_top_of_the_ladder() {
        let mut live = (0..8)
            .map(|n| candidate("t", &format!("b{n}"), n, n, 50, 0))
            .collect::<Vec<_>>();
        let policy = policy(2, 100, 8);

        let mut rounds = 0_usize;
        loop {
            let jobs = plan_compactions(&live, policy);
            if jobs.is_empty() {
                break;
            }
            rounds += 1;
            check!(rounds <= 8);
            for job in jobs {
                let merged = CompactionCandidate {
                    tenant: job.tenant.clone(),
                    object_key: format!("c-{}", job.input_keys.join("+")),
                    min_ts: job.min_ts,
                    max_ts: job.max_ts,
                    row_count: job.row_count,
                    level: job.output_level,
                };
                live.retain(|block| !job.input_keys.contains(&block.object_key));
                live.push(merged);
            }
        }

        // Every block reached the 100-row target at level 1 and was sealed
        // there, seven levels short of the cap.
        check!(rounds == 1);
        check!(live.len() == 4);
        check!(live.iter().all(|block| block.level == BlockLevel(1)));
    }

    #[test]
    fn a_compacted_block_sits_one_level_above_the_highest_of_its_sources() {
        let mut lineage = BlockLineageIndex::default();
        lineage.record_ingested("a", 10);
        lineage.record_ingested("b", 20);
        lineage.record_compacted("ab", &["a".to_string(), "b".to_string()], 30);
        lineage.record_compacted("abc", &["ab".to_string(), "c".to_string()], 35);

        check!(lineage.level("a") == BlockLevel::INGESTED);
        check!(lineage.level("ab") == BlockLevel(1));
        // "c" has no record, so it reads as level zero; the highest source
        // still decides.
        check!(lineage.level("abc") == BlockLevel(2));
        check!(
            lineage.lineage("ab")
                == Some(&BlockLineage {
                    level: BlockLevel(1),
                    row_count: 30,
                    sources: vec!["a".to_string(), "b".to_string()],
                })
        );
        check!(lineage.row_count("a") == 10);
        check!(lineage.row_count("missing") == 0);

        // A block that replaced nothing is not one rung above nothing.
        lineage.record_compacted("fresh", &[], 5);
        check!(lineage.level("fresh") == BlockLevel::INGESTED);
    }

    /// Snapshot merging re-adds every block it knows, including compacted
    /// ones. Registering a block that already has lineage must not reset it,
    /// or the ladder would collapse back to level zero on every save and the
    /// planner would never stop.
    #[test]
    fn re_registering_a_compacted_block_does_not_demote_it() {
        let mut lineage = BlockLineageIndex::default();
        lineage.record_compacted("ab", &["a".to_string(), "b".to_string()], 30);
        lineage.record_ingested("ab", 30);
        check!(lineage.level("ab") == BlockLevel(1));
    }

    #[test]
    fn forgetting_and_retaining_bound_the_map() {
        let mut lineage = BlockLineageIndex::default();
        lineage.record_ingested("a", 1);
        lineage.record_ingested("b", 1);
        lineage.record_ingested("c", 1);
        check!(lineage.len() == 3);

        lineage.forget(["a"]);
        check!(lineage.lineage("a").is_none());

        lineage.retain_keys(&BTreeSet::from(["b".to_string()]));
        check!(lineage.len() == 1);
        check!(lineage.lineage("b").is_some());

        let mut other = BlockLineageIndex::default();
        other.record_compacted("b", &["x".to_string()], 9);
        lineage.merge_from(&other);
        check!(lineage.level("b") == BlockLevel(1), "the other side wins");
        check!(!lineage.is_empty());
    }

    #[test]
    fn a_policy_clamps_caps_that_would_make_planning_pointless() {
        let clamped = CompactionPolicy::new(0, 0, BlockLevel(0), 0);
        check!(clamped.max_blocks_per_job() == 2);
        check!(clamped.target_rows_per_block() == 1);
        check!(clamped.max_level() == BlockLevel(1));
        check!(clamped.window_ns_for(BlockLevel::INGESTED) == 1);
    }

    #[test]
    fn the_window_doubles_per_level_and_saturates_rather_than_wrapping() {
        let policy = CompactionPolicy::new(4, 100, BlockLevel(64), 1_000);
        check!(policy.window_ns_for(BlockLevel(0)) == 1_000);
        check!(policy.window_ns_for(BlockLevel(1)) == 2_000);
        check!(policy.window_ns_for(BlockLevel(3)) == 8_000);
        check!(policy.window_ns_for(BlockLevel(62)) == i64::MAX);
        check!(policy.window_ns_for(BlockLevel(64)) == i64::MAX);
    }

    #[test]
    fn a_level_renders_as_its_number() {
        check!(BlockLevel::INGESTED.to_string() == "0");
        check!(BlockLevel(3).next().to_string() == "4");
        check!(BlockLevel(u32::MAX).next() == BlockLevel(u32::MAX));
    }

    /// `input_key_fingerprint` hashes a list of keys into one value, folding a
    /// separator between them so that where one key ends and the next begins
    /// is part of the input. Two compaction inputs that join differently must
    /// not share an output name.
    ///
    /// The expected hashes are stated outright rather than compared against
    /// each other. Inequality is too weak a claim for a hash: dropping the
    /// separator, or leaving it unmixed, or replacing the xor with an or, all
    /// still produce different values for different inputs, and all survived
    /// a version of this test that only asserted the values differed.
    #[test]
    fn hashing_keys_folds_a_separator_between_them() {
        let hash = |keys: &[&str]| {
            input_key_fingerprint(&keys.iter().map(|k| (*k).to_string()).collect::<Vec<_>>())
        };

        // No keys leaves the offset basis untouched.
        check!(hash(&[]) == 0xcbf2_9ce4_8422_2325);

        // One empty key still folds a separator, so it is not the same as no
        // key at all.
        check!(hash(&[""]) == 0xaf64_724c_8602_eb6e);
        check!(hash(&["a"]) == 0x089b_c907_b544_c769);

        // Order is part of the input.
        check!(hash(&["a", "b"]) == 0xd2b3_7181_9297_f98a);
        check!(hash(&["b", "a"]) == 0x0185_7199_9fe5_8c66);

        // So is where the keys divide: the same bytes split two ways, and
        // joined into one, give three different hashes.
        check!(hash(&["ab", "c"]) == 0x20ba_9b30_25a8_b421);
        check!(hash(&["a", "bc"]) == 0xa0a3_542c_19b9_00ab);
        check!(hash(&["abc"]) == 0xfc18_2483_ee08_06dc);
    }
}

mod block_level;
mod block_lineage;
mod block_lineage_index;
mod compaction_candidate;
mod compaction_job;
mod compaction_policy;
mod input_key_fingerprint;
mod job_from_run;
mod plan_compactions;

pub use block_level::BlockLevel;
pub use block_lineage::BlockLineage;
pub use block_lineage_index::BlockLineageIndex;
pub use compaction_candidate::CompactionCandidate;
pub use compaction_job::CompactionJob;
pub use compaction_policy::{
    CompactionPolicy, DEFAULT_LEVEL_WINDOW_NS, DEFAULT_MAX_BLOCKS_PER_JOB, DEFAULT_MAX_LEVEL,
    DEFAULT_TARGET_ROWS_PER_BLOCK,
};
pub use input_key_fingerprint::input_key_fingerprint;
use job_from_run::job_from_run;
pub use plan_compactions::plan_compactions;
