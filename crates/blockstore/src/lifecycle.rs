//! Block lifecycle: retention planning, deletion, and orphan reconciliation.
//!
//! Compaction decides which blocks to merge. This module decides which blocks
//! to *delete*, and deletes them. The two halves are deliberately apart: a
//! plan is pure and testable, and only [`delete_blocks`] and
//! [`reconcile_orphans`] touch object storage.
//!
//! Three pieces make up the lifecycle a block has after it is written:
//!
//! - [`plan_expired_blocks`] names the blocks that fall outside their tenant's
//!   [`RetentionWindows`] window. It reads the same [`CompactionCandidate`]
//!   records the compaction planner reads, so a signal that can offer one
//!   planner its blocks can offer the other.
//! - [`delete_blocks`] removes a block and its sidecars from object storage.
//!   One block that cannot be deleted does not stop the rest.
//! - [`reconcile_orphans`] deletes the objects under a prefix that the index
//!   does not name. A grace window keeps it off the blocks a writer has put
//!   but not yet published.
//!
//! An index is what says a block is live, so a block leaves the index before
//! it leaves object storage. The removal seams are
//! [`TraceIndex::remove_trace_blocks`](crate::TraceIndex::remove_trace_blocks),
//! [`ProfileIndex::remove_profile_blocks`](crate::ProfileIndex::remove_profile_blocks),
//! and [`Index::remove_blocks`](crate::Index::remove_blocks).
//!
//! # Timestamp units
//!
//! Block bounds are plain `i64` ticks and the signals do not agree on what a
//! tick is. Metrics and profiles count epoch milliseconds; traces counts epoch
//! nanoseconds. A retention window is a [`Time`], so a caller says which unit
//! its blocks count in with [`BlockTimestampUnit`]. A single `i64` "now" would
//! mis-expire by a factor of a million and report nothing wrong.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use krabka_units::{Time, convert::TimeExt, hours};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tracing::instrument;

use crate::compaction::CompactionCandidate;

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::{days, millis};

    use super::*;
    use crate::compaction::BlockLevel;

    /// A per-tenant window table. A tenant it does not name keeps its blocks
    /// forever, which is what an unconfigured tenant does in production.
    struct Windows(BTreeMap<String, Time>);

    impl Windows {
        fn new(entries: &[(&str, Time)]) -> Self {
            Self(
                entries
                    .iter()
                    .map(|(tenant, window)| ((*tenant).to_string(), *window))
                    .collect(),
            )
        }
    }

    impl RetentionWindows for Windows {
        fn block_retention(&self, tenant: &str) -> Time {
            self.0.get(tenant).copied().unwrap_or(Time::ZERO)
        }
    }

    fn candidate(tenant: &str, object_key: &str, max_ts: i64) -> CompactionCandidate {
        CompactionCandidate {
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            min_ts: max_ts,
            max_ts,
            row_count: 1,
            level: BlockLevel::INGESTED,
        }
    }

    fn expired_keys(expired: &[ExpiredBlock]) -> Vec<String> {
        expired
            .iter()
            .map(|block| block.object_key.clone())
            .collect()
    }

    /// The cutoff is `now - retention`, and a block is expired only once it
    /// ends strictly before it. A block that ends exactly on the cutoff still
    /// holds a sample the window covers.
    #[test]
    fn a_block_expires_only_once_it_ends_before_the_cutoff() {
        const NOW: i64 = 10_000;
        let windows = Windows::new(&[("t", millis(1_000))]);

        for (name, max_ts, want) in [
            ("well before the cutoff", 8_000_i64, true),
            ("one tick before the cutoff", 8_999, true),
            ("exactly on the cutoff", 9_000, false),
            ("one tick after the cutoff", 9_001, false),
            ("still being written", NOW, false),
        ] {
            let candidates = vec![candidate("t", "b", max_ts)];

            let got = plan_expired_blocks(&candidates, NOW, BlockTimestampUnit::Millis, &windows);

            check!(!got.is_empty() == want, "{name}");
        }
    }

    /// Zero is the "no retention" sentinel, so it keeps everything. Reading it
    /// the other way round would make an unconfigured tenant lose its data.
    #[test]
    fn a_zero_window_keeps_every_block_forever() {
        let windows = Windows::new(&[("t", Time::ZERO)]);
        let candidates = vec![
            candidate("t", "ancient", i64::MIN + 1),
            candidate("t", "recent", 0),
        ];

        let got = plan_expired_blocks(&candidates, i64::MAX, BlockTimestampUnit::Millis, &windows);

        check!(got.is_empty());
    }

    #[test]
    fn each_tenant_is_swept_by_its_own_window() {
        let windows = Windows::new(&[("short", millis(1_000)), ("long", millis(100_000))]);
        let candidates = vec![
            candidate("short", "short-block", 5_000),
            candidate("long", "long-block", 5_000),
            candidate("unlisted", "unlisted-block", 5_000),
        ];

        let got = plan_expired_blocks(&candidates, 10_000, BlockTimestampUnit::Millis, &windows);

        // All three blocks are five seconds old. `short` keeps one second, so
        // its block goes. `long` keeps a hundred, and `unlisted` has no window
        // at all, so both stay.
        check!(expired_keys(&got) == vec!["short-block".to_string()]);
    }

    /// The unit hazard, pinned in both directions.
    ///
    /// Metrics and profiles count epoch milliseconds and traces counts epoch
    /// nanoseconds. A planner that measured one signal's blocks with the
    /// other's unit would be wrong by a factor of a million, and it would be
    /// wrong silently: in nanoseconds a one-day window read as milliseconds
    /// expires a block that is an hour old.
    #[test]
    fn one_wall_clock_window_expires_the_same_blocks_in_millis_and_nanos() {
        const NOW_MS: i64 = 1_700_000_000_000;
        const HOUR_MS: i64 = 60 * 60 * 1_000;
        const DAY_MS: i64 = 24 * HOUR_MS;
        let windows = Windows::new(&[("t", days(1))]);

        for (unit, now, per_ms) in [
            (BlockTimestampUnit::Millis, NOW_MS, 1_i64),
            (BlockTimestampUnit::Nanos, NOW_MS * 1_000_000, 1_000_000),
        ] {
            let candidates = vec![
                candidate("t", "two-days-old", now - 2 * DAY_MS * per_ms),
                candidate("t", "one-hour-old", now - HOUR_MS * per_ms),
            ];

            let got = plan_expired_blocks(&candidates, now, unit, &windows);

            check!(
                expired_keys(&got) == vec!["two-days-old".to_string()],
                "{unit:?}"
            );
        }
    }

    /// Two compactor passes over one index have to agree on what to delete, so
    /// the plan cannot depend on the order the candidates arrived in.
    #[test]
    fn the_plan_is_ordered_by_tenant_and_then_object_key() {
        let windows = Windows::new(&[("a", millis(1)), ("b", millis(1))]);
        let candidates = vec![
            candidate("b", "z", 0),
            candidate("a", "y", 0),
            candidate("b", "a", 0),
            candidate("a", "x", 0),
        ];

        let got = plan_expired_blocks(&candidates, 1_000, BlockTimestampUnit::Millis, &windows);

        check!(
            got == vec![
                ExpiredBlock {
                    tenant: "a".to_string(),
                    object_key: "x".to_string(),
                },
                ExpiredBlock {
                    tenant: "a".to_string(),
                    object_key: "y".to_string(),
                },
                ExpiredBlock {
                    tenant: "b".to_string(),
                    object_key: "a".to_string(),
                },
                ExpiredBlock {
                    tenant: "b".to_string(),
                    object_key: "z".to_string(),
                },
            ]
        );
    }

    /// A block with a broken clock, or a clock at either end of its range,
    /// must not panic the compactor.
    #[test]
    fn a_clock_at_the_bounds_of_the_range_saturates_rather_than_overflowing() {
        let windows = Windows::new(&[("t", days(365))]);

        for (name, now, max_ts, want) in [
            ("the clock at its floor", i64::MIN, i64::MIN, false),
            ("the clock at its ceiling", i64::MAX, i64::MIN, true),
            ("a block at the ceiling", i64::MAX, i64::MAX, false),
        ] {
            let candidates = vec![candidate("t", "b", max_ts)];

            let got = plan_expired_blocks(&candidates, now, BlockTimestampUnit::Nanos, &windows);

            check!(!got.is_empty() == want, "{name}");
        }
    }

    #[test]
    fn a_window_is_counted_in_the_unit_the_caller_names() {
        for (unit, want) in [
            (BlockTimestampUnit::Millis, 1_000_i64),
            (BlockTimestampUnit::Nanos, 1_000_000_000),
        ] {
            check!(unit.ticks(millis(1_000)) == want, "{unit:?}");
        }
    }

    /// A window cannot be negative, and a caller that supplies one means "no
    /// window" rather than "a cutoff in the future".
    #[test]
    fn a_negative_window_counts_as_no_window() {
        let negative = Time::from_millis(-1_000);

        check!(BlockTimestampUnit::Millis.ticks(negative) == 0);
        check!(BlockTimestampUnit::Nanos.ticks(negative) == 0);

        let windows = Windows::new(&[("t", negative)]);
        let candidates = vec![candidate("t", "b", 0)];
        check!(
            plan_expired_blocks(&candidates, 10_000, BlockTimestampUnit::Millis, &windows)
                .is_empty()
        );
    }
}

mod block_deletion;
mod block_deletion_failure;
mod block_deletion_report;
mod block_timestamp_unit;
mod default_block_sweep_grace;
mod delete_blocks;
mod expired_block;
mod lifecycle_error;
mod orphan_sweep_stats;
mod plan_expired_blocks;
mod reconcile_orphans;
mod retention_windows;

pub use block_deletion::BlockDeletion;
pub use block_deletion_failure::BlockDeletionFailure;
pub use block_deletion_report::BlockDeletionReport;
pub use block_timestamp_unit::BlockTimestampUnit;
pub use default_block_sweep_grace::DEFAULT_BLOCK_SWEEP_GRACE;
pub use delete_blocks::delete_blocks;
pub use expired_block::ExpiredBlock;
pub use lifecycle_error::LifecycleError;
pub use orphan_sweep_stats::OrphanSweepStats;
pub use plan_expired_blocks::plan_expired_blocks;
pub use reconcile_orphans::reconcile_orphans;
pub use retention_windows::RetentionWindows;
