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

    struct ExpirationCase {
        name: &'static str,
        max_ts: i64,
        want: bool,
    }

    struct UnitWindowCase {
        unit: BlockTimestampUnit,
        now: i64,
        per_ms: i64,
    }

    struct ClockBoundsCase {
        name: &'static str,
        now: i64,
        max_ts: i64,
        want: bool,
    }

    struct UnitTicksCase {
        unit: BlockTimestampUnit,
        want: i64,
    }

    struct TenantRetentionWindow {
        tenant: String,
        window: Time,
    }

    fn retention_windows(
        entries: impl IntoIterator<Item = TenantRetentionWindow>,
    ) -> BTreeMap<String, Time> {
        let mut windows = BTreeMap::new();
        for entry in entries {
            windows.insert(entry.tenant, entry.window);
        }
        windows
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
        let windows = retention_windows([TenantRetentionWindow {
            tenant: "t".to_string(),
            window: millis(1_000),
        }]);

        for case in [
            ExpirationCase {
                name: "well before the cutoff",
                max_ts: 8_000_i64,
                want: true,
            },
            ExpirationCase {
                name: "one tick before the cutoff",
                max_ts: 8_999,
                want: true,
            },
            ExpirationCase {
                name: "exactly on the cutoff",
                max_ts: 9_000,
                want: false,
            },
            ExpirationCase {
                name: "one tick after the cutoff",
                max_ts: 9_001,
                want: false,
            },
            ExpirationCase {
                name: "still being written",
                max_ts: NOW,
                want: false,
            },
        ] {
            let candidates = vec![candidate("t", "b", case.max_ts)];

            let got = plan_expired_blocks(&candidates, NOW, BlockTimestampUnit::Millis, &windows);

            check!(!got.is_empty() == case.want, "{}", case.name);
        }
    }

    /// Zero is the "no retention" sentinel, so it keeps everything. Reading it
    /// the other way round would make an unconfigured tenant lose its data.
    #[test]
    fn a_zero_window_keeps_every_block_forever() {
        let windows = retention_windows([TenantRetentionWindow {
            tenant: "t".to_string(),
            window: Time::ZERO,
        }]);
        let candidates = vec![
            candidate("t", "ancient", i64::MIN + 1),
            candidate("t", "recent", 0),
        ];

        let got = plan_expired_blocks(&candidates, i64::MAX, BlockTimestampUnit::Millis, &windows);

        check!(got.is_empty());
    }

    #[test]
    fn each_tenant_is_swept_by_its_own_window() {
        let windows = retention_windows([
            TenantRetentionWindow {
                tenant: "short".to_string(),
                window: millis(1_000),
            },
            TenantRetentionWindow {
                tenant: "long".to_string(),
                window: millis(100_000),
            },
        ]);
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
        let windows = retention_windows([TenantRetentionWindow {
            tenant: "t".to_string(),
            window: days(1),
        }]);

        for case in [
            UnitWindowCase {
                unit: BlockTimestampUnit::Millis,
                now: NOW_MS,
                per_ms: 1_i64,
            },
            UnitWindowCase {
                unit: BlockTimestampUnit::Nanos,
                now: NOW_MS * 1_000_000,
                per_ms: 1_000_000,
            },
        ] {
            let candidates = vec![
                candidate("t", "two-days-old", case.now - 2 * DAY_MS * case.per_ms),
                candidate("t", "one-hour-old", case.now - HOUR_MS * case.per_ms),
            ];

            let got = plan_expired_blocks(&candidates, case.now, case.unit, &windows);

            check!(
                expired_keys(&got) == vec!["two-days-old".to_string()],
                "{:?}",
                case.unit
            );
        }
    }

    /// Two compactor passes over one index have to agree on what to delete, so
    /// the plan cannot depend on the order the candidates arrived in.
    #[test]
    fn the_plan_is_ordered_by_tenant_and_then_object_key() {
        let windows = retention_windows([
            TenantRetentionWindow {
                tenant: "a".to_string(),
                window: millis(1),
            },
            TenantRetentionWindow {
                tenant: "b".to_string(),
                window: millis(1),
            },
        ]);
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
        let windows = retention_windows([TenantRetentionWindow {
            tenant: "t".to_string(),
            window: days(365),
        }]);

        for case in [
            ClockBoundsCase {
                name: "the clock at its floor",
                now: i64::MIN,
                max_ts: i64::MIN,
                want: false,
            },
            ClockBoundsCase {
                name: "the clock at its ceiling",
                now: i64::MAX,
                max_ts: i64::MIN,
                want: true,
            },
            ClockBoundsCase {
                name: "a block at the ceiling",
                now: i64::MAX,
                max_ts: i64::MAX,
                want: false,
            },
        ] {
            let candidates = vec![candidate("t", "b", case.max_ts)];

            let got =
                plan_expired_blocks(&candidates, case.now, BlockTimestampUnit::Nanos, &windows);

            check!(!got.is_empty() == case.want, "{}", case.name);
        }
    }

    #[test]
    fn a_window_is_counted_in_the_unit_the_caller_names() {
        for case in [
            UnitTicksCase {
                unit: BlockTimestampUnit::Millis,
                want: 1_000_i64,
            },
            UnitTicksCase {
                unit: BlockTimestampUnit::Nanos,
                want: 1_000_000_000,
            },
        ] {
            check!(
                case.unit.ticks(millis(1_000)) == case.want,
                "{:?}",
                case.unit
            );
        }
    }

    /// A window cannot be negative, and a caller that supplies one means "no
    /// window" rather than "a cutoff in the future".
    #[test]
    fn a_negative_window_counts_as_no_window() {
        let negative = Time::from_millis(-1_000);

        check!(BlockTimestampUnit::Millis.ticks(negative) == 0);
        check!(BlockTimestampUnit::Nanos.ticks(negative) == 0);

        let windows = retention_windows([TenantRetentionWindow {
            tenant: "t".to_string(),
            window: negative,
        }]);
        let candidates = vec![candidate("t", "b", 0)];
        check!(
            plan_expired_blocks(&candidates, 10_000, BlockTimestampUnit::Millis, &windows)
                .is_empty()
        );
    }
}

mod retention_windows;

pub use retention_windows::RetentionWindows;

/// One block to delete, with the sidecar objects that belong to it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockDeletion {
    /// The block object itself.
    pub object_key: String,
    /// Objects written beside the block and named after it.
    pub sidecars: Vec<String>,
}

/// One object a deletion pass could not delete, and why.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockDeletionFailure {
    /// The block the failed object belongs to.
    pub object_key: String,
    /// The object that would not delete.
    pub failed_key: String,
    /// The object store's own message.
    pub error: String,
}

/// What one pass of [`delete_blocks`] did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockDeletionReport {
    /// Block objects the store reported deleted.
    pub blocks_deleted: usize,
    /// Sidecar objects the store reported deleted.
    pub sidecars_deleted: usize,
    /// Objects that were already gone.
    pub objects_absent: usize,
    /// Objects that would not delete. The pass carries on past each one.
    pub failures: Vec<BlockDeletionFailure>,
}

/// How long an object the index does not name is left alone before it may be deleted.
pub const DEFAULT_BLOCK_SWEEP_GRACE: Time = hours(1);

/// One block that has fallen outside its tenant's retention window.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExpiredBlock {
    /// The index whose entry must be removed before deleting the object.
    pub tenant: String,
    pub object_key: String,
}

/// What one pass of orphan reconciliation found and did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OrphanSweepStats {
    pub listed: usize,
    pub live: usize,
    /// Objects too new to judge as orphans.
    pub kept_within_grace: usize,
    pub deleted: usize,
    pub absent: usize,
    pub failed: usize,
}

/// Deletes each block and every sidecar that belongs to it.
///
/// One object that will not delete does not end the pass. A retention sweep
/// covers many tenants, and a single object that a backend refuses would
/// otherwise stop every block behind it in the list from ever being deleted.
/// Each failure is reported against its block in
/// [`BlockDeletionReport::failures`] and the pass carries on, so a caller can
/// log what failed and still credit what worked.
///
/// An object that is already gone counts as success. A sweep that ran before
/// and was interrupted, and a sweep that runs twice, both reach objects that
/// no longer exist, and neither is a fault.
///
/// The deletes are issued one at a time rather than through
/// [`ObjectStore::delete_stream`]. The stream reports a failure without
/// naming the object that caused it, which is exactly what a per-block report
/// needs. [`reconcile_orphans`](super::reconcile_orphans) has no such
/// attribution to keep and does use the stream.
#[instrument(
    level = "debug",
    skip_all,
    fields(
        blocks = deletions.len(),
        deleted = tracing::field::Empty,
        failed = tracing::field::Empty,
    )
)]
pub async fn delete_blocks(
    store: &dyn ObjectStore,
    deletions: &[BlockDeletion],
) -> BlockDeletionReport {
    let mut report = BlockDeletionReport::default();
    for deletion in deletions {
        let mut sidecar_failed = false;
        for key in &deletion.sidecars {
            match store.delete(&Path::from(key.as_str())).await {
                Ok(()) => report.sidecars_deleted += 1,
                Err(object_store::Error::NotFound { .. }) => report.objects_absent += 1,
                Err(error) => {
                    sidecar_failed = true;
                    report.failures.push(BlockDeletionFailure {
                        object_key: deletion.object_key.clone(),
                        failed_key: key.clone(),
                        error: error.to_string(),
                    });
                }
            }
        }
        if sidecar_failed {
            continue;
        }
        match store
            .delete(&Path::from(deletion.object_key.as_str()))
            .await
        {
            Ok(()) => report.blocks_deleted += 1,
            Err(object_store::Error::NotFound { .. }) => report.objects_absent += 1,
            Err(error) => report.failures.push(BlockDeletionFailure {
                object_key: deletion.object_key.clone(),
                failed_key: deletion.object_key.clone(),
                error: error.to_string(),
            }),
        }
    }

    tracing::Span::current().record("deleted", report.blocks_deleted + report.sidecars_deleted);
    tracing::Span::current().record("failed", report.failures.len());
    report
}

use krabka_o11y_verified::retention_cutoff;

/// Names the blocks that end before their tenant's retention cutoff.
///
/// A block expires when its `max_ts` is strictly older than
/// `now_ts - retention`, both counted in `unit`. The newest sample decides,
/// not the oldest: a block that still holds one sample inside the window is
/// still readable data, and deleting it would answer a query that the window
/// covers with a hole.
///
/// A tenant whose window is `Time::ZERO` never expires a block. See
/// [`RetentionWindows::block_retention`].
///
/// The result is ordered by tenant and then by object key. Two passes over one
/// index therefore produce the same plan, which is what lets a caller compare
/// them, log them, or apply them in batches.
///
/// The arithmetic saturates, so a block or a clock at either end of the `i64`
/// range does not panic the compactor.
#[must_use]
pub fn plan_expired_blocks(
    candidates: &[CompactionCandidate],
    now_ts: i64,
    unit: BlockTimestampUnit,
    windows: &dyn RetentionWindows,
) -> Vec<ExpiredBlock> {
    // One lookup per tenant rather than one per block: a tenant's window is
    // configuration, and a large index holds far more blocks than tenants.
    let mut cutoffs: BTreeMap<&str, Option<i64>> = BTreeMap::new();
    let mut expired: Vec<ExpiredBlock> = candidates
        .iter()
        .filter(|candidate| {
            let cutoff = *cutoffs.entry(candidate.tenant.as_str()).or_insert_with(|| {
                let ticks = unit.ticks(windows.block_retention(&candidate.tenant));
                retention_cutoff(now_ts, ticks)
            });
            cutoff.is_some_and(|cutoff| candidate.max_ts < cutoff)
        })
        .map(|candidate| ExpiredBlock {
            tenant: candidate.tenant.clone(),
            object_key: candidate.object_key.clone(),
        })
        .collect();
    expired.sort();
    expired
}

use futures::StreamExt as _;

/// Deletes the objects under `prefix` that `live_keys` does not name.
///
/// An index is the only record of which blocks are live, so an object the
/// index does not name is unreachable and nothing will ever read it. A write
/// that failed after it put the block, a compaction whose index save lost the
/// race, and a deletion pass that was interrupted all leave one behind.
///
/// # The grace window
///
/// `grace` is what keeps the sweep from deleting a live block. **A block is
/// written before its index entry is saved**, so a writer that has put its
/// block and not yet published it is indistinguishable from one that never
/// will: both are objects no index names. Without the grace window this
/// deletes freshly-written blocks, including ones another replica is
/// publishing at that moment.
///
/// An object whose `last_modified` is newer than `now - grace` is therefore
/// left alone, and so is an object the sweep cannot date. Pass
/// [`DEFAULT_BLOCK_SWEEP_GRACE`](super::DEFAULT_BLOCK_SWEEP_GRACE) unless the
/// caller knows its own publication is faster.
///
/// # Scope
///
/// Every object under `prefix` that `live_keys` does not name is deleted, so
/// `prefix` must name a location the blocks own. A prefix shared with anything
/// else loses that other thing.
///
/// One object that will not delete does not end the pass; it is counted in
/// [`OrphanSweepStats::failed`] and the next pass sees it again. An object
/// already gone counts in [`OrphanSweepStats::absent`].
///
/// # Errors
/// Returns [`LifecycleError::ObjectStore`] when listing `prefix` fails. The
/// sweep then knows nothing about what exists, and deleting on that basis
/// would delete live blocks.
#[instrument(
    level = "debug",
    skip_all,
    fields(
        prefix = %prefix,
        listed = tracing::field::Empty,
        deleted = tracing::field::Empty,
    ),
    err
)]
pub async fn reconcile_orphans(
    store: &dyn ObjectStore,
    prefix: &str,
    live_keys: &BTreeSet<String>,
    grace: Time,
    now: SystemTime,
) -> Result<OrphanSweepStats, LifecycleError> {
    let cutoff = now
        .checked_sub(grace.to_std())
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_secs()).ok());

    let mut stats = OrphanSweepStats::default();
    let mut stale = Vec::new();
    let mut listing = store.list(Some(&Path::from(prefix)));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        stats.listed += 1;
        if live_keys.contains(meta.location.as_ref()) {
            stats.live += 1;
            continue;
        }
        // At or before the cutoff, not strictly before it: object timestamps
        // are whole seconds, and a zero grace has to mean "sweep everything"
        // rather than "sweep everything written in a previous second".
        if cutoff.is_some_and(|cutoff| meta.last_modified.timestamp() <= cutoff) {
            stale.push(meta.location);
        } else {
            stats.kept_within_grace += 1;
        }
    }

    let locations = futures::stream::iter(stale.into_iter().map(Ok)).boxed();
    let mut deletions = store.delete_stream(locations);
    while let Some(outcome) = deletions.next().await {
        match outcome {
            Ok(_) => stats.deleted += 1,
            Err(object_store::Error::NotFound { .. }) => stats.absent += 1,
            Err(error) => {
                stats.failed += 1;
                // The stream does not say which object failed, so the message
                // is all there is to report. The object stays, and the next
                // pass reaches it again.
                tracing::warn!(%error, %prefix, "could not delete an orphaned block object");
            }
        }
    }

    tracing::Span::current().record("listed", stats.listed);
    tracing::Span::current().record("deleted", stats.deleted);
    Ok(stats)
}

/// The unit a signal's block timestamps count in.
///
/// A block's `min_ts` and `max_ts` are plain `i64` ticks, and the signals do
/// not agree on what a tick is. A retention window is a [`Time`], so something
/// has to say which unit to express it in before the two can be compared. A
/// caller that names the unit here cannot be wrong by a factor of a million,
/// and an `i64` "now" paired with the wrong unit fails to compile rather than
/// expiring the wrong blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BlockTimestampUnit {
    /// Epoch milliseconds, as the metrics and profiles blocks count.
    ///
    /// A profile row's field is named `timestamp_ns` and holds a millisecond
    /// value, so the field name is not evidence of the unit here.
    Millis,
    /// Epoch nanoseconds, as the traces blocks count.
    Nanos,
}

impl BlockTimestampUnit {
    /// `extent` as a count of ticks in this unit.
    ///
    /// A negative extent is not a window, so it counts as zero ticks. The
    /// callers read zero as "keep forever", which is what an operator who
    /// configured a negative window meant.
    #[must_use]
    pub fn ticks(self, extent: Time) -> i64 {
        let ticks = match self {
            Self::Millis => extent.millis_i64(),
            Self::Nanos => extent.nanos_i64(),
        };
        ticks.max(0)
    }
}

/// Errors raised while a block lifecycle pass runs.
///
/// Deleting one object is not one of them. A sweep that stopped at the first
/// failed delete would leave the rest of the pass undone and retry the same
/// object forever, so [`delete_blocks`](delete_blocks) and
/// [`reconcile_orphans`](reconcile_orphans) report a failed delete and
/// carry on. Only a failure that leaves the pass unable to tell a live block
/// from a dead one reaches here.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// Listing the prefix failed, so the sweep never learned which objects
    /// exist. It cannot tell an orphan from a live block, and deleting on that
    /// basis would delete live blocks.
    #[error("object store error: {0}")]
    ObjectStore(String),
}

impl From<object_store::Error> for LifecycleError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}
