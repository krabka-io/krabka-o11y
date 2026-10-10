use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

use crate::planner::StepGrid;

/// Sample accounting gathered at the engine's existing fetched-row count seams.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct QuerySampleStats {
    pub(crate) total_queryable_samples: u64,
    pub(crate) peak_samples: usize,
    pub(crate) per_step: BTreeMap<i64, u64>,
}

/// Whether [`collect_query_sample_stats`] breaks the queryable samples down
/// by evaluation step, as Prometheus does for `stats=all`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PerStepSampleStats {
    /// Report only the totals.
    Omit,
    /// Also report a per-step count for every instant on the grid, zero
    /// included.
    Include(StepGrid),
}

type SharedQuerySampleStats = Arc<Mutex<QuerySampleStats>>;

tokio::task_local! {
    static QUERY_SAMPLE_STATS: SharedQuerySampleStats;
    static QUERY_STATS_STEP_MS: i64;
}

/// Runs `future` with query sample accounting enabled.
pub(crate) async fn collect_query_sample_stats<F>(
    per_step: PerStepSampleStats,
    future: F,
) -> (F::Output, QuerySampleStats)
where
    F: Future,
{
    let mut stats = QuerySampleStats::default();
    if let PerStepSampleStats::Include(grid) = per_step {
        let mut timestamp_ms = grid.start;
        while timestamp_ms <= grid.end {
            stats.per_step.insert(timestamp_ms, 0);
            let next = timestamp_ms.saturating_add(grid.step);
            if next <= timestamp_ms {
                break;
            }
            timestamp_ms = next;
        }
    }
    let shared = Arc::new(Mutex::new(stats));
    let output = QUERY_SAMPLE_STATS.scope(Arc::clone(&shared), future).await;
    let stats = shared.lock().expect("query sample stats poisoned").clone();
    (output, stats)
}

/// Associates sample observations made by `future` with an evaluation step.
pub(crate) async fn query_stats_step<F>(timestamp_ms: i64, future: F) -> F::Output
where
    F: Future,
{
    QUERY_STATS_STEP_MS.scope(timestamp_ms, future).await
}

pub(super) fn query_stats_enabled() -> bool {
    QUERY_SAMPLE_STATS.try_with(|_| ()).is_ok()
}

pub(super) fn record_queryable_samples(count: usize) {
    let Ok(shared) = QUERY_SAMPLE_STATS.try_with(Arc::clone) else {
        return;
    };
    let mut stats = shared.lock().expect("query sample stats poisoned");
    let count_u64 = u64::try_from(count).unwrap_or(u64::MAX);
    stats.total_queryable_samples = stats.total_queryable_samples.saturating_add(count_u64);
    stats.peak_samples = stats.peak_samples.max(count);
    if let Ok(timestamp_ms) = QUERY_STATS_STEP_MS.try_with(|timestamp_ms| *timestamp_ms)
        && let Some(step) = stats.per_step.get_mut(&timestamp_ms)
    {
        *step = step.saturating_add(count_u64);
    }
}
