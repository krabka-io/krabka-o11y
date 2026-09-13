use krabka_observability::CancellationToken;

use super::{Arc, ObjectStore, OverridesProvider, ServiceMetrics, SystemTime, Time, TimeExt};

/// Runs the compaction-retention sweep on a timer, returning its handle for
/// the caller to supervise.
///
/// A sweep that merely fails is not an exit: the loop logs it and tries again
/// on the next interval. An exit means the sweeper is gone, and a compactor
/// that has stopped enforcing retention says nothing about it -- the object
/// store simply keeps growing until something else notices.
///
/// `overrides` holds the window of every tenant, so the sweep reads one window
/// per tenant it finds rather than one window for the whole bucket. A
/// deployment where no tenant has a window still runs the sweep, because the
/// same pass reconciles the objects no manifest names.
// cargo-mutants: background wall-clock loop is exercised through compactor integration.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_retention_sweeper(
    store: Arc<dyn ObjectStore>,
    overrides: Arc<OverridesProvider>,
    sweep_interval: Time,
    stopping: CancellationToken,
    metrics: ServiceMetrics,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match krabka_metrics::enforce_compaction_retention(
                &store,
                SystemTime::now(),
                overrides.as_ref(),
            )
            .await
            {
                Ok(stats) => record_retention_stats(&metrics, &stats),
                Err(error) => {
                    if let Some(stats) = error.partial_stats() {
                        record_retention_stats(&metrics, stats);
                    }
                    tracing::warn!(%error, "metrics compactor retention sweep failed");
                }
            }
            tokio::select! {
                () = stopping.cancelled() => break,
                () = tokio::time::sleep(sweep_interval.to_std()) => {}
            }
        }
    })
}

fn record_retention_stats(
    metrics: &ServiceMetrics,
    stats: &krabka_metrics::CompactionRetentionStats,
) {
    metrics.compaction.record_deleted(
        stats.blocks_deleted.deleted as u64,
        stats.manifests_retired.deleted as u64,
        stats.failures().count() as u64,
    );
    metrics
        .compaction
        .record_orphan_sweep(stats.orphans.deleted as u64, stats.orphans.failed as u64);
    for failure in stats.failures() {
        tracing::warn!(
            block = %failure.object_key,
            object = %failure.failed_key,
            error = %failure.error,
            "metrics compactor retention could not delete an object"
        );
    }
    if stats.deleted_anything() {
        tracing::info!(
            manifests_scanned = stats.manifests_scanned,
            manifests_retired = stats.manifests_retired.deleted,
            blocks_deleted = stats.blocks_deleted.deleted,
            manifests_absent = stats.manifests_retired.absent,
            blocks_absent = stats.blocks_deleted.absent,
            orphans_deleted = stats.orphans.deleted,
            orphans_failed = stats.orphans.failed,
            "metrics compactor retention deleted old blocks"
        );
    }
}
