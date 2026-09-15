use krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE;
use krabka_profiles::{
    ProfilesError,
    blockbuilder::BLOCK_OBJECT_PREFIX,
    lifecycle::{LifecycleOptions, LifecycleReport, run_lifecycle_pass},
};
use object_store::ObjectStoreExt as _;

use super::{
    Arc, Cli, CompactionPolicy, DownsamplePolicy, ObjectStore, OverridesProvider, ProfileIndex,
    ServiceMetrics, SystemTime,
};

/// Loads the index, runs one whole compactor pass, and reports how many
/// replacement blocks it wrote.
///
/// The pass is merge, expire, delete and reconcile in that order. See
/// [`run_lifecycle_pass`] for why the order is the contract.
///
/// # Errors
/// Returns an error when the index cannot be loaded or saved, when a
/// compaction job fails, or when the block prefix cannot be listed.
pub(crate) async fn run_compaction_pass(
    store: &Arc<dyn ObjectStore>,
    index_key: &str,
    cli: &Cli,
    policy: CompactionPolicy,
    downsample: Option<DownsamplePolicy>,
    overrides: &OverridesProvider,
    metrics: &ServiceMetrics,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
        store,
        index_key,
        cli.index_snapshot_max,
    )
    .await?;
    let ingested = index.all_blocks();
    let capability =
        object_store::path::Path::from(krabka_profiles::recording::RECORDING_RULES_ENABLED_KEY);
    if let Some(url) = &cli.recording_rules_remote_write_url {
        store.put(&capability, Vec::new().into()).await?;
        match krabka_profiles::recording::evaluate_compacted_blocks(store, &index, &ingested, url)
            .await
        {
            Ok(requests) if requests > 0 => {
                tracing::info!(requests, "profiles recording rules exported");
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "profiles recording-rule export failed"),
        }
    } else if let Err(error) = store.delete(&capability).await
        && !matches!(error, object_store::Error::NotFound { .. })
    {
        return Err(error.into());
    }
    let result = run_lifecycle_pass(
        store,
        &mut index,
        &LifecycleOptions {
            index_key,
            index_snapshot_retain: cli.index_snapshot_retain,
            policy,
            downsample,
            retention: overrides,
            block_prefix: BLOCK_OBJECT_PREFIX,
            orphan_grace: DEFAULT_BLOCK_SWEEP_GRACE,
            now: SystemTime::now(),
        },
    )
    .await;
    match &result {
        Ok(report) => record_lifecycle_report(metrics, report),
        Err(ProfilesError::Lifecycle { report, .. }) => {
            record_lifecycle_report(metrics, report);
        }
        Err(_) => {}
    }
    let report = result?;
    Ok(report.compacted.len())
}

fn record_lifecycle_report(metrics: &ServiceMetrics, report: &LifecycleReport) {
    if !report.compacted.is_empty() {
        metrics
            .compaction
            .record_output(report.compacted.len() as u64);
    }
    metrics.compaction.record_deleted(
        report.deletions.blocks_deleted as u64,
        report.deletions.sidecars_deleted as u64,
        report.deletions.failures.len() as u64,
    );
    metrics
        .compaction
        .record_orphan_sweep(report.orphans.deleted as u64, report.orphans.failed as u64);
    if report.expired > 0 || report.deletions.blocks_deleted > 0 || report.orphans.deleted > 0 {
        tracing::info!(
            expired_blocks = report.expired,
            deleted_objects = report.deletions.blocks_deleted + report.deletions.sidecars_deleted,
            undeletable_objects = report.deletions.failures.len(),
            orphans_deleted = report.orphans.deleted,
            orphans_kept_within_grace = report.orphans.kept_within_grace,
            "profiles compactor reclaimed block storage"
        );
    }
}
