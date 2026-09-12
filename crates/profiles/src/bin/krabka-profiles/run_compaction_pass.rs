use krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE;
use krabka_profiles::{
    blockbuilder::BLOCK_OBJECT_PREFIX,
    lifecycle::{LifecycleOptions, run_lifecycle_pass},
};

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
    let report = run_lifecycle_pass(
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
    .await?;
    if !report.compacted.is_empty() {
        metrics
            .compaction
            .record_output(report.compacted.len() as u64);
    }
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
    Ok(report.compacted.len())
}
