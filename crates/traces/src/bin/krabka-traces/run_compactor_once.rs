use std::time::SystemTime;

use krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE;

use super::{
    BlockWriter, Cli, CompactionPolicy, ConfiguredObjectStore, OverridesProvider, ServiceMetrics,
    TraceIndex, compact_once_with_policy, delete_trace_blocks, expire_trace_blocks, now_unix_nanos,
    sweep_orphaned_trace_blocks,
};

/// Loads the index, compacts what the policy asks for, expires what retention
/// asks for, publishes the result, and then deletes what the index no longer
/// names. Returns how many replacement blocks the pass wrote.
///
/// The order is what keeps a querier honest. The index is the only record of
/// which blocks are live, so a block leaves the index and the snapshot is saved
/// **before** its object is deleted. If the object went first, a reader that
/// still holds the older snapshot would resolve a key whose object is already
/// gone. This order costs only that an object outlives its index entry until
/// the delete lands, and the orphan sweep collects whatever a torn pass left.
///
/// The index is reloaded per pass rather than carried across ticks: the block
/// builder publishes new blocks into the same snapshot chain, and a stale
/// in-memory copy would plan against blocks that have since been replaced.
pub(crate) async fn run_compactor_once(
    cli: &Cli,
    configured: &ConfiguredObjectStore,
    policy: CompactionPolicy,
    overrides: &OverridesProvider,
    metrics: &ServiceMetrics,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let writer =
        BlockWriter::new(configured.store.clone()).with_metrics(metrics.object_store.clone());
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let mut index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    let now = SystemTime::now();
    let pass = compact_once_with_policy(
        configured.store.clone(),
        &writer,
        &mut index,
        configured.prefix.as_ref(),
        policy,
        cli.block_read_max,
    )
    .await?;
    // A deployment where every window is zero has nothing that can expire, and
    // the plan reads every block of every tenant out of the index to decide it.
    let expired = if overrides.expires_any_blocks() {
        expire_trace_blocks(&mut index, now_unix_nanos(now), overrides)
    } else {
        Vec::new()
    };
    let mut retired = pass.retired_inputs;
    retired.extend(expired.iter().map(|block| block.object_key.clone()));

    // A pass that changed nothing has nothing to publish. A save anyway burns a
    // snapshot generation every tick and evicts the retained history a reader
    // falls back on. The save is also what makes the retired objects safe to
    // delete, so both happen together or neither does.
    if !retired.is_empty() || !pass.outputs.is_empty() {
        metrics.compaction.record_output(pass.outputs.len() as u64);
        index
            .save_latest_snapshot_with_retain(
                &configured.store,
                &trace_index_key,
                cli.index_snapshot_retain,
            )
            .await?;
        let report = delete_trace_blocks(&configured.store, &retired).await;
        if report.failures.is_empty() {
            tracing::debug!(
                blocks_deleted = report.blocks_deleted,
                objects_absent = report.objects_absent,
                expired = expired.len(),
                "traces compactor deleted the blocks its index no longer names"
            );
        } else {
            // Nothing is lost: the blocks are already out of the index, and the
            // orphan sweep reaches them once they are older than its grace.
            tracing::warn!(
                blocks_deleted = report.blocks_deleted,
                failures = report.failures.len(),
                "some retired traces blocks would not delete"
            );
        }
    }

    let swept = sweep_orphaned_trace_blocks(
        &configured.store,
        configured.prefix.as_ref(),
        &trace_index_key,
        &index,
        DEFAULT_BLOCK_SWEEP_GRACE,
        now,
    )
    .await?;
    tracing::debug!(
        listed = swept.listed,
        live = swept.live,
        kept_within_grace = swept.kept_within_grace,
        deleted = swept.deleted,
        failed = swept.failed,
        "traces compactor reconciled the block prefix against its index"
    );
    Ok(pass.outputs.len())
}
