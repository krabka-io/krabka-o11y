use super::{
    BlockWriter, Cli, CompactionPolicy, ConfiguredObjectStore, ServiceMetrics, TraceIndex,
    compact_once_with_policy,
};

/// Loads the index, compacts what the policy asks for, and publishes the
/// result. Returns how many replacement blocks the pass wrote.
///
/// The index is reloaded per pass rather than carried across ticks: the block
/// builder publishes new blocks into the same snapshot chain, and a stale
/// in-memory copy would plan against blocks that have since been replaced.
pub(crate) async fn run_compactor_once(
    cli: &Cli,
    configured: &ConfiguredObjectStore,
    policy: CompactionPolicy,
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
    let metas = compact_once_with_policy(
        configured.store.clone(),
        &writer,
        &mut index,
        configured.prefix.as_ref(),
        policy,
        cli.block_read_max,
    )
    .await?;
    // A pass that planned nothing has nothing to publish. Saving anyway would
    // burn a snapshot generation every tick and evict the retained history
    // that a reader falls back on.
    if metas.is_empty() {
        return Ok(0);
    }
    metrics.compaction.record_output(metas.len() as u64);
    index
        .save_latest_snapshot_with_retain(
            &configured.store,
            &trace_index_key,
            cli.index_snapshot_retain,
        )
        .await?;
    Ok(metas.len())
}
