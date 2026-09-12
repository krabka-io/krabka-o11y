use krabka_metrics::{
    DeferredBlockDeletions, MetricCompactionError, MetricCompactionPass, compact_metric_blocks_once,
};

use super::{
    Arc, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX, ObjectStore,
    ObjectStoreCompactionIndexSink, ServiceMetrics,
};

/// Runs one compaction pass and reports what it did.
///
/// The manifests are reloaded inside the pass, so nothing is carried across
/// ticks: the block builder publishes new blocks into the same prefix, and a
/// stale set would plan against blocks a previous pass replaced.
///
/// A pass that planned nothing publishes nothing. Counting its zero output
/// would leave a compactor with nothing to do exporting what a compactor that
/// merged a block exports.
///
/// # Errors
/// Returns an error when the manifests cannot be read, or when a merge cannot
/// read its inputs or write its output.
pub(crate) async fn run_compactor_once(
    store: &Arc<dyn ObjectStore>,
    block_writer: &BlockWriter,
    index_sink: &ObjectStoreCompactionIndexSink,
    policy: CompactionPolicy,
    deferred: &mut DeferredBlockDeletions,
    metrics: &ServiceMetrics,
) -> Result<MetricCompactionPass, MetricCompactionError> {
    let pass = compact_metric_blocks_once(
        store,
        block_writer,
        index_sink,
        policy,
        DEFAULT_BLOCK_READ_MAX,
        deferred,
    )
    .await?;
    metrics.compaction.record_output(pass.outputs.len() as u64);
    Ok(pass)
}
