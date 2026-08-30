use super::{
    Arc, BlockBuilderConfig, BlockWriter, FlushAccumulator, Mutex, ObjectStore, ServiceMetrics,
    TraceIndex, TracesError, WalConsumerCommit, flush_partition_windows,
};

/// Flush the accumulated buffer to durable blocks, then commit WAL offsets.
///
/// This drains the accumulator first, so a flush error leaves nothing
/// double-counted. `commit_sync` runs only after `flush_partition_windows`
/// reports that the blocks and the trace index are durably written.
pub(crate) async fn flush_and_commit<C>(
    consumer: &mut C,
    writer: &BlockWriter,
    index: &Arc<Mutex<TraceIndex>>,
    object_store: &Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    metrics: &ServiceMetrics,
    accumulator: &mut FlushAccumulator,
) -> Result<(), TracesError>
where
    C: WalConsumerCommit,
{
    let windows = accumulator.take();
    let blocks = {
        let mut guard = index.lock().await;
        flush_partition_windows(
            writer,
            &mut guard,
            Arc::clone(object_store),
            config,
            windows,
        )
        .await?
    };
    // One counter tick per durably-written span block (post-flush, pre-commit).
    for _ in 0..blocks {
        metrics.record_block_flushed();
    }
    consumer.commit_sync().await
}
