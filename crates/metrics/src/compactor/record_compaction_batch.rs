use super::{CompactionBatchResult, CompactionLoopResult, ServiceMetrics};

pub(crate) fn record_compaction_batch(
    batch: CompactionBatchResult,
    summary: &mut CompactionLoopResult,
    metrics: &ServiceMetrics,
) -> Vec<super::CompactionPartitionOffset> {
    metrics.record_blocks_compacted(batch.writes.len() as u64);
    summary.writes += batch.writes.len();
    summary
        .committed_offsets
        .extend(batch.committed_offsets.iter().cloned());
    batch.committed_offsets
}
