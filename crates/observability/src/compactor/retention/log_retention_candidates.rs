use super::{BlockDescriptor, BlockLevel, CompactionCandidate};

/// Reduces log block descriptors to the records the shared expiry rule reads.
///
/// [`plan_expired_blocks`](krabka_blockstore::plan_expired_blocks) takes the
/// same record every signal offers its compaction planner, so logs answers in
/// that shape rather than holding an expiry rule of its own.
///
/// `max_ts` is [`BlockKey::time_range`](krabka_blockstore::BlockKey)'s
/// `end_ns`, in epoch nanoseconds. The compactor folds it from the event
/// timestamps of the records before it applies any delete filter, and a block
/// rewrite keeps the key it had, so the bound only ever over-estimates how new
/// a block's newest row is. An over-estimate keeps a block one sweep longer
/// than it had to be, which is the safe direction.
///
/// The row count is `0`, because the log index does not record one. Expiry
/// reads `max_ts` alone, and the level says what a compaction may merge rather
/// than what a sweep may delete.
pub(crate) fn log_retention_candidates<'a>(
    blocks: impl IntoIterator<Item = &'a BlockDescriptor>,
) -> Vec<CompactionCandidate> {
    blocks
        .into_iter()
        .map(|block| CompactionCandidate {
            tenant: block.key.tenant.clone(),
            object_key: block.key.object_key(),
            min_ts: block.key.time_range.start_ns,
            max_ts: block.key.time_range.end_ns,
            row_count: 0,
            level: BlockLevel::INGESTED,
        })
        .collect()
}
