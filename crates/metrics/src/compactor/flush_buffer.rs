use std::time::Instant;

use krabka_units::{Time, convert::TimeExt as _};

use super::{
    BlockWriter, CompactionIndexSink, CompactionLoopResult, CompactionOffsetCommitter,
    CompactionPartitionOffset, CompactionPollError, CompactionWalRecord, ServiceMetrics,
    process_compaction_record_batch,
};

/// Writes one block from the buffered records, commits their offsets, and folds
/// the result into the running loop summary.
///
/// CORRECTNESS: `process_compaction_record_batch` writes every partition's block
/// and index sidecar durably *before* it commits any offsets, and then commits
/// once after all writes succeed. Offsets advance only after the accumulated
/// blocks are durable. The caller empties the buffer with `take` *before* this
/// call, so a write or commit error leaves the buffer empty and the next poll
/// re-reads from the last committed offset. That is at-least-once.
pub(crate) async fn flush_buffer<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    records: &[CompactionWalRecord],
    summary: &mut CompactionLoopResult,
    metrics: &ServiceMetrics,
) -> Result<Vec<CompactionPartitionOffset>, CompactionPollError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    if records.is_empty() {
        return Ok(Vec::new());
    }
    // The flush is the compactor's unit of work, so it is the unit the run
    // counter counts, and a flush that fails is counted as well.
    let started = Instant::now();
    let outcome =
        process_compaction_record_batch(block_writer, index_sink, committer, records).await;
    metrics
        .compaction
        .record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
    let batch = outcome?;
    metrics.compaction.record_output(batch.writes.len() as u64);
    metrics.record_blocks_compacted(batch.writes.len() as u64);
    summary.writes += batch.writes.len();
    summary
        .committed_offsets
        .extend(batch.committed_offsets.iter().cloned());
    Ok(batch.committed_offsets)
}
