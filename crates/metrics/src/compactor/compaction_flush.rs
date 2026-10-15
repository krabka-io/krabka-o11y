use std::time::Instant;

use krabka_units::{Time, convert::TimeExt as _};

use super::*;

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
    Ok(record_compaction_batch(outcome?, summary, metrics))
}

/// Writes one block from the buffered records and commits through the consumer
/// handle. It folds the result into the running summary and returns the offsets
/// this flush committed.
///
/// CORRECTNESS: `process_compaction_record_batch_with_consumer` writes the block
/// and index sidecar durably before `commit_sync_mut`, so it commits offsets
/// only after the accumulated block is durable.
pub(crate) async fn flush_buffer_with_consumer<C, S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    consumer: &mut C,
    records: &[CompactionWalRecord],
    summary: &mut CompactionLoopResult,
    metrics: &ServiceMetrics,
) -> Result<Vec<CompactionPartitionOffset>, CompactionPollError>
where
    C: CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    if records.is_empty() {
        return Ok(Vec::new());
    }
    // The flush is the compactor's unit of work, so it is the unit the run
    // counter counts. A flush that fails is counted too: the loop propagates
    // the error and the role exits, and without the counter the only record
    // that the flush ran at all is the log.
    let started = Instant::now();
    let outcome =
        process_compaction_record_batch_with_consumer(block_writer, index_sink, consumer, records)
            .await;
    metrics
        .compaction
        .record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
    Ok(record_compaction_batch(outcome?, summary, metrics))
}

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
