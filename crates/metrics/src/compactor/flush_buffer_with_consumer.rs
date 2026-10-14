use std::time::Instant;

use krabka_units::{Time, convert::TimeExt as _};

use super::{
    BlockWriter, CompactionConsumerCommitMut, CompactionIndexSink, CompactionLoopResult,
    CompactionPartitionOffset, CompactionPollError, CompactionWalRecord, ServiceMetrics,
    process_compaction_record_batch_with_consumer,
    record_compaction_batch::record_compaction_batch,
};

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
