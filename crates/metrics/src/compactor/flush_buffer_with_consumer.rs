use super::{BlockWriter, CompactionConsumerCommitMut, CompactionIndexSink, CompactionLoopResult, CompactionPartitionOffset, CompactionPollError, CompactionWalRecord, process_compaction_record_batch_with_consumer};

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
) -> Result<Vec<CompactionPartitionOffset>, CompactionPollError>
where
    C: CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let batch =
        process_compaction_record_batch_with_consumer(block_writer, index_sink, consumer, records)
            .await?;
    summary.writes += batch.writes.len();
    summary
        .committed_offsets
        .extend(batch.committed_offsets.iter().cloned());
    Ok(batch.committed_offsets)
}
