use super::{
    BlockWriter, CompactionConsumerCommitMut, CompactionConsumerPoll, CompactionIndexSink,
    CompactionPollError, CompactionPollResult, ServiceMetrics, Time, poll_compaction_wal_records,
    process_compaction_record_batch_with_consumer,
};

/// Polls, compacts, and commits once with a single mutable consumer handle.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_consumer_once<C, S>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    wal_topic: &str,
    timeout: Time,
    metrics: &ServiceMetrics,
) -> Result<CompactionPollResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    let polled = poll_compaction_wal_records(consumer, wal_topic, timeout, metrics).await?;
    let compacted_records = polled.wal_records.len();
    let batch = process_compaction_record_batch_with_consumer(
        block_writer,
        index_sink,
        consumer,
        &polled.wal_records,
    )
    .await?;

    Ok(CompactionPollResult {
        polled_records: polled.polled_records,
        compacted_records,
        batch,
    })
}
