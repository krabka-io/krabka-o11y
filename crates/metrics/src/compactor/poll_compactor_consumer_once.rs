use super::*;

/// Polls, compacts, and commits once with a single mutable consumer handle.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_consumer_once<C, S>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    wal_topic: &str,
    timeout: Time,
) -> Result<CompactionPollResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    let polled_records = records.len();
    let wal_records = compaction_wal_records_from_consumer_records(wal_topic, &records)?;
    let compacted_records = wal_records.len();
    let batch = process_compaction_record_batch_with_consumer(
        block_writer,
        index_sink,
        consumer,
        &wal_records,
    )
    .await?;

    Ok(CompactionPollResult {
        polled_records,
        compacted_records,
        batch,
    })
}
