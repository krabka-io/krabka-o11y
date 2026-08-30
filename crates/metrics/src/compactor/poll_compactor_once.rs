use super::{CompactionConsumerPoll, CompactionIndexSink, CompactionOffsetCommitter, BlockWriter, Time, CompactionPollResult, CompactionPollError, compaction_wal_records_from_consumer_records, process_compaction_record_batch};

/// Polls the metrics WAL consumer once, compacts the returned records, and
/// commits on success.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_once<P, S, C>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    wal_topic: &str,
    timeout: Time,
) -> Result<CompactionPollResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let records = poller.poll(timeout).await?;
    let polled_records = records.len();
    let wal_records = compaction_wal_records_from_consumer_records(wal_topic, &records)?;
    let compacted_records = wal_records.len();
    let batch =
        process_compaction_record_batch(block_writer, index_sink, committer, &wal_records).await?;

    Ok(CompactionPollResult {
        polled_records,
        compacted_records,
        batch,
    })
}
