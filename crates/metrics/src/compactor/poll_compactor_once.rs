use super::{
    BlockWriter, CompactionConsumerPoll, CompactionIndexSink, CompactionOffsetCommitter,
    CompactionPollError, CompactionPollResult, ServiceMetrics, Time, poll_compaction_wal_records,
    process_compaction_record_batch,
};

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
    metrics: &ServiceMetrics,
) -> Result<CompactionPollResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let poll_result = poll_compaction_wal_records(poller, wal_topic, timeout, metrics).await?;
    let compacted_records = poll_result.wal_records.len();
    let batch = process_compaction_record_batch(
        block_writer,
        index_sink,
        committer,
        &poll_result.wal_records,
    )
    .await?;

    Ok(CompactionPollResult {
        polled_records: poll_result.polled_records,
        compacted_records,
        batch,
    })
}
