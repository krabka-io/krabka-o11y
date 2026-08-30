use super::{
    BlockWriter, CompactionBatchResult, CompactionBuffer, CompactionClock, CompactionConsumerPoll,
    CompactionIndexSink, CompactionLoopConfig, CompactionLoopResult, CompactionOffsetCommitter,
    CompactionPollError, CompactionPollResult, compaction_wal_records_from_consumer_records,
    flush_buffer,
};

/// Accumulate-then-flush compactor loop with an injectable clock.
///
/// Each poll appends to an in-memory buffer and does not write a block. The loop
/// writes a block and commits offsets only when the buffer reaches
/// `flush_max_rows`, when its oldest record reaches `flush_max_age`, or when
/// `should_stop` fires at shutdown. At shutdown it flushes the remaining buffer,
/// so it drops no records. The `CompactionPollResult` that reaches `should_stop`
/// reports this poll's `polled_records` and `compacted_records`. Its `batch`
/// holds only the writes and commits of this iteration, so it is empty while the
/// loop buffers and populated on a flush.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_loop_with_clock<P, S, C, Stop, Clock>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    config: CompactionLoopConfig,
    mut should_stop: Stop,
    clock: &Clock,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
    Clock: CompactionClock + ?Sized,
{
    let mut summary = CompactionLoopResult::default();
    let mut buffer = CompactionBuffer::new();
    loop {
        let records = poller.poll(config.poll_timeout).await?;
        let polled_records = records.len();
        let wal_records =
            compaction_wal_records_from_consumer_records(&config.wal_topic, &records)?;
        let compacted_records = wal_records.len();

        let now = clock.now();
        buffer.extend(wal_records, now);

        let mut iteration_offsets = Vec::new();
        if buffer.should_flush(&config, now) {
            let buffered = buffer.take();
            iteration_offsets =
                flush_buffer(block_writer, index_sink, committer, &buffered, &mut summary).await?;
        }

        summary.polls += 1;
        summary.polled_records += polled_records;
        summary.compacted_records += compacted_records;

        let result = CompactionPollResult {
            polled_records,
            compacted_records,
            batch: CompactionBatchResult {
                partition_results: Vec::new(),
                writes: Vec::new(),
                committed_offsets: iteration_offsets,
            },
        };

        if should_stop(&result) {
            // Shutdown: flush whatever is still buffered so no records are lost.
            let buffered = buffer.take();
            flush_buffer(block_writer, index_sink, committer, &buffered, &mut summary).await?;
            break;
        }
    }
    Ok(summary)
}
