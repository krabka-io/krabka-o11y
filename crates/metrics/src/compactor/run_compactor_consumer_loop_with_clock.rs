use super::*;

/// Accumulate-then-flush single-consumer compactor loop with an injectable clock.
///
/// This mirrors [`run_compactor_loop_with_clock`], but it polls and commits
/// through one mutable consumer handle,
/// `process_compaction_record_batch_with_consumer`. That handle also writes the
/// block durably before it commits offsets.
///
/// # Consumer group rebalances
///
/// The loop commits only the offsets its durable writes produced, so a restart
/// replays at most the buffer it had not written. That holds while this member
/// keeps its partitions. It does not hold across a rebalance: the buffer spans
/// polls, and the consumer group releases a partition from a background task
/// without calling anything in this process, so the buffered records for that
/// partition are abandoned.
///
/// [`WalAssignmentConsumer`] reports each such revocation on
/// `wal_consumer_partition_revocations` and in the log. It does not repair it.
/// See [`krabka_observability::wal_group_assignment`] for why, and for what the
/// group id does and does not do.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_consumer_loop_with_clock<C, S, Stop, Clock>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    config: CompactionLoopConfig,
    mut should_stop: Stop,
    context: CompactionLoopContext<'_, Clock>,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
    Clock: CompactionClock + ?Sized,
{
    let mut summary = CompactionLoopResult::default();
    let mut buffer = CompactionBuffer::new();
    loop {
        let records = consumer
            .poll(config.poll_timeout)
            .await
            .inspect_err(|_| context.metrics.wal_consumer.record_poll_failure())?;
        context.metrics.wal_consumer.record_poll(&records);
        let polled_records = records.len();
        let wal_records =
            compaction_wal_records_from_consumer_records(&config.wal_topic, &records)?;
        let compacted_records = wal_records.len();
        // ONE consumer span per poll batch, parented on the producer trace carried
        // in a polled WAL record's `traceparent` header. Built once per batch and
        // run over the flush so the compaction block/index writes join the ingest
        // trace; a batch that only buffers (no flush this iteration) does no
        // compaction work and correctly carries no span.
        let span = compaction_batch_span(&records, compacted_records);

        let now = context.clock.now();
        buffer.extend(wal_records, now);

        let mut iteration_offsets = Vec::new();
        if buffer.should_flush(&config, now) {
            let buffered = buffer.take();
            iteration_offsets = flush_buffer_with_consumer(
                block_writer,
                index_sink,
                consumer,
                &buffered,
                &mut summary,
                context.metrics,
            )
            .instrument(span)
            .await?;
        } else {
            drop(span);
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
            flush_buffer_with_consumer(
                block_writer,
                index_sink,
                consumer,
                &buffered,
                &mut summary,
                context.metrics,
            )
            .await?;
            break;
        }
    }
    Ok(summary)
}
