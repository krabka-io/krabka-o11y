use super::{
    BlockWriter, CompactionConsumerCommitMut, CompactionConsumerPoll, CompactionIndexSink,
    CompactionLoopConfig, CompactionLoopResult, CompactionPollError, CompactionPollResult,
    SystemCompactionClock, run_compactor_consumer_loop_with_clock,
};

/// Runs the compactor polling loop with a single consumer handle for poll and
/// commit. It uses the real monotonic clock for flush-by-age.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_consumer_loop<C, S, Stop>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    config: CompactionLoopConfig,
    should_stop: Stop,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
{
    run_compactor_consumer_loop_with_clock(
        consumer,
        block_writer,
        index_sink,
        config,
        should_stop,
        &SystemCompactionClock,
    )
    .await
}
