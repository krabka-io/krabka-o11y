use super::{CompactionConsumerPoll, CompactionIndexSink, CompactionOffsetCommitter, CompactionPollResult, BlockWriter, CompactionLoopConfig, CompactionLoopResult, CompactionPollError, run_compactor_loop_with_clock, SystemCompactionClock};

/// Runs the compactor polling loop until `should_stop` returns true. It uses the
/// real monotonic clock for flush-by-age.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_loop<P, S, C, Stop>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    config: CompactionLoopConfig,
    should_stop: Stop,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
{
    run_compactor_loop_with_clock(
        poller,
        block_writer,
        index_sink,
        committer,
        config,
        should_stop,
        &SystemCompactionClock,
    )
    .await
}
