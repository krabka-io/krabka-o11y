use super::{
    MetricStore, PrometheusApiState, RulerStateConsumerError, Time, WalHeadConsumerCommit,
    WalHeadConsumerLoopSummary, WalHeadConsumerPoll, poll_ruler_state_consumer_once,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_ruler_state_consumer_loop<S, C, Stop>(
    consumer: &mut C,
    state: &PrometheusApiState<S>,
    state_topic: &str,
    timeout: Time,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, RulerStateConsumerError>
where
    S: MetricStore,
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result = poll_ruler_state_consumer_once(consumer, state, state_topic, timeout).await?;
        summary.polls += 1;
        summary.polled_records += result.polled_records;
        summary.replayed_records += result.replayed_records;
        summary.committed_offsets.extend(result.committed_offsets);

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}
