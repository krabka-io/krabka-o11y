use krabka_observability::{ReadinessGate, wal_consumer_metrics::WalConsumerMetrics};

use super::{
    Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerError, WalHeadConsumerLoopSummary,
    WalHeadConsumerPoll, poll_wal_head_consumer_once,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_wal_head_consumer_loop<C, Stop>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Time,
    metrics: Option<&WalConsumerMetrics>,
    catch_up: Option<&ReadinessGate>,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result =
            poll_wal_head_consumer_once(consumer, head, wal_topic, timeout, metrics).await?;
        summary.record_poll(
            result.polled_records,
            result.replayed_records,
            result.committed_offsets,
        );

        if let Some(recovery) = consumer.recovery_state().await {
            if let Some(metrics) = metrics {
                metrics.record_assignment(&recovery.assignment, recovery.at_log_end);
            }
            if let Some(gate) = catch_up {
                if recovery.at_log_end {
                    gate.mark_ready();
                } else {
                    gate.mark_unready();
                }
            }
        } else if let Some(gate) = catch_up {
            if result.replayed_records == 0 {
                gate.mark_ready();
            } else {
                gate.mark_unready();
            }
        }

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}
