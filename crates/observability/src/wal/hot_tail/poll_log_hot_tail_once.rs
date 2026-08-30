use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn poll_log_hot_tail_once(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Time,
) -> Result<usize, HotTailPollError> {
    poll_log_hot_tail_once_with_frontier(consumer, hot_tail, timeout, None).await
}
