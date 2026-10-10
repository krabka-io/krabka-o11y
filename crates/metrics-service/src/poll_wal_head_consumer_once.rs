use krabka_observability::wal_consumer_metrics::WalConsumerMetrics;

use super::{
    Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerError, WalHeadConsumerPoll,
    WalHeadReplayResult, checked_replay_records, replay_wal_head_records,
};

#[tracing::instrument(
    level = "debug",
    name = "metrics.wal_head.poll_once",
    skip_all,
    fields(wal_topic = %wal_topic, polled = tracing::field::Empty, replayed = tracing::field::Empty),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn poll_wal_head_consumer_once<C>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Time,
    metrics: Option<&WalConsumerMetrics>,
) -> Result<WalHeadReplayResult, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    if let Some(metrics) = metrics {
        metrics.record_poll(&records);
    }
    let replay_records = checked_replay_records(records, wal_topic)
        .map_err(|error| WalHeadConsumerError::UnsupportedFormat(error.to_string()))?;
    let result = replay_wal_head_records(head, wal_topic, &replay_records)?;
    let span = tracing::Span::current();
    span.record("polled", result.polled_records);
    span.record("replayed", result.replayed_records);
    if result.replayed_records > 0 {
        consumer.commit_sync().await?;
        if let Some(metrics) = metrics {
            metrics.record_commit();
        }
    }
    Ok(result)
}
