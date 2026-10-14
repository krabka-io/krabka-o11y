use krabka_observability::wal_consumer_metrics::WalConsumerMetrics;

use super::{
    Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerError, WalHeadConsumerPoll,
    WalHeadReplayResult, finish_wal_consumer_replay::finish_wal_consumer_replay,
    poll_checked_replay_records::poll_checked_replay_records, replay_wal_head_records,
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
    let replay_records = poll_checked_replay_records(
        consumer,
        wal_topic,
        timeout,
        |records| {
            if let Some(metrics) = metrics {
                metrics.record_poll(records);
            }
        },
        |error| error,
        |error| WalHeadConsumerError::UnsupportedFormat(error.to_string()),
    )
    .await?;
    let result = replay_wal_head_records(head, wal_topic, &replay_records)?;
    finish_wal_consumer_replay(
        consumer,
        result,
        |error| error,
        || {
            if let Some(metrics) = metrics {
                metrics.record_commit();
            }
        },
    )
    .await
}
