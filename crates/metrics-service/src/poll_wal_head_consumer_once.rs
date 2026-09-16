use krabka_observability::{
    persisted_format::validate_persisted_format, wal_consumer_metrics::WalConsumerMetrics,
};

use super::{
    Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerError, WalHeadConsumerPoll,
    WalHeadConsumerRecord, WalHeadReplayResult, replay_wal_head_records,
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
    for record in records.iter().filter(|record| record.topic == wal_topic) {
        validate_persisted_format(
            record
                .headers
                .iter()
                .map(|header| (header.key.as_str(), header.value.as_deref())),
        )
        .map_err(|error| WalHeadConsumerError::UnsupportedFormat(error.to_string()))?;
    }
    let replay_records = records
        .into_iter()
        .map(|record| WalHeadConsumerRecord {
            topic: record.topic,
            partition: record.partition.into(),
            offset: record.offset.into(),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
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
