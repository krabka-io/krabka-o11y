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
) -> Result<WalHeadReplayResult, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
{
    let records = consumer.poll(timeout).await?;
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
    }
    Ok(result)
}
