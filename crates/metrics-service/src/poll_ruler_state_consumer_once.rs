use super::*;

#[tracing::instrument(
    level = "debug",
    name = "metrics.ruler_state.poll_once",
    skip_all,
    fields(state_topic = %state_topic, polled = tracing::field::Empty, replayed = tracing::field::Empty),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn poll_ruler_state_consumer_once<S, C>(
    consumer: &mut C,
    state: &PrometheusApiState<S>,
    state_topic: &str,
    timeout: Time,
) -> Result<WalHeadReplayResult, RulerStateConsumerError>
where
    S: MetricStore,
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
{
    let records = consumer
        .poll(timeout)
        .await
        .map_err(|error| RulerStateConsumerError::Poll(error.to_string()))?;
    let replay_records = records
        .into_iter()
        .map(|record| WalHeadConsumerRecord {
            topic: record.topic,
            partition: record.partition.into(),
            offset: record.offset.into(),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
    let result = replay_ruler_state_records(state, state_topic, &replay_records)?;
    let span = tracing::Span::current();
    span.record("polled", result.polled_records);
    span.record("replayed", result.replayed_records);
    if result.replayed_records > 0 {
        consumer
            .commit_sync()
            .await
            .map_err(|error| RulerStateConsumerError::Commit(error.to_string()))?;
    }
    Ok(result)
}
