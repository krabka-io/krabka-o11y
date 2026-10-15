use super::{
    MetricStore, PrometheusApiState, RulerStateConsumerError, Time, WalHeadConsumerCommit,
    WalHeadConsumerPoll, WalHeadReplayResult,
    finish_wal_consumer_replay, poll_checked_replay_records, replay_ruler_state_records,
};

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
    let replay_records = poll_checked_replay_records(
        consumer,
        state_topic,
        timeout,
        |_| {},
        |error| RulerStateConsumerError::Poll(error.to_string()),
        |error| RulerStateConsumerError::UnsupportedFormat(error.to_string()),
    )
    .await?;
    let result = replay_ruler_state_records(state, state_topic, &replay_records)?;
    finish_wal_consumer_replay(
        consumer,
        result,
        |error| RulerStateConsumerError::Commit(error.to_string()),
        || {},
    )
    .await
}
