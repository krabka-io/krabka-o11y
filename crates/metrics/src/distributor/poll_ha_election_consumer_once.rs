use krabka_observability::persisted_format::validate_persisted_format;

use super::{
    HaElectionConsumerCommit, HaElectionConsumerError, HaElectionConsumerPoll,
    HaElectionConsumerRecord, HaElectionReplayResult, HaTracker, Offset, PartitionIndex, Time,
    replay_ha_election_records,
};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_ha_election_consumer_once<C>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Time,
) -> Result<HaElectionReplayResult, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    for record in records.iter().filter(|record| record.topic == ha_topic) {
        validate_persisted_format(
            record
                .headers
                .iter()
                .map(|header| (header.key.as_str(), header.value.as_deref())),
        )
        .map_err(|error| HaElectionConsumerError::UnsupportedFormat(error.to_string()))?;
    }
    let replay_records = records
        .into_iter()
        .map(|record| HaElectionConsumerRecord {
            topic: record.topic,
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
    let result = replay_ha_election_records(tracker, ha_topic, &replay_records)?;
    if result.replayed_records > 0 {
        consumer.commit_sync().await?;
    }
    Ok(result)
}
