use super::*;
use krabka_observability::persisted_format::{PersistedHeader, validate_persisted_format};

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
        validate_persisted_format(record.headers.iter().map(|header| PersistedHeader {
            key: header.key.as_str(),
            value: header.value.as_deref(),
        }))
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

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn replay_ha_election_records(
    tracker: &HaTracker,
    ha_topic: &str,
    records: &[HaElectionConsumerRecord],
) -> Result<HaElectionReplayResult, HaElectionReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut replayed_records = 0;
    for record in records {
        if record.topic != ha_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(HaElectionReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let election_record = HaElectionRecord::decode(value)
            .map_err(|error| HaElectionReplayError::Decode(error.to_string()))?;
        tracker.persist_elected(&election_record);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }

    Ok(HaElectionReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|entry| HaElectionPartitionOffset {
                partition: entry.0,
                offset: entry.1,
            })
            .collect(),
    })
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_ha_election_consumer_loop<C, Stop>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Time,
    mut should_stop: Stop,
) -> Result<HaElectionConsumerLoopSummary, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
    Stop: FnMut(&HaElectionConsumerLoopSummary) -> bool,
{
    let mut summary = HaElectionConsumerLoopSummary::default();
    loop {
        let result = poll_ha_election_consumer_once(consumer, tracker, ha_topic, timeout).await?;
        summary.record_poll(
            result.polled_records,
            result.replayed_records,
            result.committed_offsets,
        );

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}
