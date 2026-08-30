use super::{
    BTreeMap, HaElectionConsumerRecord, HaElectionPartitionOffset, HaElectionRecord,
    HaElectionReplayError, HaElectionReplayResult, HaTracker, Offset, PartitionIndex,
};

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
            .map(|(partition, offset)| HaElectionPartitionOffset { partition, offset })
            .collect(),
    })
}
