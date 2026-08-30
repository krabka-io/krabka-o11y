use super::*;

#[tracing::instrument(
    level = "debug",
    name = "metrics.ruler_state.replay",
    skip_all,
    fields(state_topic = %state_topic, records = records.len()),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub fn replay_ruler_state_records<S: MetricStore>(
    state: &PrometheusApiState<S>,
    state_topic: &str,
    records: &[WalHeadConsumerRecord],
) -> Result<WalHeadReplayResult, RulerStateReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut replayed_records = 0;
    for record in records {
        if record.topic != state_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(RulerStateReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let state_record = RulerStateWalRecord::decode(value)
            .map_err(|error| RulerStateReplayError::Decode(error.to_string()))?;
        apply_ruler_state_record(state, state_record);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }

    Ok(WalHeadReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|(partition, offset)| WalHeadPartitionOffset { partition, offset })
            .collect(),
    })
}
