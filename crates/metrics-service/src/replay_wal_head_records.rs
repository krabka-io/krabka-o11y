use super::*;

#[tracing::instrument(
    level = "debug",
    name = "metrics.wal_head.replay",
    skip_all,
    fields(wal_topic = %wal_topic, records = records.len()),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub fn replay_wal_head_records(
    head: &WalHead,
    wal_topic: &str,
    records: &[WalHeadConsumerRecord],
) -> Result<WalHeadReplayResult, WalHeadReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut newest_timestamp_ms: Option<i64> = None;
    // Decoded first, applied second. Decoding touches no shared state, so it
    // stays outside the head's write lock, and the whole poll then reaches the
    // head as one batch: one lock acquisition and at most one copy-on-write
    // clone, rather than one of each per record. A query running beside the
    // tail sees the poll's records all at once or not at all.
    let mut decoded = Vec::with_capacity(records.len());
    for record in records {
        if record.topic != wal_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(WalHeadReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let wal_record = WalRecord::decode(value)
            .map_err(|error| WalHeadReplayError::Decode(error.to_string()))?;
        if let Some(timestamp_ms) = wal_record_max_timestamp_ms(&wal_record) {
            newest_timestamp_ms =
                Some(newest_timestamp_ms.map_or(timestamp_ms, |current| current.max(timestamp_ms)));
        }
        // partition/offset are now the shared krabka_ids types promql also uses,
        // so they pass straight through with no conversion at the seam.
        decoded.push((wal_record, record.partition, record.offset));
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }
    let replayed_records = decoded.len();
    head.apply_wal_records_at(
        decoded
            .iter()
            .map(|(record, partition, offset)| (record, *partition, *offset)),
    );
    if let Some(timestamp_ms) = newest_timestamp_ms {
        let _ = head.prune(timestamp_ms);
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
