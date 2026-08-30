use super::{CompactionConsumerRecordError, CompactionWalRecord, ConsumerRecord, Offset, PartitionIndex};

/// Converts polled consumer records from the metrics WAL topic into compactor
/// inputs.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn compaction_wal_records_from_consumer_records(
    wal_topic: &str,
    records: &[ConsumerRecord],
) -> Result<Vec<CompactionWalRecord>, CompactionConsumerRecordError> {
    let mut out = Vec::new();
    for record in records {
        if record.topic != wal_topic {
            continue;
        }
        let value = record
            .value
            .as_ref()
            .ok_or(CompactionConsumerRecordError::MissingValue {
                partition: PartitionIndex(record.partition),
                offset: Offset(record.offset),
            })?;
        out.push(CompactionWalRecord {
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            value: value.to_vec(),
        });
    }
    Ok(out)
}
