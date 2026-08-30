use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn decode_kafka_wal_record(
    value: &[u8],
    partition: PartitionIndex,
    offset: Offset,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let mut record: WalLogRecord = serde_json::from_slice(value)?;
    record.position = Some(WalPosition { partition, offset });
    Ok(record)
}
