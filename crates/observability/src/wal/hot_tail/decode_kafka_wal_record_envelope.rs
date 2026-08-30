use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn decode_kafka_wal_record_envelope(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    match decode_kafka_wal_record(&record.value, record.partition, record.offset) {
        Ok(record) => Ok(record),
        Err(_) if has_native_kafka_log_headers(&record.headers) => {
            decode_native_kafka_log_record(record)
        }
        Err(error) => Err(error),
    }
}
