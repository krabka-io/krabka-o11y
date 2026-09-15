use super::{
    KafkaWalRecord, WalLogRecord, WalRecordDecodeError, decode_kafka_wal_record,
    decode_native_kafka_log_record, has_native_kafka_log_headers,
};
use crate::persisted_format::validate_persisted_format;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn decode_kafka_wal_record_envelope(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    validate_persisted_format(
        record
            .headers
            .iter()
            .map(|header| (header.key.as_str(), header.value.as_deref())),
    )
    .map_err(|error| WalRecordDecodeError::UnsupportedFormat(error.to_string()))?;
    match decode_kafka_wal_record(&record.value, record.partition, record.offset) {
        Ok(record) => Ok(record),
        Err(_) if has_native_kafka_log_headers(&record.headers) => {
            decode_native_kafka_log_record(record)
        }
        Err(error) => Err(error),
    }
}
