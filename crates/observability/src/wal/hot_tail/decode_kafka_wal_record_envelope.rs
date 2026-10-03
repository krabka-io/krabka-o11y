use super::{
    KafkaWalRecord, WalLogRecord, WalRecordDecodeError, decode_kafka_wal_record,
    decode_native_kafka_log_record, has_native_kafka_log_headers,
};
use crate::persisted_format::{PersistedFormatError, validate_persisted_format};

/// Decodes one record of the logs WAL topic.
///
/// The topic holds two record kinds. A Krabka distributor writes a JSON record
/// with the `krabka-format-version` header. A plain Kafka client writes a
/// native record: the log line is the value, and `krabka-tenant`,
/// `krabka-log-label-*` and the other `krabka-log-*` headers carry the rest. A
/// native record has no format version header, so a record without one decodes
/// only as a native record.
///
/// # Errors
/// Returns an error when the format version header is malformed or
/// unsupported, when a record without the header is not a native record, or
/// when the record does not decode.
pub fn decode_kafka_wal_record_envelope(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    match validate_persisted_format(
        record
            .headers
            .iter()
            .map(|header| (header.key.as_str(), header.value.as_deref())),
    ) {
        Ok(()) => {}
        Err(PersistedFormatError::Missing) if has_native_kafka_log_headers(&record.headers) => {
            return decode_native_kafka_log_record(record);
        }
        Err(error) => return Err(WalRecordDecodeError::UnsupportedFormat(error.to_string())),
    }
    match decode_kafka_wal_record(&record.value, record.partition, record.offset) {
        Ok(record) => Ok(record),
        Err(_) if has_native_kafka_log_headers(&record.headers) => {
            decode_native_kafka_log_record(record)
        }
        Err(error) => Err(error),
    }
}
