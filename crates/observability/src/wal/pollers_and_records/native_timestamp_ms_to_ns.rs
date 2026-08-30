use super::*;

pub(crate) fn native_timestamp_ms_to_ns(timestamp_ms: i64) -> Result<i64, WalRecordDecodeError> {
    let converted_ns = timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        WalRecordDecodeError::InvalidNativeTimestampValue {
            value: timestamp_ms.to_string(),
        }
    })?;
    validate_native_timestamp_ns(converted_ns, timestamp_ms.to_string())
}
