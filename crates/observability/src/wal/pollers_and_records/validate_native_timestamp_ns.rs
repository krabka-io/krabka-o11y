use super::*;

pub(crate) fn validate_native_timestamp_ns(
    timestamp_ns: i64,
    value: String,
) -> Result<i64, WalRecordDecodeError> {
    if timestamp_ns < 0 {
        Err(WalRecordDecodeError::InvalidNativeTimestampValue { value })
    } else {
        Ok(timestamp_ns)
    }
}
