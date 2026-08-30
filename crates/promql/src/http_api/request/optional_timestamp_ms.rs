use super::{ApiError, timestamp_ms, unix_now_ms};

pub(crate) fn optional_timestamp_ms(value: Option<&str>) -> Result<i64, ApiError> {
    match value {
        Some(value) => timestamp_ms(value),
        None => unix_now_ms(),
    }
}
