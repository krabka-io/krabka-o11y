use super::{Result, PromqlError};

pub(crate) fn duration_to_i64_ms(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PromqlError::Plan("@ modifier timestamp is too large".to_string()))
}
