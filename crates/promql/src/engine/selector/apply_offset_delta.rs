use super::*;

pub(crate) fn apply_offset_delta(time_ms: i64, offset: Time) -> Result<i64> {
    time_ms
        .checked_add(offset.millis_i64())
        .ok_or_else(|| PromqlError::Plan("offset evaluation time overflow".to_string()))
}
