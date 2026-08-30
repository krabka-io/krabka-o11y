use super::*;

pub(crate) fn system_time_ms(time: SystemTime) -> Result<i64> {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration_to_i64_ms(duration),
        Err(error) => duration_to_i64_ms(error.duration()).and_then(|duration_ms| {
            duration_ms
                .checked_neg()
                .ok_or_else(|| PromqlError::Plan("@ modifier timestamp is too small".to_string()))
        }),
    }
}
