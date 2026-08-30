use super::*;

pub(crate) fn timestamp_ms(value: &str) -> Result<i64, ApiError> {
    seconds_to_ms(value)
        .or_else(|()| rfc3339_to_ms(value))
        .map_err(|()| ApiError::bad_data("invalid timestamp"))
}
