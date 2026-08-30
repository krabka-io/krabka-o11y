use super::*;

pub(crate) fn validate_timestamp_range(start_ms: i64, end_ms: i64) -> Result<(), ApiError> {
    if end_ms < start_ms {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    Ok(())
}
