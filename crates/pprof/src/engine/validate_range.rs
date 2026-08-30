use super::ProfileError;

pub(crate) fn validate_range(start_ms: i64, end_ms: i64) -> Result<(), ProfileError> {
    if start_ms > end_ms {
        return Err(ProfileError::Plan(format!(
            "invalid time range: start {start_ms} is after end {end_ms}"
        )));
    }
    Ok(())
}
