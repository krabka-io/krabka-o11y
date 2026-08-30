use super::*;

pub(crate) fn unix_now_ms() -> Result<i64, ApiError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::internal(format!("system time before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ApiError::internal("system time exceeds supported timestamp range"))
}
