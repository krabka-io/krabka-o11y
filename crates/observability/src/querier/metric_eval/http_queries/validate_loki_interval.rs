use super::*;

pub(crate) fn validate_loki_interval(interval: Option<i64>) -> Result<(), HttpQueryError> {
    if let Some(interval_ns) = interval
        && interval_ns < 0
    {
        return Err(HttpQueryError::InvalidInterval);
    }
    Ok(())
}
