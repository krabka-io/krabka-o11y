use super::{ApiError, Time, TimeExt, prometheus_duration_ms, seconds_to_ms};

/// Returns a Prometheus query-API duration parameter as an extent.
///
/// The parameter is a `step` or a lookback. This function accepts both
/// encodings the API allows: a bare second count, which can be fractional, and
/// a Prometheus duration string such as `5m` or `1h30m`.
pub(crate) fn duration_param(value: &str) -> Result<Time, ApiError> {
    let millis = seconds_to_ms(value)
        .or_else(|()| prometheus_duration_ms(value).ok_or(()))
        .map_err(|()| ApiError::bad_data("invalid duration"))?;
    if millis <= 0 {
        return Err(ApiError::bad_data("duration must be positive"));
    }
    Ok(Time::from_millis(millis))
}
