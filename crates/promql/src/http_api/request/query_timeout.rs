use super::{ApiError, Time, TimeExt, prometheus_duration_ms, seconds_to_ms};

/// Resolves a request timeout against the process-wide query timeout cap.
pub(crate) fn query_timeout(value: Option<&str>, process_limit: Time) -> Result<Time, ApiError> {
    let Some(value) = value else {
        return Ok(process_limit);
    };
    let millis = seconds_to_ms(value)
        .or_else(|()| prometheus_duration_ms(value).ok_or(()))
        .map_err(|()| ApiError::bad_data("invalid duration"))?;
    if millis < 0 {
        return Err(ApiError::bad_data("duration must not be negative"));
    }
    if millis == 0 {
        return Ok(process_limit);
    }
    Ok(Time::from_millis(millis).min(process_limit))
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::*;

    use super::*;

    #[test]
    fn defaults_caps_and_accepts_prometheus_duration_forms() {
        let process = minutes(2);
        assert2::assert!(query_timeout(None, process).unwrap() == process);
        assert2::assert!(query_timeout(Some("0"), process).unwrap() == process);
        assert2::assert!(query_timeout(Some("0s"), process).unwrap() == process);
        assert2::assert!(query_timeout(Some("0.25"), process).unwrap() == millis(250));
        assert2::assert!(query_timeout(Some("30s"), process).unwrap() == secs(30));
        assert2::assert!(query_timeout(Some("5m"), process).unwrap() == process);
        assert2::assert!(query_timeout(Some("nope"), process).is_err());
        assert2::assert!(query_timeout(Some("-1"), process).is_err());
    }
}
