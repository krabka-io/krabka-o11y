use super::*;

pub(crate) fn validate_positive_timeout(name: &str, timeout: Time) -> Result<(), String> {
    let duration = std::time::Duration::try_from_secs_f64(timeout.secs_f64())
        .map_err(|error| format!("debuginfod {name} timeout: {error}"))?;
    if duration.is_zero() {
        return Err(format!("debuginfod {name} timeout must be positive"));
    }
    Ok(())
}
