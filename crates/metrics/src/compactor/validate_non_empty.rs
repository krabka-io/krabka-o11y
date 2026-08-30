use super::MetricsCompactorConfigError;

pub(crate) fn validate_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), MetricsCompactorConfigError> {
    if value.is_empty() {
        Err(MetricsCompactorConfigError::Empty { field })
    } else {
        Ok(())
    }
}
