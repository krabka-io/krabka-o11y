use super::*;

/// Enforce the tenant's structural caps on one series' labels.
///
/// Each cap is a Pyroscope cap, so a zero is unlimited rather than a cap of
/// nothing.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn enforce_limits(labels: &Labels, limits: &Limits) -> Result<(), ProfilesError> {
    let names = u64::try_from(labels.len()).unwrap_or(u64::MAX);
    if limits.max_label_names_per_series > 0 && names > limits.max_label_names_per_series {
        return Err(ProfilesError::Invalid(format!(
            "too many label names: {names} > {}",
            limits.max_label_names_per_series
        )));
    }

    let max_name = limits.max_label_name.bytes_usize();
    let max_value = limits.max_label_value.bytes_usize();
    for (name, value) in labels.iter() {
        if max_name > 0 && name.len() > max_name {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` name exceeds {max_name} bytes"
            )));
        }
        if max_value > 0 && value.len() > max_value {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` value exceeds {max_value} bytes"
            )));
        }
    }

    Ok(())
}
