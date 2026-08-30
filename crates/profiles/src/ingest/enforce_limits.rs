use super::*;

/// Enforce per-tenant structural caps.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn enforce_limits(labels: &Labels, limits: &TenantLimits) -> Result<(), ProfilesError> {
    if labels.len() > limits.max_label_names_per_series {
        return Err(ProfilesError::Invalid(format!(
            "too many label names: {} > {}",
            labels.len(),
            limits.max_label_names_per_series
        )));
    }

    for (name, value) in labels.iter() {
        if name.len() > limits.max_label_name.bytes_usize() {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` name exceeds {} bytes",
                limits.max_label_name.bytes_usize()
            )));
        }
        if value.len() > limits.max_label_value.bytes_usize() {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` value exceeds {} bytes",
                limits.max_label_value.bytes_usize()
            )));
        }
    }

    Ok(())
}
