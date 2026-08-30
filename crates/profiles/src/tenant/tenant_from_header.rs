use super::*;

/// Resolve a tenant id from an optional header value.
///
/// Returns the [`ANONYMOUS_TENANT`] default when `value` is `None` or an empty
/// string. The caller signals a non-UTF-8 header by passing `None`. For a
/// present, non-empty value, [`validate_tenant`] validates the id.
///
/// # Errors
///
/// Returns [`ProfilesError::Invalid`] when a present, non-empty value fails
/// [`validate_tenant`].
pub fn tenant_from_header(value: Option<&str>) -> Result<String, ProfilesError> {
    match value {
        None | Some("") => Ok(ANONYMOUS_TENANT.to_string()),
        Some(raw) => validate_tenant(raw),
    }
}
