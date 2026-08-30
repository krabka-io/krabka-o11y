use super::*;

/// Resolves and validates the tenant from the `X-Scope-OrgID` header.
///
/// Absent, empty, or non-UTF-8 headers resolve to the anonymous tenant. This
/// function validates a present, non-empty header against the tenant charset.
/// See [`crate::tenant::validate_tenant`]. An invalid tenant id becomes a
/// [`ProfileError::Plan`], which maps to Connect `invalid_argument` and legacy
/// 400 and carries a generic message. The function never uses an invalid id as
/// a storage key.
pub(crate) fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ProfileError> {
    let header = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok());
    crate::tenant::tenant_from_header(header).map_err(|_| {
        // `validate_tenant` already returns a generic, non-leaky message; keep
        // it generic here too so we never echo an attacker-supplied tenant id.
        ProfileError::Plan("invalid tenant id".to_string())
    })
}
