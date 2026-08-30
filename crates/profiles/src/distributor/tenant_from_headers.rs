use super::*;

/// Resolves and validates the tenant from the `X-Scope-OrgID` header.
///
/// [`crate::tenant::tenant_from_header`] defaults absent, empty, or non-UTF-8
/// headers to `"anonymous"`. For a present, non-empty value, this function
/// validates the value against the Mimir/Pyroscope charset. It rejects a
/// malformed value with [`ProfilesError::Invalid`], which maps to HTTP 400 and
/// Connect `invalid_argument`. This check stops a caller from creating
/// path-unsafe or unlimited tenant ids at the ingest door.
pub(crate) fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ProfilesError> {
    let value = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok());
    crate::tenant::tenant_from_header(value)
}
