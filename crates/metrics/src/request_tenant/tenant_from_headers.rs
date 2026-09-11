use super::{
    HeaderMap, HeaderValue, RequestTenantError, TENANT_HEADER, TenantId, resolve_request_tenant,
};

/// Resolves the tenant of an HTTP request from its `X-Scope-OrgID` header.
///
/// When the request repeats the header, the first value names the tenant, as
/// Go's `Header.Get` reads it in Mimir.
///
/// # Errors
///
/// Returns the errors of [`resolve_request_tenant`].
pub fn tenant_from_headers(headers: &HeaderMap) -> Result<TenantId, RequestTenantError> {
    resolve_request_tenant(headers.get(TENANT_HEADER).map(HeaderValue::as_bytes))
}
