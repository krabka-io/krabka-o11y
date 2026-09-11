use super::{HeaderMap, TENANT_HEADER};

/// The raw `X-Scope-OrgID` bytes of a request, or `None` when it has no such
/// header.
///
/// This reads the first header of that name, as Go's `Header.Get` does. It
/// does no validation. [`super::resolve_single_tenant`] and
/// [`super::resolve_federated_tenants`] do that.
pub(crate) fn tenant_header_value(headers: &HeaderMap) -> Option<&[u8]> {
    headers
        .get(TENANT_HEADER)
        .map(axum::http::HeaderValue::as_bytes)
}
