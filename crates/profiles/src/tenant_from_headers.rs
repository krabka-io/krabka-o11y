use axum::http::{HeaderMap, HeaderValue};
use krabka_blockstore::{TENANT_HEADER, TenantId, TenantPolicy, TenantResolveError};

/// Resolves the tenant that the `X-Scope-OrgID` header of a request names.
///
/// Every distributor and querier handler reads the tenant through this one
/// function. It gives the raw header bytes to [`TenantId::resolve`] and does no
/// validation and no fallback of its own. The caller supplies `policy` and maps
/// the error to the response shape of its protocol.
pub(crate) fn tenant_from_headers(
    headers: &HeaderMap,
    policy: &TenantPolicy,
) -> Result<TenantId, TenantResolveError> {
    TenantId::resolve(
        headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
        policy,
    )
}
