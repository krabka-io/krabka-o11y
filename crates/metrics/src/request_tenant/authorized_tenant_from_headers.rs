use super::{
    HeaderMap, Principal, TenantAccessError, TenantId, authorize_tenant, tenant_from_headers,
};

/// Resolves the tenant of an HTTP request, and checks that `principal` may use it.
///
/// The authentication layer puts `principal` into the request. A server with
/// no credentials file gives `Principal::Unauthenticated`, which may use every
/// tenant, as Grafana Mimir allows by default.
///
/// # Errors
///
/// Returns [`TenantAccessError::Unresolved`] with the errors of
/// [`tenant_from_headers`]. Returns [`TenantAccessError::Denied`] when the
/// principal's grant does not include the tenant. `authorize_tenant` also
/// reports that denial to the server's security events.
pub fn authorized_tenant_from_headers(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantId, TenantAccessError> {
    let tenant = tenant_from_headers(headers)?;
    authorize_tenant(principal, &tenant)?;
    Ok(tenant)
}
