use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, TenantErrorSurface, TenantId,
    resolve_single_tenant, tenant_header_value,
};

/// The one tenant a read names, after the principal's grant and the query
/// authorizer allow it.
///
/// `surface` picks which of Loki's answers a tenant error gets. See
/// [`TenantErrorSurface`]. A tenant outside the principal's grant gets a 403
/// before the query authorizer runs. The audit trail records a refusal from
/// the broker ACLs.
pub(crate) async fn authorized_tenant(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<TenantId, HttpQueryError> {
    let tenant = resolve_single_tenant(tenant_header_value(headers))
        .map_err(|source| HttpQueryError::Tenant { source, surface })?;
    security.authorize_tenant(&tenant)?;
    state
        .query_authorizer
        .check(&security.principal, &tenant)
        .await
        .inspect_err(|error| security.record_read_refusal(error))?;
    Ok(tenant)
}
