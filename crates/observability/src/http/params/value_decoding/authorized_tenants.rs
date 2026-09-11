use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, TenantErrorSurface, TenantId,
    resolve_federated_tenants, tenant_header_value,
};

/// Every tenant a federated query names, after the principal's grant and the
/// query authorizer allow each one.
///
/// The principal's grant must include every tenant before the query
/// authorizer runs for any of them, so a query that names one tenant outside
/// the grant gets a 403 and reads nothing.
///
/// Krabka federates a query as Loki does with
/// `querier.multi_tenant_queries_enabled` on. The `loki_differential`
/// configuration leaves that switch off, and Loki then answers `a|b` with 500
/// `multiple org IDs present`. That divergence is deliberate: the multi-tenant
/// query is a feature the querier keeps.
pub(crate) async fn authorized_tenants(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<Vec<TenantId>, HttpQueryError> {
    let tenants = resolve_federated_tenants(tenant_header_value(headers))
        .map_err(|source| HttpQueryError::Tenant { source, surface })?;
    for tenant in &tenants {
        security.authorize_tenant(tenant)?;
    }
    for tenant in &tenants {
        state
            .query_authorizer
            .check(&security.principal, tenant)
            .await
            .inspect_err(|error| security.record_read_refusal(error))?;
    }
    Ok(tenants)
}
