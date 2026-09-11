use super::{
    HeaderMap, HttpQueryError, IntoResponse, QuerierState, RequestSecurity, Response,
    TenantErrorSurface, TenantId, resolve_single_tenant, tenant_header_value,
};

/// The one tenant a ruler request names, after the principal's grant and the
/// query authorizer allow it.
///
/// Loki's ruler reads the tenant with `dskit`'s `tenant.TenantID`, so a header
/// with two tenants is refused and `a|a` is tenant `a`. With `auth_enabled:
/// true`, which is the `loki_differential` configuration, a request without a
/// tenant gets 401. It is never served as tenant `fake`.
///
/// Every rule route asks the authorizer, the routes that change rule groups
/// too. An unauthenticated caller that could name any tenant could otherwise
/// read, create and delete that tenant's rules. A tenant outside the
/// principal's grant gets a 403 before the authorizer runs.
pub(crate) async fn authorized_ruler_tenant(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<TenantId, Response> {
    let tenant = resolve_single_tenant(tenant_header_value(headers))
        .map_err(|source| HttpQueryError::Tenant { source, surface }.into_response())?;
    security
        .authorize_tenant(&tenant)
        .map_err(IntoResponse::into_response)?;
    state
        .query_authorizer
        .check(&security.principal, &tenant)
        .await
        .map_err(|error| {
            security.record_read_refusal(&error);
            HttpQueryError::from(error).into_response()
        })?;
    Ok(tenant)
}
