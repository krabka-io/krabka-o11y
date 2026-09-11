use super::{
    CompactorDeleteState, HeaderMap, HttpQueryError, IntoResponse, RequestSecurity, Response,
    TenantErrorSurface, TenantId, resolve_single_tenant, tenant_header_value,
};

/// The one tenant a delete-request call names, after the principal's grant and
/// the query authorizer allow it.
///
/// Loki's compactor reads the tenant with `dskit`'s `tenant.TenantID` and
/// answers every tenant error through Go's `http.Error` with 400, except a
/// missing tenant, which gets 401. A tenant outside the principal's grant gets
/// a 403. Both checks run before any parameter is read, so a refused tenant
/// learns nothing about its requests and changes nothing.
pub(crate) async fn authorized_delete_tenant(
    state: &CompactorDeleteState,
    security: &RequestSecurity,
    headers: &HeaderMap,
) -> Result<TenantId, Response> {
    let tenant = resolve_single_tenant(tenant_header_value(headers)).map_err(|source| {
        HttpQueryError::Tenant {
            source,
            surface: TenantErrorSurface::Push,
        }
        .into_response()
    })?;
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
