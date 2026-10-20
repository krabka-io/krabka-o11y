use super::{
    HeaderMap, IntoResponse, Principal, ProfileStore, QuerierState, Response, TenantId,
    authorize_tenant, tenant_error_response, tenant_from_headers,
};

/// The tenant a plain-HTTP query names in `headers`, once `principal` may
/// read it.
///
/// # Errors
/// Returns the response that refuses the request, boxed: a tenant error when the
/// headers name no valid tenant, or a denial when `principal` may not read
/// the tenant they name.
pub(crate) fn authorized_http_tenant<S>(
    state: &QuerierState<S>,
    principal: &Principal,
    headers: &HeaderMap,
) -> Result<TenantId, Box<Response>>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(headers, &state.tenant_policy)
        .map_err(|error| Box::new(tenant_error_response(&error)))?;
    authorize_tenant(principal, &tenant).map_err(|denied| Box::new(denied.into_response()))?;
    Ok(tenant)
}
