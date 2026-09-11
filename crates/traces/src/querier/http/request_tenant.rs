use super::{
    HeaderMap, HeaderValue, IntoResponse, Principal, Response, StatusCode, TENANT_HEADER, TenantId,
    TenantPolicy, authorize_tenant,
};

/// Resolves the tenant a query names and checks that `principal` may read it, or gives the response that rejects the query.
///
/// A malformed tenant is a 400. A tenant outside the principal's grant is a
/// 403. Both answers come before the query reads anything.
pub(crate) fn request_tenant(
    headers: &HeaderMap,
    principal: &Principal,
    policy: &TenantPolicy,
) -> Result<TenantId, Box<Response>> {
    // Tempo with multi-tenancy off ignores this header. Krabka does not: it
    // isolates every read by tenant. So a malformed value is a 400 here, and
    // the query never runs as the fallback tenant.
    let tenant = TenantId::resolve(
        headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
        policy,
    )
    .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err.to_string()).into_response()))?;
    authorize_tenant(principal, &tenant).map_err(|denied| Box::new(denied.into_response()))?;
    Ok(tenant)
}
