use super::{
    HeaderMap, IntoResponse, Principal, Response, StatusCode, TenantId, TenantPolicy, Uri,
    metrics_query_param, request_tenant,
};

/// The tenant and the `TraceQL` metrics query a metrics request names, or the
/// response that rejects it.
pub(crate) fn metrics_request(
    headers: &HeaderMap,
    principal: &Principal,
    policy: &TenantPolicy,
    uri: &Uri,
) -> Result<(TenantId, String), Box<Response>> {
    let tenant = request_tenant(headers, principal, policy)?;
    let Some(query) = metrics_query_param(uri) else {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "missing query parameter q").into_response(),
        ));
    };
    Ok((tenant, query))
}
