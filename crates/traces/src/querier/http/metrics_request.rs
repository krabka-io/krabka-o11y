use super::{
    HeaderMap, Principal, Response, StatusCode, TenantId, TenantPolicy, Uri, metrics_query_param,
    request_tenant, status_text_response,
};

/// The parts of a read request that name and authorize its tenant, and the
/// URI that carries its parameters.
#[derive(Clone, Copy)]
pub(crate) struct TenantRequest<'a> {
    pub(crate) headers: &'a HeaderMap,
    pub(crate) principal: &'a Principal,
    pub(crate) policy: &'a TenantPolicy,
    pub(crate) uri: &'a Uri,
}

/// The tenant and the `TraceQL` metrics query a metrics request names, or the
/// response that rejects it.
pub(crate) struct MetricsRequest {
    pub(crate) tenant: TenantId,
    pub(crate) query: String,
}

pub(crate) fn metrics_request(request: TenantRequest<'_>) -> Result<MetricsRequest, Box<Response>> {
    let tenant = request_tenant(request.headers, request.principal, request.policy)?;
    let Some(query) = metrics_query_param(request.uri) else {
        return Err(Box::new(status_text_response(
            StatusCode::BAD_REQUEST,
            "missing query parameter q",
        )));
    };
    Ok(MetricsRequest { tenant, query })
}
