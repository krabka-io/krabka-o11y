use super::{
    Response, StatusCode, TenantId, TenantRequest, TimeBounds, Uri, request_tenant,
    status_text_response,
};

pub(crate) struct TenantTimeBounds {
    pub(crate) tenant: TenantId,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

/// The tenant a request names and its time window, read with `bounds`, or the
/// response that rejects the request.
pub(crate) fn tenant_and_bounds(
    request: TenantRequest<'_>,
    bounds: fn(&Uri) -> Result<TimeBounds, String>,
) -> Result<TenantTimeBounds, Box<Response>> {
    let tenant = request_tenant(request.headers, request.principal, request.policy)?;
    let bounds = bounds(request.uri)
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    Ok(TenantTimeBounds {
        tenant,
        start_ns: bounds.start_ns,
        end_ns: bounds.end_ns,
    })
}
