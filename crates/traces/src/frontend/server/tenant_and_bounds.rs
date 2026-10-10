use super::{IntoResponse, Response, StatusCode, TenantId, TenantRequest, Uri, request_tenant};

/// The tenant a request names and its time window, read with `bounds`, or the
/// response that rejects the request.
pub(crate) fn tenant_and_bounds(
    request: TenantRequest<'_>,
    bounds: fn(&Uri) -> Result<(i64, i64), String>,
) -> Result<(TenantId, i64, i64), Box<Response>> {
    let tenant = request_tenant(request.headers, request.principal, request.policy)?;
    let (start_ns, end_ns) = bounds(request.uri)
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
    Ok((tenant, start_ns, end_ns))
}
