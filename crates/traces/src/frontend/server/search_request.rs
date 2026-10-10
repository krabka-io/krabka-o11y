use super::{
    IntoResponse, Response, StatusCode, TenantId, TenantRequest, request_tenant,
    required_time_bounds, search_query,
};

/// The tenant, the `TraceQL` query, and the time window a search names, or
/// the response that rejects the search.
pub(crate) fn search_request(
    request: TenantRequest<'_>,
) -> Result<(TenantId, String, i64, i64), Box<Response>> {
    let TenantRequest {
        headers,
        principal,
        policy,
        uri,
    } = request;
    let tenant = request_tenant(headers, principal, policy)?;
    let query = match search_query(uri) {
        Ok(Some(query)) => query,
        Ok(None) => {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "missing query parameter q").into_response(),
            ));
        }
        Err(err) => return Err(Box::new((StatusCode::BAD_REQUEST, err).into_response())),
    };
    let (start_ns, end_ns) = required_time_bounds(uri)
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
    Ok((tenant, query, start_ns, end_ns))
}
