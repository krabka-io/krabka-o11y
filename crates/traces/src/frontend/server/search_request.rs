use super::{
    HeaderMap, IntoResponse, Principal, Response, StatusCode, TenantId, TenantPolicy, Uri,
    request_tenant, required_time_bounds, search_query,
};

/// The tenant, the `TraceQL` query, and the time window a search names, or
/// the response that rejects the search.
pub(crate) fn search_request(
    headers: &HeaderMap,
    principal: &Principal,
    policy: &TenantPolicy,
    uri: &Uri,
) -> Result<(TenantId, String, i64, i64), Box<Response>> {
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
