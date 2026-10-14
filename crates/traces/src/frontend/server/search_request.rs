use super::{
    RequiredTimeBounds, Response, StatusCode, TenantId, TenantRequest, request_tenant,
    required_time_bounds, search_query, status_text_response,
};

/// The tenant, the `TraceQL` query, and the time window a search names, or
/// the response that rejects the search.
pub(crate) struct SearchRequest {
    pub(crate) tenant: TenantId,
    pub(crate) query: String,
    pub(crate) bounds: RequiredTimeBounds,
}

pub(crate) fn search_request(request: TenantRequest<'_>) -> Result<SearchRequest, Box<Response>> {
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
            return Err(Box::new(status_text_response(
                StatusCode::BAD_REQUEST,
                "missing query parameter q",
            )));
        }
        Err(err) => return Err(Box::new(status_text_response(StatusCode::BAD_REQUEST, err))),
    };
    let bounds = required_time_bounds(uri)
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    Ok(SearchRequest {
        tenant,
        query,
        bounds,
    })
}
