use super::{
    IntoResponse, Json, QuerierRequest, Response, ScopedTag, SpanStore, StatusCode, TagScope,
    Value, is_match_all_query, optional_time_bounds, query_param, scope_param,
    scoped_tags_from_traces, status_text_response, tag_filter, tenant_and_bounds,
    traces_matching_filter,
};

/// Renders the scoped tags; this is where the v1 and v2 endpoints differ.
pub(crate) type TagsRender = fn(Vec<ScopedTag>, Option<TagScope>) -> Value;

/// Answer a tag-names request.
pub(crate) async fn search_tags_inner<S>(
    request: &QuerierRequest<S>,
    render: TagsRender,
) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let bounds = match tenant_and_bounds(request.tenant_request(), optional_time_bounds) {
        Ok(tenant_window) => tenant_window,
        Err(rejection) => return *rejection,
    };
    let scope = match scope_param(uri) {
        Ok(scope) => scope,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    if let Some(query) = query_param(uri, "q")
        && !is_match_all_query(&query)
    {
        let filter = tag_filter(bounds.tenant.as_str(), &bounds, &query, uri);
        return match traces_matching_filter(state, filter).await {
            Ok(traces) => {
                Json(render(scoped_tags_from_traces(&traces, scope), scope)).into_response()
            }
            Err(rejection) => *rejection,
        };
    }
    match state
        .engine
        .tag_names(
            bounds.tenant.as_str(),
            scope,
            bounds.start_ns,
            bounds.end_ns,
        )
        .await
    {
        Ok(tags) => Json(render(tags, scope)).into_response(),
        Err(err) => status_text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}
