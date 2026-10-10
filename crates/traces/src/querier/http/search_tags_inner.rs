use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, ScopedTag, SpanStore, StatusCode,
    TagFilter, TagScope, TenantRequest, TimeRange, Uri, Value, is_match_all_query,
    optional_time_bounds, query_param, scope_param, scoped_tags_from_traces, tenant_and_bounds,
    traces_matching_filter,
};

/// A tag-names request.
pub(crate) struct TagsRequest<'a> {
    pub(crate) principal: &'a Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) uri: Uri,
    /// Renders the scoped tags; this is where the v1 and v2 endpoints differ.
    pub(crate) render: fn(Vec<ScopedTag>, Option<TagScope>) -> Value,
}

/// Answer a tag-names request.
pub(crate) async fn search_tags_inner<S>(state: &AppState<S>, request: TagsRequest<'_>) -> Response
where
    S: SpanStore + 'static,
{
    let TagsRequest {
        principal,
        headers,
        uri,
        render,
    } = request;
    let (tenant, start_ns, end_ns) = match tenant_and_bounds(
        TenantRequest {
            headers: &headers,
            principal,
            policy: &state.cfg.tenant_policy,
            uri: &uri,
        },
        optional_time_bounds,
    ) {
        Ok(tenant_window) => tenant_window,
        Err(rejection) => return *rejection,
    };
    let scope = match scope_param(&uri) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q")
        && !is_match_all_query(&query)
    {
        let filter = TagFilter {
            tenant: tenant.as_str(),
            query: &query,
            uri: &uri,
            window: TimeRange { start_ns, end_ns },
        };
        return match traces_matching_filter(state, filter).await {
            Ok(traces) => {
                Json(render(scoped_tags_from_traces(&traces, scope), scope)).into_response()
            }
            Err(rejection) => *rejection,
        };
    }
    match state
        .engine
        .tag_names(tenant.as_str(), scope, start_ns, end_ns)
        .await
    {
        Ok(tags) => Json(render(tags, scope)).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}
