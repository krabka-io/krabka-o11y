use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, ScopedTag, SpanStore, StatusCode,
    TagScope, Uri, Value, is_match_all_query, optional_time_bounds, query_param, request_tenant,
    scope_param, scoped_tags_from_traces, traces_matching_filter,
};

/// Answer a tag-names request, rendering the scoped tags with `render`, which
/// is where the v1 and v2 endpoints differ.
pub(crate) async fn search_tags_inner<S>(
    state: &AppState<S>,
    principal: &Principal,
    headers: HeaderMap,
    uri: Uri,
    render: fn(Vec<ScopedTag>, Option<TagScope>) -> Value,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = match request_tenant(&headers, principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scope = match scope_param(&uri) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q")
        && !is_match_all_query(&query)
    {
        return match traces_matching_filter(state, tenant.as_str(), &query, &uri, start_ns, end_ns)
            .await
        {
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
