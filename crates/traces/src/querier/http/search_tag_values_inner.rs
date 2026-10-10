use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, SpanStore, StatusCode, TagFilter,
    TenantRequest, TimeRange, TypedValue, Uri, Value, exact_tag_value_filter, filter_tag_values,
    is_match_all_query, optional_time_bounds, query_param, tag_values_from_traces, tempo_tag_alias,
    tenant_and_bounds, traceql_query_error_response, traces_matching_filter,
};

/// A tag-values request.
pub(crate) struct TagValuesRequest<'a> {
    pub(crate) principal: &'a Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) tag: String,
    pub(crate) uri: Uri,
    /// Renders the values; this is where the v1 and v2 endpoints differ.
    pub(crate) render: fn(&[TypedValue]) -> Value,
}

/// Answer a tag-values request.
pub(crate) async fn search_tag_values_inner<S>(
    state: &AppState<S>,
    request: TagValuesRequest<'_>,
) -> Response
where
    S: SpanStore + 'static,
{
    let TagValuesRequest {
        principal,
        headers,
        tag,
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
    let tag = tempo_tag_alias(&tag);
    let mut expected = None;
    if let Some(query) = query_param(&uri, "q")
        && !is_match_all_query(&query)
    {
        match exact_tag_value_filter(&query, tag) {
            Ok(Some(value)) => expected = Some(value),
            Ok(None) => {
                let filter = TagFilter {
                    tenant: tenant.as_str(),
                    query: &query,
                    uri: &uri,
                    window: TimeRange { start_ns, end_ns },
                };
                return match traces_matching_filter(state, filter).await {
                    Ok(traces) => {
                        Json(render(&tag_values_from_traces(&traces, tag))).into_response()
                    }
                    Err(rejection) => *rejection,
                };
            }
            Err(err) => return traceql_query_error_response(&err),
        }
    }
    match state
        .engine
        .tag_values(tenant.as_str(), tag, start_ns, end_ns)
        .await
    {
        Ok(values) => match expected {
            Some(expected) => Json(render(&filter_tag_values(values, &expected))).into_response(),
            None => Json(render(&values)).into_response(),
        },
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}
