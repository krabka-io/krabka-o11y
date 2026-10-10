use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, SpanStore, StatusCode,
    TypedValue, Uri, Value, exact_tag_value_filter, filter_tag_values, is_match_all_query,
    optional_time_bounds, query_param, request_tenant, tag_values_from_traces, tempo_tag_alias,
    traceql_query_error_response, traces_matching_filter,
};

/// Answer a tag-values request, rendering the values with `render`, which is
/// where the v1 and v2 endpoints differ.
pub(crate) async fn search_tag_values_inner<S>(
    state: &AppState<S>,
    principal: &Principal,
    headers: HeaderMap,
    tag: String,
    uri: Uri,
    render: fn(&[TypedValue]) -> Value,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = match request_tenant(&headers, principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let tag = tempo_tag_alias(&tag);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut expected = None;
    if let Some(query) = query_param(&uri, "q")
        && !is_match_all_query(&query)
    {
        match exact_tag_value_filter(&query, tag) {
            Ok(Some(value)) => expected = Some(value),
            Ok(None) => {
                return match traces_matching_filter(
                    state,
                    tenant.as_str(),
                    &query,
                    &uri,
                    start_ns,
                    end_ns,
                )
                .await
                {
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
