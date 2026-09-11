use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, SpanStore, StatusCode, Uri,
    is_match_all_query, matching_traces, optional_time_bounds, q_filter_limit, query_param,
    request_tenant, scan_options_param, scope_param, scoped_tags_from_traces, search_tags_json,
    traceql_query_error_response,
};

pub(crate) async fn search_tags_inner<S>(
    state: &AppState<S>,
    principal: &Principal,
    headers: HeaderMap,
    uri: Uri,
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
    if let Some(query) = query_param(&uri, "q") {
        if is_match_all_query(&query) {
            return match state
                .engine
                .tag_names(tenant.as_str(), scope, start_ns, end_ns)
                .await
            {
                Ok(tags) => Json(search_tags_json(&tags)).into_response(),
                Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
            };
        }
        let scan_options = match scan_options_param(&uri) {
            Ok(value) => value,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        let limit = match q_filter_limit(
            &uri,
            state.engine.max_traces(),
            state.cfg.tag_query_filter_autocomplete_limit,
        ) {
            Ok(value) => value,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        match matching_traces(
            state.engine.as_ref(),
            tenant.as_str(),
            &query,
            start_ns,
            end_ns,
            scan_options,
            limit,
        )
        .await
        {
            Ok(traces) => {
                Json(search_tags_json(&scoped_tags_from_traces(&traces, scope))).into_response()
            }
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_names(tenant.as_str(), scope, start_ns, end_ns)
            .await
        {
            Ok(tags) => Json(search_tags_json(&tags)).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}
