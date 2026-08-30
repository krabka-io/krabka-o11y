use super::{AppState, HeaderMap, IntoResponse, Json, Response, SpanStore, StatusCode, Uri, add_intrinsic_tags, is_match_all_query, matching_traces, optional_time_bounds, q_filter_limit, query_param, scan_options_param, scope_param, scoped_tags_from_traces, search_tags_v2_json, tenant, traceql_query_error_response};

pub(crate) async fn search_tags_v2_inner<S>(state: &AppState<S>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
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
                .tag_names(&tenant, scope, start_ns, end_ns)
                .await
            {
                Ok(tags) => {
                    Json(search_tags_v2_json(&add_intrinsic_tags(tags, scope))).into_response()
                }
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
            &tenant,
            &query,
            start_ns,
            end_ns,
            scan_options,
            limit,
        )
        .await
        {
            Ok(traces) => Json(search_tags_v2_json(&add_intrinsic_tags(
                scoped_tags_from_traces(&traces, scope),
                scope,
            )))
            .into_response(),
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_names(&tenant, scope, start_ns, end_ns)
            .await
        {
            Ok(tags) => Json(search_tags_v2_json(&add_intrinsic_tags(tags, scope))).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}
