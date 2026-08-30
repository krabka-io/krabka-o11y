use super::{SpanStore, AppState, HeaderMap, Uri, Response, tenant, tempo_tag_alias, optional_time_bounds, IntoResponse, StatusCode, query_param, is_match_all_query, Json, search_tag_values_v2_json, exact_tag_value_filter, filter_tag_values, traceql_query_error_response, scan_options_param, q_filter_limit, matching_traces, tag_values_from_traces};

pub(crate) async fn search_tag_values_v2_inner<S>(
    state: &AppState<S>,
    headers: HeaderMap,
    tag: String,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let tag = tempo_tag_alias(&tag);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q") {
        if is_match_all_query(&query) {
            return match state
                .engine
                .tag_values(&tenant, tag, start_ns, end_ns)
                .await
            {
                Ok(values) => Json(search_tag_values_v2_json(&values)).into_response(),
                Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
            };
        }
        match exact_tag_value_filter(&query, tag) {
            Ok(Some(expected)) => {
                return match state
                    .engine
                    .tag_values(&tenant, tag, start_ns, end_ns)
                    .await
                {
                    Ok(values) => Json(search_tag_values_v2_json(&filter_tag_values(
                        values, &expected,
                    )))
                    .into_response(),
                    Err(err) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                    }
                };
            }
            Ok(None) => {}
            Err(err) => return traceql_query_error_response(&err),
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
            Ok(traces) => Json(search_tag_values_v2_json(&tag_values_from_traces(
                &traces, tag,
            )))
            .into_response(),
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_values(&tenant, tag, start_ns, end_ns)
            .await
        {
            Ok(values) => Json(search_tag_values_v2_json(&values)).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}
