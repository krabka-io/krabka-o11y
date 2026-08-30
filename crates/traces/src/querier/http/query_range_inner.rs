use super::*;

pub(crate) async fn query_range_inner<S>(state: &AppState<S>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let start_ns = match required_seconds_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end_ns = match required_seconds_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if end_ns < start_ns {
        return (StatusCode::BAD_REQUEST, "end must be >= start").into_response();
    }
    let limits = state.cfg.limits_for_tenant(&tenant);
    if let Err(err) = QueryEnforcer::check_search_duration(limits, start_ns, end_ns) {
        return limit_error_response(&err);
    }
    let step_ns = match step_param(&uri, UnixNano(start_ns), UnixNano(end_ns)) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_selection = exemplar_selection(&uri);
    let scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    match state
        .engine
        .query_range_with_options(&tenant, &query, start_ns, end_ns, step_ns, scan_options)
        .await
    {
        Ok(resp) => Json(trace_metrics_json(&filter_metrics_exemplars(
            resp,
            exemplar_selection,
        )))
        .into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}
