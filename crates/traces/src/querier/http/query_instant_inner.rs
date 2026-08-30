use super::*;

pub(crate) async fn query_instant_inner<S>(state: &AppState<S>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let (start_ns, end_ns, step_ns, point_ns) = match instant_metric_bounds(&uri) {
        Ok(bounds) => bounds,
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
            instant_metrics_response(resp, point_ns),
            exemplar_selection,
        )))
        .into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}
