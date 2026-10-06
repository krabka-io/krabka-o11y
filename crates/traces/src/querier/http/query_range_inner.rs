use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, QueryEnforcer, Response, SpanStore,
    StatusCode, UnixNano, Uri, exemplar_selection, filter_metrics_exemplars, limit_error_response,
    metrics_query_param, request_tenant, required_seconds_param, scan_options_param, step_param,
    tempo_metric_bounds, trace_metrics_json,
};

pub(crate) async fn query_range_inner<S>(
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
    if let Err(err) = QueryEnforcer::check_search_duration(&limits, start_ns, end_ns) {
        return limit_error_response(&err);
    }
    let step_ns = match step_param(&uri, UnixNano(start_ns), UnixNano(end_ns)) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let bounds = match tempo_metric_bounds(start_ns, end_ns, step_ns) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_selection = exemplar_selection(&uri);
    let mut scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    // Public requests and the frontend's single unrestricted Live job need
    // final label decoding; explicit per-block jobs retain raw worker labels.
    scan_options.tempo_frontend_labels = scan_options.job.is_none();
    let (scan_start_ns, scan_end_ns) = bounds.as_ref().map_or((start_ns, end_ns), |bounds| {
        (bounds.scan_start_ns, bounds.scan_end_ns)
    });

    match state
        .engine
        .query_range_with_options(
            tenant.as_str(),
            &query,
            scan_start_ns,
            scan_end_ns,
            step_ns,
            scan_options,
        )
        .await
    {
        Ok(mut resp) => {
            if let Some(bounds) = bounds {
                if let Err(err) = bounds.shift_points(&mut resp) {
                    return (StatusCode::BAD_REQUEST, err).into_response();
                }
            } else {
                resp.series.clear();
            }
            Json(trace_metrics_json(
                &filter_metrics_exemplars(resp, exemplar_selection),
                &query,
            ))
            .into_response()
        }
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}
