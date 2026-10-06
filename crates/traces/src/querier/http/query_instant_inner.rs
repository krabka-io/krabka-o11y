use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, SpanStore, StatusCode, Uri,
    instant_metric_bounds, metrics_query_param, request_tenant, scan_options_param,
    trace_metrics_instant_json,
};

pub(crate) async fn query_instant_inner<S>(
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
    let (start_ns, end_ns, step_ns, _) = match instant_metric_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    // The public frontend dispatches one unrestricted job, so reduction can
    // apply final label decoding without merging already reduced averages.
    scan_options.tempo_frontend_labels = scan_options.job.is_none();
    scan_options.tempo_instant_metrics = true;

    match state
        .engine
        .query_range_with_options(
            tenant.as_str(),
            &query,
            start_ns,
            end_ns,
            step_ns,
            scan_options,
        )
        .await
    {
        Ok(resp) => Json(trace_metrics_instant_json(&resp, &query)).into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}
