use super::{
    IntoResponse, Json, QuerierRequest, QueryEnforcer, Response, SpanStore, StatusCode, UnixNano,
    exemplar_selection, filter_metrics_exemplars, limit_error_response, required_time_range,
    scan_options_param, status_text_response, step_param, tempo_metric_bounds, trace_metrics_json,
};

pub(crate) async fn query_range_inner<S>(request: &QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let metrics = match request.metrics_request() {
        Ok(request) => request,
        Err(rejection) => return *rejection,
    };
    let time_range = match required_time_range(uri) {
        Ok(range) => range,
        Err(rejection) => return *rejection,
    };
    let limits = state.cfg.limits_for_tenant(&metrics.tenant);
    if let Err(err) =
        QueryEnforcer::check_search_duration(&limits, time_range.start_ns, time_range.end_ns)
    {
        return limit_error_response(&err);
    }
    let step_ns = match step_param(
        uri,
        UnixNano(time_range.start_ns),
        UnixNano(time_range.end_ns),
    ) {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let bounds = match tempo_metric_bounds(time_range.start_ns, time_range.end_ns, step_ns) {
        Ok(bounds) => bounds,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let exemplar_selection = exemplar_selection(uri);
    let mut scan_options = match scan_options_param(uri) {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    // Public requests and the frontend's single unrestricted Live job need
    // final label decoding; explicit per-block jobs retain raw worker labels.
    scan_options.tempo_frontend_labels = scan_options.job.is_none();
    let scan_bounds = bounds
        .as_ref()
        .map_or(time_range, |bounds| super::TimeBounds {
            start_ns: bounds.scan_start_ns,
            end_ns: bounds.scan_end_ns,
        });

    match state
        .engine
        .query_range_with_options(
            metrics.tenant.as_str(),
            &metrics.query,
            scan_bounds.start_ns,
            scan_bounds.end_ns,
            step_ns,
            scan_options,
        )
        .await
    {
        Ok(mut resp) => {
            if let Some(bounds) = bounds {
                if let Err(err) = bounds.shift_points(&mut resp) {
                    return status_text_response(StatusCode::BAD_REQUEST, err);
                }
            } else {
                resp.series.clear();
            }
            Json(trace_metrics_json(
                &filter_metrics_exemplars(resp, exemplar_selection),
                &metrics.query,
            ))
            .into_response()
        }
        Err(err) => status_text_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}
