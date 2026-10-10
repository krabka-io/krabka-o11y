use super::{
    IntoResponse, Json, QuerierRequest, Response, SpanStore, StatusCode, instant_metric_bounds,
    scan_options_param, trace_metrics_instant_json,
};

pub(crate) async fn query_instant_inner<S>(request: &QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    instant_metrics_response(request)
        .await
        .unwrap_or_else(|rejection| *rejection)
}

/// The instant metrics answer, or the response that rejects the request.
async fn instant_metrics_response<S>(request: &QuerierRequest<S>) -> Result<Response, Box<Response>>
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let (tenant, query) = request.metrics_request()?;
    let (start_ns, end_ns, step_ns, _) =
        instant_metric_bounds(uri).map_err(|err| Box::new(bad_request_response(err)))?;
    let mut scan_options =
        scan_options_param(uri).map_err(|err| Box::new(bad_request_response(err)))?;
    // The public frontend dispatches one unrestricted job, so reduction can
    // apply final label decoding without merging already reduced averages.
    scan_options.tempo_frontend_labels = scan_options.job.is_none();
    scan_options.tempo_instant_metrics = true;

    Ok(
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
        },
    )
}

fn bad_request_response(err: String) -> Response {
    (StatusCode::BAD_REQUEST, err).into_response()
}
