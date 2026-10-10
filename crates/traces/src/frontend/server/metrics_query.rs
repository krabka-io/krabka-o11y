use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, exemplar_limit,
    metrics_request, required_step, required_time_bounds,
};

/// `/api/metrics/query_range`, or with `INSTANT` the single-sample
/// `/api/metrics/query`.
pub(crate) async fn metrics_query<B, C, const INSTANT: bool>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let (tenant, query) = match metrics_request(&headers, &principal, &qf.cfg.tenant_policy, &uri) {
        Ok(request) => request,
        Err(rejection) => return *rejection,
    };
    let bounds = if INSTANT {
        crate::querier::http::instant_metric_bounds(&uri)
            .map(|(start_ns, end_ns, step_ns, _)| (start_ns, end_ns, step_ns))
    } else {
        required_time_bounds(&uri).and_then(|(start_ns, end_ns)| {
            required_step(&uri).map(|step_ns| (start_ns, end_ns, step_ns))
        })
    };
    let bounds = match bounds {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_limit = exemplar_limit(&uri);
    let resp = match qf
        .metrics_query(&tenant, &query, bounds, INSTANT, exemplar_limit)
        .await
    {
        Ok(resp) => resp,
        Err(err) => return backend_error_response(&err),
    };
    if !INSTANT {
        return Json(resp).into_response();
    }
    Json(crate::querier::http::instant_metrics_response(
        resp.series.iter().filter_map(|series| {
            series.samples.first().map(|sample| {
                (
                    serde_json::to_value(&series.labels).expect("JSON metric labels"),
                    sample.value,
                )
            })
        }),
    ))
    .into_response()
}
