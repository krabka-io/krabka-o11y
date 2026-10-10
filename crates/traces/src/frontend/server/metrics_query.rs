use super::{
    BlockCatalog, FrontendRequest, IntoResponse, Json, MetricsQueryKind, QuerierBackend, Response,
    RouteVariant, StatusCode, backend_error_response, exemplar_limit, metrics_request,
    required_step, required_time_bounds,
};

/// `/api/metrics/query_range`, or for [`MetricsQueryKind::Instant`] the
/// single-sample `/api/metrics/query`.
pub(crate) async fn metrics_query<B, C, K>(request: FrontendRequest<B, C>) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    K: RouteVariant<MetricsQueryKind>,
{
    let (tenant, query) = match metrics_request(request.tenant_request()) {
        Ok(metrics) => metrics,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, uri, .. } = request;
    let instant = matches!(K::VARIANT, MetricsQueryKind::Instant);
    let bounds = if instant {
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
        .metrics_query(&tenant, &query, bounds, instant, exemplar_limit)
        .await
    {
        Ok(resp) => resp,
        Err(err) => return backend_error_response(&err),
    };
    if !instant {
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
