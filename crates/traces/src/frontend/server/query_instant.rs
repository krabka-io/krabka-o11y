use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, exemplar_limit,
    metrics_query_param, request_tenant,
};

pub(crate) async fn query_instant<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = match request_tenant(&headers, &principal, &qf.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let (start_ns, end_ns, step_ns, _) = match crate::querier::http::instant_metric_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_limit = exemplar_limit(&uri);
    match qf
        .metrics_query(
            &tenant,
            &query,
            (start_ns, end_ns, step_ns),
            true,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => Json(crate::querier::http::instant_metrics_response(
            resp.series.iter().filter_map(|series| {
                series.samples.first().map(|sample| {
                    (
                        serde_json::to_value(&series.labels).expect("JSON metric labels"),
                        sample.value,
                    )
                })
            }),
        ))
        .into_response(),
        Err(err) => backend_error_response(&err),
    }
}
