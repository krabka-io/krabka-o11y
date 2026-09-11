use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, exemplar_limit,
    metrics_query_param, optional_seconds, query_param, request_tenant, required_time_bounds,
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
    // Instant query: a window via start/end, else a single `time` point.
    let (start_ns, end_ns) =
        if query_param(&uri, "start").is_some() || query_param(&uri, "end").is_some() {
            match required_time_bounds(&uri) {
                Ok(bounds) => bounds,
                Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
            }
        } else {
            let ts = match optional_seconds(&uri, "time") {
                Ok(value) => value.unwrap_or(0),
                Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
            };
            (ts, ts)
        };
    let step_ns = end_ns.saturating_sub(start_ns).max(1);
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
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}
