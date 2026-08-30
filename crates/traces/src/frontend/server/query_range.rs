use super::{QuerierBackend, BlockCatalog, State, Arc, QueryFrontend, HeaderMap, Uri, Response, tenant, metrics_query_param, IntoResponse, StatusCode, required_time_bounds, required_step, exemplar_limit, Json, backend_error_response};

pub(crate) async fn query_range<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let step_ns = match required_step(&uri) {
        Ok(step) => step,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_limit = exemplar_limit(&uri);
    match qf
        .metrics_query(
            &tenant,
            &query,
            (start_ns, end_ns, step_ns),
            false,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}
