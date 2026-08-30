use super::{QuerierBackend, BlockCatalog, State, Arc, QueryFrontend, HeaderMap, Uri, Response, tenant, search_query, IntoResponse, StatusCode, required_time_bounds, bounded_count, Json, backend_error_response};

pub(crate) async fn search<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let query = match search_query(&uri) {
        Ok(Some(q)) => q,
        Ok(None) => return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response(),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());

    match qf
        .search(&tenant, &query, start_ns, end_ns, limit, spss)
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}
