use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, bounded_count,
    ndjson_search_stream, search_request,
};

/// `/api/search`, or with `STREAM` the NDJSON stream of cumulative responses
/// that `/api/search/stream` sends as shards complete.
pub(crate) async fn search<B, C, const STREAM: bool>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let (tenant, query, start_ns, end_ns) =
        match search_request(&headers, &principal, &qf.cfg.tenant_policy, &uri) {
            Ok(request) => request,
            Err(rejection) => return *rejection,
        };
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());

    if STREAM {
        return match qf
            .search_stream(&tenant, &query, start_ns, end_ns, limit, spss)
            .await
        {
            Ok(receiver) => ndjson_search_stream(receiver),
            Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
        };
    }
    match qf
        .search(&tenant, &query, start_ns, end_ns, limit, spss)
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}
