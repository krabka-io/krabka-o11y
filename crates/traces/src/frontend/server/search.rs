use super::{
    BlockCatalog, FrontendRequest, IntoResponse, Json, QuerierBackend, Response, RouteVariant,
    SearchDelivery, StatusCode, backend_error_response, bounded_count, ndjson_search_stream,
    search_request, status_text_response,
};

/// `/api/search`, or for [`SearchDelivery::Streamed`] the NDJSON stream of
/// cumulative responses that `/api/search/stream` sends as shards complete.
pub(crate) async fn search<B, C, D>(request: FrontendRequest<B, C>) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    D: RouteVariant<SearchDelivery>,
{
    let search = match search_request(request.tenant_request()) {
        Ok(search) => search,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, uri, .. } = request;
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());

    if matches!(D::VARIANT, SearchDelivery::Streamed) {
        return match qf
            .search_stream(
                &search.tenant,
                &search.query,
                search.bounds.start_ns,
                search.bounds.end_ns,
                limit,
                spss,
            )
            .await
        {
            Ok(receiver) => ndjson_search_stream(receiver),
            Err(error) => status_text_response(StatusCode::BAD_GATEWAY, error.to_string()),
        };
    }
    match qf
        .search(
            &search.tenant,
            &search.query,
            search.bounds.start_ns,
            search.bounds.end_ns,
            limit,
            spss,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}
