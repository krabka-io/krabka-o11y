use super::{
    HeaderMap, IntoResponse, QuerierState, QueryKind, Response, api_prom_streams_only_response,
    execute_http_query, parse_query_params,
};

pub(crate) async fn handle_api_prom_query(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, QueryKind::Instant).await {
        Ok(value) => api_prom_streams_only_response(&value),
        Err(error) => error.into_response(),
    }
}
