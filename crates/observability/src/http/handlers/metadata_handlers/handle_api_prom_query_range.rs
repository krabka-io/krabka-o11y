use super::{
    HeaderMap, HttpQueryError, QuerierState, QueryKind, RequestSecurity, Response,
    api_prom_streams_only_response, execute_http_query, parse_query_params,
};

pub(crate) async fn handle_api_prom_query_range(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Result<Response, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let value = execute_http_query(&state, &security, &headers, params, QueryKind::Range).await?;
    Ok(api_prom_streams_only_response(&value))
}
