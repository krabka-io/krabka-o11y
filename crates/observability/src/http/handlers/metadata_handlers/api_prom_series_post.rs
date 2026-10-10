use super::{
    Bytes, HeaderMap, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State,
    execute_api_prom_series_query, parse_posted_series_params,
};

pub(crate) async fn api_prom_series_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let params = match parse_posted_series_params(raw_query.as_deref(), &body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_series_query(&state, &security, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
