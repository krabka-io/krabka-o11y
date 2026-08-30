use super::*;

pub(crate) async fn api_prom_series(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
