use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Response, SeriesParams, loki_success,
    series_data,
};

pub(crate) async fn execute_api_prom_series_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    Ok(loki_success(
        series_data(state, security, headers, params).await?,
    ))
}
