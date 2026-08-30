use super::*;

pub(crate) async fn execute_api_prom_series_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    Ok(loki_success(series_data(state, headers, params).await?))
}
