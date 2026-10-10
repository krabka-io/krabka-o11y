use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Response, SeriesParams,
    label_values_data, loki_sparse_success, loki_success, series_data,
};

pub(crate) async fn execute_label_values_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_values_data(series_data(state, security, headers, params).await?, name);
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}
