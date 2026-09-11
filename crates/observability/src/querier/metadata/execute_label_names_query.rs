use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Response, SeriesParams,
    label_names_data, loki_sparse_success, loki_success,
};

pub(crate) async fn execute_label_names_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_names_data(state, security, headers, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}
