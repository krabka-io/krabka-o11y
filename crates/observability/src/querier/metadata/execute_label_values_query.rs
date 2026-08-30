use super::{
    HeaderMap, HttpQueryError, QuerierState, Response, SeriesParams, label_values_data,
    loki_sparse_success, loki_success,
};

pub(crate) async fn execute_label_values_query(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_values_data(state, headers, name, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}
