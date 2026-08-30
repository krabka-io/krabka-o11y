use super::*;

pub(crate) async fn execute_label_names_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_names_data(state, headers, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}
