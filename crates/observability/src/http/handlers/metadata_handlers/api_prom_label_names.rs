use super::{
    HeaderMap, QuerierState, RawQuery, RequestSecurity, Response, State, api_prom_series_query,
    execute_api_prom_label_names_query,
};

pub(crate) async fn api_prom_label_names(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    api_prom_series_query(raw_query.as_deref(), |params| async move {
        execute_api_prom_label_names_query(&state, &security, &headers, &params).await
    })
    .await
}
