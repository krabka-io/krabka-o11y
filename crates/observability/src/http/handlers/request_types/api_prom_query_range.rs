use super::*;

pub(crate) async fn api_prom_query_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query_range(state, headers, raw_query.as_deref()).await
}
