use super::{HeaderMap, QuerierState, RawQuery, Response, State, handle_api_prom_query};

pub(crate) async fn api_prom_query(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query(state, headers, raw_query.as_deref()).await
}
