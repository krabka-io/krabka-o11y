use super::{
    HeaderMap, QuerierState, RawQuery, RequestSecurity, Response, State, handle_api_prom_query,
};

pub(crate) async fn api_prom_query(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query(state, security, headers, raw_query.as_deref()).await
}
