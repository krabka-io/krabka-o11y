use super::{
    HeaderMap, QuerierState, RawQuery, RequestSecurity, Response, State,
    handle_api_prom_query_range,
};

pub(crate) async fn api_prom_query_range(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query_range(state, security, headers, raw_query.as_deref()).await
}
