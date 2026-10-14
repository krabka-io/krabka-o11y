use super::{
    HeaderMap, QuerierState, QueryKind, RawQuery, RequestSecurity, Response, State,
    handle_instant_or_range_query,
};

pub(crate) async fn query_range(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_instant_or_range_query(
        state.clone(),
        security,
        headers,
        raw_query.as_deref(),
        QueryKind::Range,
        "query_range",
    )
    .await
}
