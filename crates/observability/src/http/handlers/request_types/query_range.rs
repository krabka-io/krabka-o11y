use super::{HeaderMap, Instant, QuerierState, QueryKind, RawQuery, Response, State, handle_query};

pub(crate) async fn query_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = handle_query(
        state.clone(),
        headers,
        raw_query.as_deref(),
        QueryKind::Range,
    )
    .await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}
