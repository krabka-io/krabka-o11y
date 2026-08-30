use super::*;

pub(crate) async fn query(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = handle_query(
        state.clone(),
        headers,
        raw_query.as_deref(),
        QueryKind::Instant,
    )
    .await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}
