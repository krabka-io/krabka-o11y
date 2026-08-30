use super::*;

pub(crate) async fn index_stats(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_stats_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_stats", resp.status().is_success(), start);
    resp
}
