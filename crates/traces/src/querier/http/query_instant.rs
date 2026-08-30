use super::*;

pub(crate) async fn query_instant<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_instant_inner(&state, headers, uri).await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}
