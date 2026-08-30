use super::{AppState, HeaderMap, Response, SpanStore, State, Uri, query_range_inner};

pub(crate) async fn query_range<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_range_inner(&state, headers, uri).await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}
