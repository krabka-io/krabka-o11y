use super::*;

pub(crate) async fn search<S>(State(state): State<AppState<S>>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_inner(&state, headers, uri).await;
    state.record_query("search", resp.status().is_success(), start);
    resp
}
