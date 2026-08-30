use super::*;

pub(crate) async fn search_tags<S>(State(state): State<AppState<S>>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tags_inner(&state, headers, uri).await;
    state.record_query("tags", resp.status().is_success(), start);
    resp
}
