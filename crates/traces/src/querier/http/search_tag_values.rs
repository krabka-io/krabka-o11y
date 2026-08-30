use super::{SpanStore, State, Path, AppState, HeaderMap, Uri, Response, search_tag_values_inner};

pub(crate) async fn search_tag_values<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tag_values_inner(&state, headers, tag, uri).await;
    state.record_query("tag_values", resp.status().is_success(), start);
    resp
}
