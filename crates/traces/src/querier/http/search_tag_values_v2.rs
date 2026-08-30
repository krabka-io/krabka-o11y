use super::{
    AppState, HeaderMap, Path, Response, SpanStore, State, Uri, search_tag_values_v2_inner,
};

pub(crate) async fn search_tag_values_v2<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tag_values_v2_inner(&state, headers, tag, uri).await;
    state.record_query("tag_values", resp.status().is_success(), start);
    resp
}
