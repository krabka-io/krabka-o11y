use super::{
    AppState, Extension, HeaderMap, Principal, Response, SpanStore, State, Uri,
    search_tags_v2_inner,
};

pub(crate) async fn search_tags_v2<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tags_v2_inner(&state, &principal, headers, uri).await;
    state.record_query("tags", resp.status().is_success(), start);
    resp
}
