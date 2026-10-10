use super::{
    AppState, Extension, HeaderMap, Principal, Response, SpanStore, State, Uri, search_tags_inner,
    search_tags_json,
};

pub(crate) async fn search_tags<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tags_inner(&state, &principal, headers, uri, |tags, _| {
        search_tags_json(&tags)
    })
    .await;
    state.record_query("tags", resp.status().is_success(), start);
    resp
}
