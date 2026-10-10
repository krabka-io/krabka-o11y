use super::{
    AppState, Extension, HeaderMap, Principal, Response, SpanStore, State, TagsRequest, Uri,
    add_intrinsic_tags, search_tags_inner, search_tags_v2_json,
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
    let resp = search_tags_inner(
        &state,
        TagsRequest {
            principal: &principal,
            headers,
            uri,
            render: |tags, scope| search_tags_v2_json(&add_intrinsic_tags(tags, scope)),
        },
    )
    .await;
    state.record_query("tags", resp.status().is_success(), start);
    resp
}
