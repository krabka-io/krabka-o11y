use super::{
    QuerierRequest, Response, SpanStore, TagsRequest, add_intrinsic_tags, search_tags_inner,
    search_tags_v2_json,
};

pub(crate) async fn search_tags_v2<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest {
        state,
        principal,
        headers,
        uri,
    } = request;
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
    state.record_query("tags", resp.status(), start);
    resp
}
