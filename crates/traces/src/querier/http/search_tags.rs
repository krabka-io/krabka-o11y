use super::{
    QuerierRequest, Response, SpanStore, TagsRequest, search_tags_inner, search_tags_json,
};

pub(crate) async fn search_tags<S>(request: QuerierRequest<S>) -> Response
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
            render: |tags, _| search_tags_json(&tags),
        },
    )
    .await;
    state.record_query("tags", resp.status().is_success(), start);
    resp
}
