use super::{
    Path, QuerierRequest, Response, SpanStore, TagValuesRequest, search_tag_values_inner,
    search_tag_values_json,
};

pub(crate) async fn search_tag_values<S>(
    request: QuerierRequest<S>,
    Path(tag): Path<String>,
) -> Response
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
    let resp = search_tag_values_inner(
        &state,
        TagValuesRequest {
            principal: &principal,
            headers,
            tag,
            uri,
            render: search_tag_values_json,
        },
    )
    .await;
    state.record_query("tag_values", resp.status().is_success(), start);
    resp
}
