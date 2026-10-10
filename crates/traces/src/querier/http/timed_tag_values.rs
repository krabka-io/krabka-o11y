use super::{
    QuerierRequest, Response, SpanStore, TagValuesRequest, TypedValue, Value,
    search_tag_values_inner,
};

/// Answers a tag-values request for `tag`, rendering the values with
/// `render`, and records the query under `tag_values`.
pub(crate) async fn timed_tag_values<S>(
    request: QuerierRequest<S>,
    tag: String,
    render: fn(&[TypedValue]) -> Value,
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
            render,
        },
    )
    .await;
    state.record_query("tag_values", resp.status(), start);
    resp
}
