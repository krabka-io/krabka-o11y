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
    let start = std::time::Instant::now();
    let resp = search_tag_values_inner(&request, TagValuesRequest { tag, render }).await;
    request
        .state
        .record_query("tag_values", resp.status(), start);
    resp
}
