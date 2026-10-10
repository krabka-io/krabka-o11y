use super::{QuerierRequest, Response, SpanStore, search_tags_inner, search_tags_json};

pub(crate) async fn search_tags<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tags_inner(&request, |tags, _| search_tags_json(&tags)).await;
    request.state.record_query("tags", resp.status(), start);
    resp
}
