use super::{QuerierRequest, Response, SpanStore, search_inner};

pub(crate) async fn search<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_inner(&request).await;
    request
        .state
        .record_query("search", resp.status().is_success(), start);
    resp
}
