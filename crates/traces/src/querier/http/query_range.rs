use super::{QuerierRequest, Response, SpanStore, query_range_inner};

pub(crate) async fn query_range<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_range_inner(&request).await;
    request
        .state
        .record_query("query_range", resp.status().is_success(), start);
    resp
}
