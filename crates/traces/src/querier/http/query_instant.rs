use super::{QuerierRequest, Response, SpanStore, query_instant_inner};

pub(crate) async fn query_instant<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_instant_inner(&request).await;
    request.state.record_query("query", resp.status(), start);
    resp
}
