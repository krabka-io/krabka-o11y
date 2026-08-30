use super::{Arc, DiscoveryParams, HeaderMap, MetricStore, PrometheusApiState, Response, labels_dispatch, record_query_response};

pub(crate) async fn labels_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = labels_dispatch(&state, &headers, params).await;
    record_query_response(&state, "labels", &response, started);
    response
}
