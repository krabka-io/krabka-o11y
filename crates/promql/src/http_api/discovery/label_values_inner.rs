use super::{MetricStore, Arc, PrometheusApiState, HeaderMap, DiscoveryParams, Response, label_values_dispatch, record_query_response};

pub(crate) async fn label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    name: String,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = label_values_dispatch(&state, &headers, name, params).await;
    record_query_response(&state, "label_values", &response, started);
    response
}
