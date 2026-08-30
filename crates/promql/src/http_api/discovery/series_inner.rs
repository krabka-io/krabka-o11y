use super::{Arc, DiscoveryParams, HeaderMap, MetricStore, PrometheusApiState, Response, record_query_response, series_dispatch};

pub(crate) async fn series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = series_dispatch(&state, &headers, params).await;
    record_query_response(&state, "series", &response, started);
    response
}
