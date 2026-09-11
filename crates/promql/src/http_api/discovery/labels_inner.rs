use super::{
    Arc, DiscoveryParams, HeaderMap, MetricStore, Principal, PrometheusApiState, Response,
    labels_dispatch, record_query_response,
};

pub(crate) async fn labels_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = labels_dispatch(&state, &headers, &principal, params).await;
    record_query_response(&state, "labels", &response, started);
    response
}
