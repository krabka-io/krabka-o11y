use super::{
    Arc, DiscoveryParams, MetricStore, PrometheusApiState, RequestAuth, Response, labels_dispatch,
    record_query_response,
};

pub(crate) async fn labels_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = labels_dispatch(state, auth, params).await;
    record_query_response(state, "labels", &response, started);
    response
}
