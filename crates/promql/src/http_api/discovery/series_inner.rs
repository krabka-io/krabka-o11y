use super::{
    Arc, DiscoveryParams, MetricStore, PrometheusApiState, RequestAuth, Response,
    record_query_response, series_dispatch,
};

pub(crate) async fn series_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = series_dispatch(state, auth, params).await;
    record_query_response(state, "series", &response, started);
    response
}
