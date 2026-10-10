use super::{
    Arc, DiscoveryParams, MetricStore, ParsedQuery, PrometheusApiState, RequestCaller, Response,
    State, series_inner,
};

pub(crate) async fn series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<DiscoveryParams>,
) -> Response {
    series_inner(&state, caller.auth(), params).await
}
