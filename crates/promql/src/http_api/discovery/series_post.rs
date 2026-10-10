use super::{
    Arc, DiscoveryParams, MetricStore, ParsedForm, PrometheusApiState, RequestCaller, Response,
    State, series_inner,
};

pub(crate) async fn series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<DiscoveryParams>,
) -> Response {
    series_inner(&state, caller.auth(), params).await
}
