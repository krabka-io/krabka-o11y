use super::{
    Arc, DiscoveryParams, MetricStore, ParsedForm, PrometheusApiState, RequestCaller, Response,
    State, labels_inner,
};

pub(crate) async fn labels_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<DiscoveryParams>,
) -> Response {
    labels_inner(&state, caller.auth(), params).await
}
