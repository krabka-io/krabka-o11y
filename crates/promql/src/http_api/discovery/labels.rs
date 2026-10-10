use super::{
    Arc, DiscoveryParams, MetricStore, ParsedQuery, PrometheusApiState, RequestCaller, Response,
    State, labels_inner,
};

pub(crate) async fn labels<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<DiscoveryParams>,
) -> Response {
    labels_inner(&state, caller.auth(), params).await
}
