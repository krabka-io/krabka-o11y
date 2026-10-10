use super::{
    Arc, DiscoveryParams, LabelValuesQuery, MetricStore, ParsedQuery, Path, PrometheusApiState,
    RequestCaller, Response, State, label_values_inner,
};

pub(crate) async fn label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    Path(name): Path<String>,
    ParsedQuery(params): ParsedQuery<DiscoveryParams>,
) -> Response {
    label_values_inner(&state, caller.auth(), LabelValuesQuery { name, params }).await
}
