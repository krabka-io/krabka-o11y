use super::{
    Arc, CardinalityParams, MetricStore, ParsedQuery, PrometheusApiState, RequestCaller, Response,
    State, cardinality_label_values_inner,
};

pub(crate) async fn cardinality_label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<CardinalityParams>,
) -> Response {
    cardinality_label_values_inner(&state, caller.auth(), params).await
}
