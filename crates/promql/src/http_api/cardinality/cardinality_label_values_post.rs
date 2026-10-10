use super::{
    Arc, CardinalityParams, MetricStore, ParsedForm, PrometheusApiState, RequestCaller, Response,
    State, cardinality_label_values_inner,
};

pub(crate) async fn cardinality_label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<CardinalityParams>,
) -> Response {
    cardinality_label_values_inner(&state, caller.auth(), params).await
}
