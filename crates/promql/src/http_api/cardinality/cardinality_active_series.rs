use super::{
    Arc, CardinalityParams, MetricStore, ParsedQuery, PrometheusApiState, RequestCaller, Response,
    State, cardinality_active_series_inner,
};

pub(crate) async fn cardinality_active_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<CardinalityParams>,
) -> Response {
    cardinality_active_series_inner(state, caller.headers, caller.principal, params).await
}
