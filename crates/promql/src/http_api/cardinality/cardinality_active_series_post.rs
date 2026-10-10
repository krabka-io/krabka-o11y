use super::{
    Arc, CardinalityParams, MetricStore, ParsedForm, PrometheusApiState, RequestCaller, Response,
    State, cardinality_active_series_inner,
};

pub(crate) async fn cardinality_active_series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<CardinalityParams>,
) -> Response {
    let RequestCaller { principal, headers } = caller;
    cardinality_active_series_inner(state, headers, principal, params).await
}
