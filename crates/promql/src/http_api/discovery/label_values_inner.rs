use super::{
    Arc, LabelValuesQuery, MetricStore, PrometheusApiState, RequestAuth, Response,
    label_values_dispatch, record_query_response,
};

pub(crate) async fn label_values_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    query: LabelValuesQuery,
) -> Response {
    let started = std::time::Instant::now();
    let response = label_values_dispatch(state, auth, query).await;
    record_query_response(state, "label_values", &response, started);
    response
}
