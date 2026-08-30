use super::{Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, RawQuery, Response, State, cardinality_label_values_inner, parse_cardinality_params};

pub(crate) async fn cardinality_label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_values_inner(state, headers, params).await
}
