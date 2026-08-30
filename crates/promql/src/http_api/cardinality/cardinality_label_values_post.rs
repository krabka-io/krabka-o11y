use super::{Arc, Bytes, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State, cardinality_label_values_inner, parse_cardinality_form};

pub(crate) async fn cardinality_label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_values_inner(state, headers, params).await
}
