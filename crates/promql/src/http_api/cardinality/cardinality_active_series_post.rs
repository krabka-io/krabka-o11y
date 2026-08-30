use super::{MetricStore, State, Arc, PrometheusApiState, HeaderMap, Bytes, Response, parse_cardinality_form, IntoResponse, cardinality_active_series_inner};

pub(crate) async fn cardinality_active_series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_series_inner(state, headers, params).await
}
