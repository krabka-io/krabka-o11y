use super::{MetricStore, State, Arc, PrometheusApiState, HeaderMap, Bytes, Response, parse_discovery_form, IntoResponse, labels_inner};

pub(crate) async fn labels_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, params).await
}
