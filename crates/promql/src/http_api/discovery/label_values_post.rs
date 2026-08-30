use super::*;

pub(crate) async fn label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, name, params).await
}
