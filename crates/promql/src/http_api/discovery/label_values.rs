use super::{MetricStore, State, Path, RawQuery, Arc, PrometheusApiState, HeaderMap, Response, parse_discovery_params, IntoResponse, label_values_inner};

pub(crate) async fn label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, name, params).await
}
