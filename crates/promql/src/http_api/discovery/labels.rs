use super::{Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, RawQuery, Response, State, labels_inner, parse_discovery_params};

pub(crate) async fn labels<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, params).await
}
