use super::{MetricStore, State, RawQuery, Arc, PrometheusApiState, HeaderMap, Response, parse_discovery_params, IntoResponse, series_inner};

pub(crate) async fn series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    series_inner(state, headers, params).await
}
