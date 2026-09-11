use super::{
    Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState, RawQuery,
    Response, State, parse_discovery_params, series_inner,
};

pub(crate) async fn series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    series_inner(state, headers, principal, params).await
}
