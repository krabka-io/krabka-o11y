use super::{
    Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState, RawQuery,
    Response, State, cardinality_active_series_inner, parse_cardinality_params,
};

pub(crate) async fn cardinality_active_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_series_inner(state, headers, principal, params).await
}
