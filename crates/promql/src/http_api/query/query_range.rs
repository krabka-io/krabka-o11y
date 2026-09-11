use super::{
    Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState, RawQuery,
    Response, State, query_range_inner, range_query_params_from_form,
};

pub(crate) async fn query_range<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match range_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_range_inner(state, headers, principal, params).await
}
