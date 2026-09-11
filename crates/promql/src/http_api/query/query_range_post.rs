use super::{
    Arc, Bytes, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    Response, State, query_range_inner, range_query_params_from_form,
};

pub(crate) async fn query_range_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match range_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_range_inner(state, headers, principal, params).await
}
