use super::{Arc, Bytes, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State, query_range_inner, range_query_params_from_form};

pub(crate) async fn query_range_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match range_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_range_inner(state, headers, params).await
}
