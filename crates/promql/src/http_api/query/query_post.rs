use super::{Arc, Bytes, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State, instant_query_params_from_form, query_inner};

pub(crate) async fn query_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match instant_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_inner(state, headers, params).await
}
