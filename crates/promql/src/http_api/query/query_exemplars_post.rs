use super::{
    Arc, Bytes, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State,
    exemplars_query_params_from_form, query_exemplars_inner,
};

pub(crate) async fn query_exemplars_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match exemplars_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_exemplars_inner(state, headers, params).await
}
