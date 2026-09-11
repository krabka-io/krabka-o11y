use super::{
    Arc, Bytes, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    Response, State, labels_inner, parse_discovery_form,
};

pub(crate) async fn labels_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, principal, params).await
}
