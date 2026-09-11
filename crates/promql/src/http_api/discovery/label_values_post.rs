use super::{
    Arc, Bytes, Extension, HeaderMap, IntoResponse, MetricStore, Path, Principal,
    PrometheusApiState, Response, State, label_values_inner, parse_discovery_form,
};

pub(crate) async fn label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, principal, name, params).await
}
