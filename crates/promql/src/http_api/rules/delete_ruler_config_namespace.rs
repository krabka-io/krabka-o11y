use super::{ApiError, Arc, HeaderMap, IntoResponse, MetricStore, Path, PrometheusApiState, Response, State, StatusCode, tenant_from_headers};

pub(crate) async fn delete_ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant) {
                namespaces.remove(&namespace);
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}
