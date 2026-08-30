use super::{ApiError, Arc, HeaderMap, IntoResponse, MetricStore, Path, PrometheusApiState, Response, State, StatusCode, tenant_from_headers, yaml_response};

pub(crate) async fn ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let groups = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match groups {
        Some(groups) => yaml_response(StatusCode::OK, &groups.into_values().collect::<Vec<_>>()),
        None => ApiError::not_found("rule namespace not found").into_response(),
    }
}
