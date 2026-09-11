use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Path, Principal,
    PrometheusApiState, Response, State, StatusCode, authorized_tenant_from_headers, yaml_response,
};

pub(crate) async fn ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
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
