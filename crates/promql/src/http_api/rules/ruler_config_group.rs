use super::{MetricStore, State, Path, Arc, PrometheusApiState, HeaderMap, Response, tenant_from_headers, IntoResponse, ApiError, yaml_response, StatusCode};

pub(crate) async fn ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let group = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .and_then(|groups| groups.get(&group_name))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match group {
        Some(group) => yaml_response(StatusCode::OK, &group),
        None => ApiError::not_found("rule group not found").into_response(),
    }
}
