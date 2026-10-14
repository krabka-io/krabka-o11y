use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Path, Principal,
    PrometheusApiState, Response, RuleGroupPath, State, StatusCode, response_with_status,
    yaml_response,
};

pub(crate) async fn ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(RuleGroupPath {
        namespace,
        group_name,
    }): Path<RuleGroupPath>,
) -> Response {
    let tenant = super::authorized_ruler_group_tenant_or_return!(
        &headers,
        &principal,
        super::authorized_ruler_group_tenant
    );
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
        None => response_with_status(StatusCode::NOT_FOUND, "group does not exist\n"),
    }
}
