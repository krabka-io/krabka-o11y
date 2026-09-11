use super::{
    ApiError, Arc, ConnectInfo, Extension, HeaderMap, IntoResponse, MetricStore,
    OPERATION_RULE_GROUP_DELETE, Path, PeerAddr, Principal, PrometheusApiState,
    RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, Response, State, StatusCode,
    authorized_tenant_from_headers, record_ruler_config_change, resource,
};

pub(crate) async fn delete_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant)
                && let Some(groups) = namespaces.get_mut(&namespace)
            {
                groups.remove(&group_name);
                if groups.is_empty() {
                    namespaces.remove(&namespace);
                }
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    record_ruler_config_change(
        &state.audit,
        &principal,
        peer,
        OPERATION_RULE_GROUP_DELETE,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
            resource(RESOURCE_RULE_GROUP, format!("{namespace}/{group_name}")),
        ],
        &response,
    );
    response
}
