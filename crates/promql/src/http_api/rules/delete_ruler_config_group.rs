use std::collections::BTreeMap;

use super::{
    ApiError, Arc, ConnectInfo, Extension, HeaderMap, IntoResponse, MetricStore,
    OPERATION_RULE_GROUP_DELETE, Path, PeerAddr, Principal, PrometheusApiState,
    RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, Response, RuleGroupPath, State,
    StatusCode, accepted_response, record_ruler_config_change, resource, response_with_status,
};

pub(crate) async fn delete_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
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
    let previous_rules = state.ruler_rules.read().ok().map(|rules| rules.clone());
    let response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(groups) = rules
                .get_mut(&tenant)
                .and_then(|namespaces| namespaces.get_mut(&namespace))
            {
                if groups.remove(&group_name).is_none() {
                    return response_with_status(StatusCode::NOT_FOUND, "group does not exist\n");
                }
                if groups.is_empty() {
                    rules
                        .get_mut(&tenant)
                        .expect("the tenant exists while its group is removed")
                        .remove(&namespace);
                }
                if rules.get(&tenant).is_some_and(BTreeMap::is_empty) {
                    rules.remove(&tenant);
                }
                accepted_response()
            } else {
                response_with_status(StatusCode::NOT_FOUND, "group does not exist\n")
            }
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let response =
        super::persist_ruler_config_or_restore(&state, &tenant, previous_rules, response).await;
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
