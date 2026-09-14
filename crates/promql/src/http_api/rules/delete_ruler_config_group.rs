use std::collections::BTreeMap;

use super::{
    ApiError, Arc, ConnectInfo, Extension, HeaderMap, IntoResponse, MetricStore,
    OPERATION_RULE_GROUP_DELETE, Path, PeerAddr, Principal, PrometheusApiState,
    RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, Response, State, StatusCode,
    accepted_response, authorized_tenant_from_headers, record_ruler_config_change, resource,
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
    let previous_rules = state.ruler_rules.read().ok().map(|rules| rules.clone());
    let mut response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(groups) = rules
                .get_mut(&tenant)
                .and_then(|namespaces| namespaces.get_mut(&namespace))
            {
                if groups.remove(&group_name).is_none() {
                    return (StatusCode::NOT_FOUND, "group does not exist\n").into_response();
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
                (StatusCode::NOT_FOUND, "group does not exist\n").into_response()
            }
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    if response.status().is_success()
        && let Err(error) = state.persist_ruler_config(&tenant).await
    {
        if let Some(previous_rules) = previous_rules
            && let Ok(mut rules) = state.ruler_rules.write()
        {
            *rules = previous_rules;
        }
        response = (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist ruler config: {error}\n"),
        )
            .into_response();
    }
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
