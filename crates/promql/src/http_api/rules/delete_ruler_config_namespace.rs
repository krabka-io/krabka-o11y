use std::collections::BTreeMap;

use super::{
    ApiError, IntoResponse, MetricStore, OPERATION_RULE_NAMESPACE_DELETE, RESOURCE_RULE_NAMESPACE,
    RESOURCE_TENANT, Response, RulerConfigRequest, StatusCode, accepted_response,
    authorize_config_tenant, record_ruler_config_change, resource, response_with_status,
};

pub(crate) async fn delete_ruler_config_namespace<S: MetricStore>(
    RulerConfigRequest {
        state: super::State(state),
        principal: super::Extension(principal),
        peer,
        headers,
        namespace: super::Path(namespace),
    }: RulerConfigRequest<S>,
) -> Response {
    let tenant = match authorize_config_tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let mut response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            let removed = rules
                .get_mut(&tenant)
                .and_then(|namespaces| namespaces.remove(&namespace));
            if removed.is_none() {
                response_with_status(StatusCode::NOT_FOUND, "group namespace does not exist\n")
            } else {
                if rules.get(&tenant).is_some_and(BTreeMap::is_empty) {
                    rules.remove(&tenant);
                }
                accepted_response()
            }
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    if response.status().is_success()
        && let Err(error) = state.persist_ruler_config(&tenant).await
    {
        response = response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist ruler config: {error}\n"),
        );
    }
    record_ruler_config_change(
        &state.audit,
        &principal,
        peer,
        OPERATION_RULE_NAMESPACE_DELETE,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
        ],
        &response,
    );
    response
}
