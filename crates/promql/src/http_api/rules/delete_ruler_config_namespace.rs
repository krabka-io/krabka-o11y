use std::collections::BTreeMap;

use super::{
    ApiError, Arc, ConnectInfo, Extension, HeaderMap, IntoResponse, MetricStore,
    OPERATION_RULE_NAMESPACE_DELETE, Path, PeerAddr, Principal, PrometheusApiState,
    RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, Response, State, StatusCode, accepted_response,
    authorized_tenant_from_headers, record_ruler_config_change, resource,
};

pub(crate) async fn delete_ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let mut response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            let removed = rules
                .get_mut(&tenant)
                .and_then(|namespaces| namespaces.remove(&namespace));
            if removed.is_none() {
                (StatusCode::NOT_FOUND, "group namespace does not exist\n").into_response()
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
        OPERATION_RULE_NAMESPACE_DELETE,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
        ],
        &response,
    );
    response
}
