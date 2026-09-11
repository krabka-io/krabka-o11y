use super::{
    AuditOutcome, HeaderMap, HttpQueryError, IntoResponse, OPERATION_RULE_NAMESPACE_DELETE, Path,
    QuerierState, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, RequestSecurity, Response, State,
    StatusCode, TenantErrorSurface, authorized_ruler_tenant, json, json_response, resource,
    text_response,
};

/// `DELETE /loki/api/v1/rules/{namespace}`: delete every rule group in a
/// namespace.
///
/// The audit trail records each deletion of a namespace that exists, with the
/// outcome of the write. A namespace that does not exist gets 404 and changes
/// nothing, so it records nothing.
pub(crate) async fn delete_loki_rule_namespace(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant =
        match authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await
        {
            Ok(tenant) => tenant,
            Err(response) => return response,
        };
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
    ];
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        let Some(namespaces) = rules.get_mut(tenant.as_str()) else {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        };
        if namespaces.remove(&namespace).is_none() {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        }
        if namespaces.is_empty() {
            rules.remove(tenant.as_str());
        }
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        security.admin_operation(
            OPERATION_RULE_NAMESPACE_DELETE,
            resources,
            AuditOutcome::Failure,
        );
        return HttpQueryError::from(error).into_response();
    }
    security.admin_operation(
        OPERATION_RULE_NAMESPACE_DELETE,
        resources,
        AuditOutcome::Success,
    );
    state.alert_states.clear_tenant(tenant.as_str());
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}
