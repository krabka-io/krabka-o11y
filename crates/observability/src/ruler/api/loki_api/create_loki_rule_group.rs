use super::{
    AuditOutcome, Bytes, HeaderMap, HttpQueryError, IntoResponse, OPERATION_RULE_GROUP_SET, Path,
    QuerierState, RESOURCE_RULE_GROUP, RESOURCE_TENANT, RequestSecurity, Response, State,
    StatusCode, TenantErrorSurface, authorized_ruler_tenant, json, json_response,
    loki_rule_group_name, parse_loki_rule_group, resource, text_response,
};

/// `POST /loki/api/v1/rules/{namespace}`: create or replace one rule group.
///
/// The audit trail records each attempt to store a group that parsed, with
/// the outcome of the write.
pub(crate) async fn create_loki_rule_group(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant =
        match authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await
        {
            Ok(tenant) => tenant,
            Err(response) => return response,
        };
    let Ok(rule_group) = parse_loki_rule_group(&body) else {
        return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n");
    };
    let name = match loki_rule_group_name(&rule_group) {
        Some(name) => name.to_string(),
        None => return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n"),
    };
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_GROUP, format!("{namespace}/{name}")),
    ];
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        rules
            .entry(tenant.as_str().to_owned())
            .or_default()
            .entry(namespace)
            .or_default()
            .insert(name, rule_group);
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        security.admin_operation(OPERATION_RULE_GROUP_SET, resources, AuditOutcome::Failure);
        return HttpQueryError::from(error).into_response();
    }
    security.admin_operation(OPERATION_RULE_GROUP_SET, resources, AuditOutcome::Success);
    state.alert_states.clear_tenant(tenant.as_str());
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}
