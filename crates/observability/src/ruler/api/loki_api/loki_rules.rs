use super::{
    HeaderMap, QuerierState, RequestSecurity, Response, State, StatusCode, TenantErrorSurface,
    authorized_ruler_tenant, loki_rule_namespace_response, loki_yaml_response,
    missing_loki_rule_directory_response,
};

pub(crate) async fn loki_rules(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
) -> Response {
    let tenant =
        match authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await
        {
            Ok(tenant) => tenant,
            Err(response) => return response,
        };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(namespaces) = rules.get(tenant.as_str()).map(loki_rule_namespace_response) else {
        return missing_loki_rule_directory_response(tenant.as_str());
    };
    loki_yaml_response(StatusCode::OK, &namespaces)
}
