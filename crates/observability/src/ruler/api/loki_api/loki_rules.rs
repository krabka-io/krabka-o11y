use super::{
    HeaderMap, IntoResponse, QuerierState, Response, State, StatusCode,
    loki_rule_namespace_response, loki_ruler_tenant, loki_yaml_response,
    missing_loki_rule_directory_response,
};

pub(crate) async fn loki_rules(State(state): State<QuerierState>, headers: HeaderMap) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(namespaces) = rules.get(&tenant).map(loki_rule_namespace_response) else {
        return missing_loki_rule_directory_response(&tenant);
    };
    loki_yaml_response(StatusCode::OK, &namespaces)
}
