use super::{
    HeaderMap, IntoResponse, Path, QuerierState, Response, State, StatusCode, loki_ruler_tenant,
    loki_yaml_response, missing_loki_rule_namespace_response, text_response,
};

pub(crate) async fn loki_rule_namespace(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    if !rules.contains_key(&tenant) {
        return missing_loki_rule_namespace_response(&tenant, &namespace);
    }
    let Some(groups) = rules
        .get(&tenant)
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
    };
    loki_yaml_response(
        StatusCode::OK,
        &groups.values().cloned().collect::<Vec<_>>(),
    )
}
