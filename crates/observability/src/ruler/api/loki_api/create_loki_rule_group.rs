use super::*;

pub(crate) async fn create_loki_rule_group(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let Ok(rule_group) = parse_loki_rule_group(&body) else {
        return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n");
    };
    let name = match loki_rule_group_name(&rule_group) {
        Some(name) => name.to_string(),
        None => return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n"),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        rules
            .entry(tenant.clone())
            .or_default()
            .entry(namespace)
            .or_default()
            .insert(name, rule_group);
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}
