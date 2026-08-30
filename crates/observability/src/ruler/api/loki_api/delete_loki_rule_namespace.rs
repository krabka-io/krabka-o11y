use super::*;

pub(crate) async fn delete_loki_rule_namespace(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        let Some(namespaces) = rules.get_mut(&tenant) else {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        };
        if namespaces.remove(&namespace).is_none() {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        }
        if namespaces.is_empty() {
            rules.remove(&tenant);
        }
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}
