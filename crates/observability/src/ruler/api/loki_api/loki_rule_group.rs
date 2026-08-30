use super::{
    HeaderMap, IntoResponse, Path, QuerierState, Response, State, StatusCode, loki_ruler_tenant,
    loki_yaml_response, text_response,
};

pub(crate) async fn loki_rule_group(
    State(state): State<QuerierState>,
    Path((namespace, group_name)): Path<(String, String)>,
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
    let Some(groups) = rules
        .get(&tenant)
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "GetRuleGroup unsupported in rule local store\n",
        );
    };
    let Some(group) = groups.get(&group_name) else {
        return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
    };
    loki_yaml_response(StatusCode::OK, group)
}
