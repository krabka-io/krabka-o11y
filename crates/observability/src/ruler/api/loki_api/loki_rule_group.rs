use super::{
    HttpQueryError, LokiRuleGroupPath, Path, QuerierState, RequestSecurity, Response, RulerTenant,
    State, StatusCode, loki_yaml_response, text_response,
};

pub(crate) async fn loki_rule_group(
    State(state): State<QuerierState>,
    // Extracted ahead of the path, so a request without a principal is
    // refused first; `RulerTenant` reads the principal again.
    _security: RequestSecurity,
    Path(path): Path<LokiRuleGroupPath>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(groups) = rules
        .get(tenant.as_str())
        .and_then(|namespaces| namespaces.get(&path.namespace))
    else {
        return Ok(text_response(
            StatusCode::BAD_REQUEST,
            "GetRuleGroup unsupported in rule local store\n",
        ));
    };
    let Some(group) = groups.get(&path.group_name) else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "group does not exist\n",
        ));
    };
    Ok(loki_yaml_response(StatusCode::OK, group))
}
