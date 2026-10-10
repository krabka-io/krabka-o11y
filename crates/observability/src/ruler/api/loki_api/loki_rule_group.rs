use super::{
    HeaderMap, HttpQueryError, Path, QuerierState, RequestSecurity, Response, State, StatusCode,
    TenantErrorSurface, authorized_ruler_tenant, loki_yaml_response, text_response,
};

pub(crate) async fn loki_rule_group(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path((namespace, group_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, HttpQueryError> {
    let tenant =
        authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await?;
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(groups) = rules
        .get(tenant.as_str())
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return Ok(text_response(
            StatusCode::BAD_REQUEST,
            "GetRuleGroup unsupported in rule local store\n",
        ));
    };
    let Some(group) = groups.get(&group_name) else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "group does not exist\n",
        ));
    };
    Ok(loki_yaml_response(StatusCode::OK, group))
}
