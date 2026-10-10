use super::{
    HeaderMap, HttpQueryError, Path, QuerierState, RequestSecurity, Response, State, StatusCode,
    TenantErrorSurface, authorized_ruler_tenant, loki_yaml_response,
    missing_loki_rule_namespace_response, text_response,
};

pub(crate) async fn loki_rule_namespace(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Result<Response, HttpQueryError> {
    let tenant =
        authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await?;
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    if !rules.contains_key(tenant.as_str()) {
        return Ok(missing_loki_rule_namespace_response(
            tenant.as_str(),
            &namespace,
        ));
    }
    let Some(groups) = rules
        .get(tenant.as_str())
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "no rule groups found\n",
        ));
    };
    Ok(loki_yaml_response(
        StatusCode::OK,
        &groups.values().cloned().collect::<Vec<_>>(),
    ))
}
