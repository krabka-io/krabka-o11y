use super::{
    HttpQueryError, OPERATION_RULE_NAMESPACE_DELETE, Path, QuerierState, RESOURCE_RULE_NAMESPACE,
    RESOURCE_TENANT, RequestSecurity, Response, RulerTenant, State, StatusCode, resource,
    text_response,
};

/// `DELETE /loki/api/v1/rules/{namespace}`: delete every rule group in a
/// namespace.
///
/// The audit trail records each deletion of a namespace that exists, with the
/// outcome of the write. A namespace that does not exist gets 404 and changes
/// nothing, so it records nothing.
pub(crate) async fn delete_loki_rule_namespace(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(namespace): Path<String>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
    ];
    let Some(snapshot) =
        super::snapshot_after_rule_removal(&state, tenant.as_str(), |namespaces| {
            namespaces.remove(&namespace).is_some()
        })
    else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "no rule groups found\n",
        ));
    };
    super::deleted_rule_response(
        &state,
        security,
        OPERATION_RULE_NAMESPACE_DELETE,
        resources,
        tenant.as_str(),
        &snapshot,
    )
}
