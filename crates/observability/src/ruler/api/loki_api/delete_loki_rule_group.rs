use super::{
    HttpQueryError, LokiRuleGroupPath, OPERATION_RULE_GROUP_DELETE, Path, QuerierState,
    RESOURCE_RULE_GROUP, RESOURCE_TENANT, RequestSecurity, Response, RulerTenant, State,
    StatusCode, json, json_response, resource, text_response,
};

/// `DELETE /loki/api/v1/rules/{namespace}/{group_name}`: delete one rule group.
///
/// The audit trail records each deletion of a group that exists, with the
/// outcome of the write. A group that does not exist gets 404 and changes
/// nothing, so it records nothing.
pub(crate) async fn delete_loki_rule_group(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(path): Path<LokiRuleGroupPath>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(
            RESOURCE_RULE_GROUP,
            format!("{}/{}", path.namespace, path.group_name),
        ),
    ];
    let Some(snapshot) =
        super::snapshot_after_rule_removal(&state, tenant.as_str(), |namespaces| {
            let Some(groups) = namespaces.get_mut(&path.namespace) else {
                return false;
            };
            if groups.remove(&path.group_name).is_none() {
                return false;
            }
            if groups.is_empty() {
                namespaces.remove(&path.namespace);
            }
            true
        })
    else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "group does not exist\n",
        ));
    };
    super::deleted_rule_response(
        &state,
        security,
        OPERATION_RULE_GROUP_DELETE,
        resources,
        tenant.as_str(),
        &snapshot,
    )
}
