use super::{
    Bytes, MetricStore, OPERATION_RULE_GROUP_SET, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE,
    RESOURCE_TENANT, Response, RulerConfigRequest, authorize_config_tenant,
    record_ruler_config_change, resource, store_ruler_config_group,
};

pub(crate) async fn set_ruler_config_group<S: MetricStore>(
    RulerConfigRequest {
        state: super::State(state),
        principal: super::Extension(principal),
        peer,
        headers,
        namespace: super::Path(namespace),
    }: RulerConfigRequest<S>,
    body: Bytes,
) -> Response {
    let tenant = match authorize_config_tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let mut resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
    ];
    let previous_rules = state.ruler_rules.read().ok().map(|rules| rules.clone());
    let stored = store_ruler_config_group(&state, tenant.clone(), &namespace, &body);
    let response =
        super::persist_ruler_config_or_restore(&state, &tenant, previous_rules, stored.response)
            .await;
    if let Some(group_name) = stored.group_name {
        resources.push(resource(
            RESOURCE_RULE_GROUP,
            format!("{namespace}/{group_name}"),
        ));
    }
    record_ruler_config_change(
        &state.audit,
        &principal,
        peer,
        OPERATION_RULE_GROUP_SET,
        resources,
        &response,
    );
    response
}
