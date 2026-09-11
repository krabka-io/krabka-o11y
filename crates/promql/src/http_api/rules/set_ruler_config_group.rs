use super::{
    Arc, Bytes, ConnectInfo, Extension, HeaderMap, IntoResponse, MetricStore,
    OPERATION_RULE_GROUP_SET, Path, PeerAddr, Principal, PrometheusApiState, RESOURCE_RULE_GROUP,
    RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, Response, State, authorized_tenant_from_headers,
    record_ruler_config_change, resource, store_ruler_config_group,
};

pub(crate) async fn set_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let mut resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
    ];
    let (response, group_name) =
        store_ruler_config_group(&state, &headers, tenant, &namespace, &body);
    if let Some(group_name) = group_name {
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
