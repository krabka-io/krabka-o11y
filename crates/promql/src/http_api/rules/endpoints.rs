use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE};

use super::{
    ApiError, Arc, Bytes, ConnectInfo, Extension, HeaderMap, IntoResponse, MetricStore,
    OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET, OPERATION_RULE_NAMESPACE_DELETE, Path,
    PeerAddr, Principal, PrometheusApiState, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE,
    RESOURCE_TENANT, RawQuery, RequestAuth, Response, RuleGroupPath, RuleRenderOptions,
    RuleTypeFilter, RulerConfigRequest, State, StatusCode, accepted_response,
    authorize_config_tenant, json, parse_rules_params, prometheus_rule_groups_json,
    record_ruler_config_change, resource, response_with_status, store_ruler_config_group,
    success_data_response, tenant_ruler_rules, yaml_response,
};

pub(crate) async fn delete_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    headers: HeaderMap,
    Path(RuleGroupPath {
        namespace,
        group_name,
    }): Path<RuleGroupPath>,
) -> Response {
    let tenant = super::authorized_ruler_group_tenant_or_return!(
        &headers,
        &principal,
        super::authorized_ruler_group_tenant
    );
    let previous_rules = state.ruler_rules.read().ok().map(|rules| rules.clone());
    let response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(groups) = rules
                .get_mut(&tenant)
                .and_then(|namespaces| namespaces.get_mut(&namespace))
            {
                if groups.remove(&group_name).is_none() {
                    return response_with_status(StatusCode::NOT_FOUND, "group does not exist\n");
                }
                if groups.is_empty() {
                    rules
                        .get_mut(&tenant)
                        .expect("the tenant exists while its group is removed")
                        .remove(&namespace);
                }
                if rules.get(&tenant).is_some_and(BTreeMap::is_empty) {
                    rules.remove(&tenant);
                }
                accepted_response()
            } else {
                response_with_status(StatusCode::NOT_FOUND, "group does not exist\n")
            }
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let response =
        super::persist_ruler_config_or_restore(&state, &tenant, previous_rules, response).await;
    record_ruler_config_change(
        &state.audit,
        &principal,
        peer,
        OPERATION_RULE_GROUP_DELETE,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
            resource(RESOURCE_RULE_GROUP, format!("{namespace}/{group_name}")),
        ],
        &response,
    );
    response
}

pub(crate) async fn delete_ruler_config_namespace<S: MetricStore>(
    RulerConfigRequest {
        state: super::State(state),
        principal: super::Extension(principal),
        peer,
        headers,
        namespace: super::Path(namespace),
    }: RulerConfigRequest<S>,
) -> Response {
    let tenant = match authorize_config_tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let mut response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            let removed = rules
                .get_mut(&tenant)
                .and_then(|namespaces| namespaces.remove(&namespace));
            if removed.is_none() {
                response_with_status(StatusCode::NOT_FOUND, "group namespace does not exist\n")
            } else {
                if rules.get(&tenant).is_some_and(BTreeMap::is_empty) {
                    rules.remove(&tenant);
                }
                accepted_response()
            }
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    if response.status().is_success()
        && let Err(error) = state.persist_ruler_config(&tenant).await
    {
        response = response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist ruler config: {error}\n"),
        );
    }
    record_ruler_config_change(
        &state.audit,
        &principal,
        peer,
        OPERATION_RULE_NAMESPACE_DELETE,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
        ],
        &response,
    );
    response
}

pub(crate) async fn ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(RuleGroupPath {
        namespace,
        group_name,
    }): Path<RuleGroupPath>,
) -> Response {
    let tenant = super::authorized_ruler_group_tenant_or_return!(
        &headers,
        &principal,
        super::authorized_ruler_group_tenant
    );
    let group = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .and_then(|groups| groups.get(&group_name))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match group {
        Some(group) => yaml_response(StatusCode::OK, &group),
        None => response_with_status(StatusCode::NOT_FOUND, "group does not exist\n"),
    }
}

pub(crate) async fn rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_rules_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let snapshot = match tenant_ruler_rules(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
    ) {
        Ok(tenant_rules) => tenant_rules,
        Err(rejection) => return rejection.into_response(),
    };
    let tenant = snapshot.tenant;
    let rules = snapshot.rules;
    let groups = match prometheus_rule_groups_json(
        &state,
        &tenant,
        rules,
        RuleRenderOptions {
            type_filter: RuleTypeFilter::from_param(params.rule_type.as_deref()),
            exclude_alerts: params.exclude_alerts.unwrap_or(false),
        },
        &params.rule_names,
        &params.rule_groups,
        &params.files,
    )
    .await
    {
        Ok(groups) => groups,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let start = params.group_next_token.as_deref().map_or(0, |token| {
        let decoded = URL_SAFE.decode(token).unwrap_or_default();
        groups
            .iter()
            .position(|group| {
                format!("{}/{}", group.namespace, group.stored_group_name).as_bytes()
                    >= decoded.as_slice()
            })
            .unwrap_or(groups.len())
    });
    let limit = params.group_limit.filter(|limit| *limit > 0);
    let next_token = limit.and_then(|limit| {
        groups.get(start.saturating_add(limit)).map(|group| {
            URL_SAFE.encode(format!("{}/{}", group.namespace, group.stored_group_name))
        })
    });
    let groups = groups
        .into_iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX))
        .map(|group| group.value)
        .collect::<Vec<_>>();
    let mut data = json!({ "groups": groups });
    if let Some(token) = next_token {
        data["groupNextToken"] = json!(token);
    }
    success_data_response(data)
}

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
