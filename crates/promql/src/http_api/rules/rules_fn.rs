use base64::{Engine as _, engine::general_purpose::URL_SAFE};

use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RawQuery, Response, RuleRenderOptions, RuleTypeFilter, State, authorized_tenant_from_headers,
    json, parse_rules_params, prometheus_rule_groups_json, success_data_response,
};

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
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
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
            .position(|(namespace, group, _)| {
                format!("{namespace}/{group}").as_bytes() >= decoded.as_slice()
            })
            .unwrap_or(groups.len())
    });
    let limit = params.group_limit.filter(|limit| *limit > 0);
    let next_token = limit.and_then(|limit| {
        groups
            .get(start.saturating_add(limit))
            .map(|(namespace, group, _)| URL_SAFE.encode(format!("{namespace}/{group}")))
    });
    let groups = groups
        .into_iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX))
        .map(|(_, _, group)| group)
        .collect::<Vec<_>>();
    let mut data = json!({ "groups": groups });
    if let Some(token) = next_token {
        data["groupNextToken"] = json!(token);
    }
    success_data_response(data)
}
