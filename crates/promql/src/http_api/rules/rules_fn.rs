use base64::{Engine as _, engine::general_purpose::URL_SAFE};

use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RawQuery, RequestAuth, Response, RuleRenderOptions, RuleTypeFilter, State, json,
    parse_rules_params, prometheus_rule_groups_json, success_data_response, tenant_ruler_rules,
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
    let (tenant, rules) = match tenant_ruler_rules(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
    ) {
        Ok(tenant_rules) => tenant_rules,
        Err(rejection) => return rejection.into_response(),
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
