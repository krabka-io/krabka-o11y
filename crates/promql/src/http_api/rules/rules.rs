use super::*;

pub(crate) async fn rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_rules_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
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
    )
    .await
    {
        Ok(groups) => groups,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "groups": groups,
    }))
}
