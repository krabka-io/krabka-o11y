use super::{
    HttpQueryError, PrometheusRulesFilters, QuerierState, Value, json, loki_yaml_mapping,
    prometheus_alerts_for_rule, prometheus_rule_response, serde_yaml_key,
};

pub(crate) async fn prometheus_rules_for_group(
    state: &QuerierState,
    tenant: &str,
    group: &serde_yaml::Value,
    filters: &PrometheusRulesFilters,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let mut response_rules = Vec::new();
    let Some(rules) = loki_yaml_mapping(group)
        .and_then(|fields| fields.get(serde_yaml_key("rules")))
        .and_then(serde_yaml::Value::as_sequence)
    else {
        return Ok(response_rules);
    };
    for source_rule in rules {
        let Some(mut rule) = prometheus_rule_response(source_rule) else {
            continue;
        };
        if !filters.matches_rule(&rule, source_rule) {
            continue;
        }
        if !filters.exclude_alerts && rule.get("type").and_then(Value::as_str) == Some("alerting") {
            let alerts =
                prometheus_alerts_for_rule(state, tenant, source_rule, evaluation_time).await?;
            rule["alerts"] = json!(alerts);
        }
        response_rules.push(rule);
    }
    Ok(response_rules)
}
