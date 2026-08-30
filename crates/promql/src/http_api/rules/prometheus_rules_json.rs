use super::{MetricStore, PrometheusApiState, RuleRenderOptions, Value, PromqlError, prometheus_rule_json};

pub(crate) async fn prometheus_rules_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    group: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Vec<Value>, PromqlError> {
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for rule in rules {
        if let Some(rule_json) = prometheus_rule_json(state, tenant, rule, options).await? {
            out.push(rule_json);
        }
    }
    Ok(out)
}
