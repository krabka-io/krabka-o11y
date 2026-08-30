use super::*;

pub(crate) async fn prometheus_alerts_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
) -> Result<Vec<Value>, PromqlError> {
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let mut alerts = Vec::new();
    for namespace_groups in rules.into_values() {
        for group in namespace_groups.into_values() {
            if let Some(group_rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) {
                for rule in group_rules {
                    alerts.extend(
                        prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await?,
                    );
                }
            }
        }
    }
    Ok(alerts)
}
