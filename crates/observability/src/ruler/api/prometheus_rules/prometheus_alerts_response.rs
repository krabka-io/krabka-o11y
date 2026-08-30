use super::*;

pub(crate) async fn prometheus_alerts_response(
    state: &QuerierState,
    tenant: &str,
    namespaces: &LokiRuleNamespaces,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let mut alerts = Vec::new();
    for groups in namespaces.values() {
        for group in groups.values() {
            let Some(rules) = loki_yaml_mapping(group)
                .and_then(|fields| fields.get(serde_yaml_key("rules")))
                .and_then(serde_yaml::Value::as_sequence)
            else {
                continue;
            };
            for rule in rules {
                alerts.extend(
                    prometheus_alerts_for_rule(state, tenant, rule, evaluation_time).await?,
                );
            }
        }
    }
    Ok(alerts)
}
