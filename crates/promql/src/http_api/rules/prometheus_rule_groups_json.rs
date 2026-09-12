use super::{
    BTreeMap, MetricStore, PrometheusApiState, PromqlError, RuleRenderOptions, TenantId, TimeExt,
    Value, json, prometheus_rules_json, rfc3339_time_string, yaml_duration, yaml_string,
    zero_evaluation_time,
};

pub(crate) async fn prometheus_rule_groups_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    options: RuleRenderOptions,
) -> Result<Vec<Value>, PromqlError> {
    let mut groups = Vec::new();
    for (namespace, namespace_groups) in rules {
        for (stored_group_name, group) in namespace_groups {
            let rules = prometheus_rules_json(
                state,
                tenant,
                &namespace,
                &stored_group_name,
                &group,
                options,
            )
            .await?;
            if rules.is_empty() {
                continue;
            }
            let group_name = yaml_string(&group, "name");
            let status = state.ruler_group_status(tenant, &namespace, &stored_group_name);
            let last_evaluation = state
                .ruler_group_last_eval_ms(tenant.as_str(), &namespace, &stored_group_name)
                .map_or_else(|| zero_evaluation_time().to_string(), rfc3339_time_string);
            groups.push(json!({
                "name": group_name,
                "file": namespace,
                "interval": yaml_duration(&group, "interval").secs_i64(),
                "lastEvaluation": last_evaluation,
                "evaluationTime": status.as_ref().map_or(0.0, |status| status.evaluation_time_seconds),
                "lastError": status.map_or_else(String::new, |status| status.last_error),
                "limit": 0,
                "rules": rules,
            }));
        }
    }
    Ok(groups)
}
