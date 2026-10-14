use std::collections::BTreeSet;

use super::{
    BTreeMap, MetricStore, PrometheusApiState, PromqlError, RuleRenderOptions, TenantId, TimeExt,
    Value, json, prometheus_rules_json, rfc3339_time_string, yaml_duration, yaml_string,
    zero_evaluation_time,
};

pub(crate) struct PrometheusRuleGroupJson {
    pub(crate) namespace: String,
    pub(crate) stored_group_name: String,
    pub(crate) value: Value,
}

pub(crate) async fn prometheus_rule_groups_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    options: RuleRenderOptions,
    rule_names: &BTreeSet<String>,
    rule_groups: &BTreeSet<String>,
    files: &BTreeSet<String>,
) -> Result<Vec<PrometheusRuleGroupJson>, PromqlError> {
    let mut groups = Vec::new();
    for namespace_entry in rules {
        let namespace = namespace_entry.0;
        let namespace_groups = namespace_entry.1;
        if !files.is_empty() && !files.contains(&namespace) {
            continue;
        }
        for group_entry in namespace_groups {
            let stored_group_name = group_entry.0;
            let group = group_entry.1;
            if !rule_groups.is_empty() && !rule_groups.contains(&stored_group_name) {
                continue;
            }
            let rules = prometheus_rules_json(
                state,
                tenant,
                &namespace,
                &stored_group_name,
                &group,
                options,
                rule_names,
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
            let value = json!({
                "name": group_name,
                "file": namespace,
                "interval": yaml_duration(&group, "interval").secs_i64(),
                "lastEvaluation": last_evaluation,
                "evaluationTime": status.as_ref().map_or(0.0, |status| status.evaluation_time_seconds),
                "sourceTenants": [],
                "rules": rules,
            });
            groups.push(PrometheusRuleGroupJson {
                namespace: namespace.clone(),
                stored_group_name,
                value,
            });
        }
    }
    Ok(groups)
}
