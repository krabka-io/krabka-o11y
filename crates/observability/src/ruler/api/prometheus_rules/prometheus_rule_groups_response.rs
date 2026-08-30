use super::{
    HttpQueryError, LokiRuleNamespaces, PrometheusRuleGroupResponse, PrometheusRulesFilters,
    PrometheusRulesPage, QuerierState, json, loki_rule_group_name,
    prometheus_rule_group_interval_seconds, prometheus_rule_group_page_token,
    prometheus_rules_for_group,
};

pub(crate) async fn prometheus_rule_groups_response(
    state: &QuerierState,
    tenant: &str,
    namespaces: &LokiRuleNamespaces,
    filters: &PrometheusRulesFilters,
    evaluation_time: i64,
) -> Result<PrometheusRulesPage, HttpQueryError> {
    let mut response_groups = Vec::new();
    for (namespace, groups) in namespaces {
        if !filters.files.is_empty() && !filters.files.contains(namespace) {
            continue;
        }
        for group in groups.values() {
            let Some(name) = loki_rule_group_name(group) else {
                continue;
            };
            if !filters.rule_groups.is_empty() && !filters.rule_groups.contains(name) {
                continue;
            }
            let rules =
                prometheus_rules_for_group(state, tenant, group, filters, evaluation_time).await?;
            if filters.has_rule_filter() && rules.is_empty() {
                continue;
            }
            response_groups.push(PrometheusRuleGroupResponse {
                token: prometheus_rule_group_page_token(namespace, name),
                value: json!({
                    "name": name,
                    "file": namespace,
                    "interval": prometheus_rule_group_interval_seconds(group),
                    "limit": 0,
                    "rules": rules,
                }),
            });
        }
    }
    filters.page_groups(response_groups)
}
