use super::{BTreeMap, RulerGroupState, filter_ruler_rule_set, ruler_group_due_for_eval};

/// Returns the rule groups whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    filter_ruler_rule_set(rules, |namespace, group_name, group| {
        ruler_group_due_for_eval(
            tenant,
            namespace,
            group_name,
            group,
            group_state,
            eval_time_ms,
        )
    })
}
