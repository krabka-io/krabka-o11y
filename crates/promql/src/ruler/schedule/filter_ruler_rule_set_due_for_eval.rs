use super::*;

/// Returns the rule groups whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for (namespace, namespace_groups) in rules {
        let groups = namespace_groups
            .iter()
            .filter(|(group_name, group)| {
                ruler_group_due_for_eval(
                    tenant,
                    namespace,
                    group_name,
                    group,
                    group_state,
                    eval_time_ms,
                )
            })
            .map(|(group_name, group)| (group_name.clone(), group.clone()))
            .collect::<BTreeMap<_, _>>();
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}
