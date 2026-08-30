use super::*;

/// Returns the rule groups one ruler shard owns for a tenant.
#[must_use]
pub fn filter_ruler_rule_set_for_shard(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    shard: RulerShard,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for (namespace, namespace_groups) in rules {
        let groups = namespace_groups
            .iter()
            .filter(|(group_name, _)| shard.owns_group(tenant, namespace, group_name))
            .map(|(group_name, group)| (group_name.clone(), group.clone()))
            .collect::<BTreeMap<_, _>>();
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}
