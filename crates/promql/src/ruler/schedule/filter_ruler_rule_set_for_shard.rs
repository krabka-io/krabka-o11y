use super::{BTreeMap, RulerShard, filter_ruler_rule_set};

/// Returns the rule groups one ruler shard owns for a tenant.
#[must_use]
pub fn filter_ruler_rule_set_for_shard(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    shard: RulerShard,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    filter_ruler_rule_set(rules, |namespace, group_name, _| {
        shard.owns_group(tenant, namespace, group_name)
    })
}
