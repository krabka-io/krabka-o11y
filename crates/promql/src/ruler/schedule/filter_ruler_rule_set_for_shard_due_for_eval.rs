use super::{BTreeMap, RulerGroupState, RulerShard, filter_ruler_rule_set_due_for_eval, filter_ruler_rule_set_for_shard};

/// Returns the rule groups one shard owns whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_for_shard_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    shard: RulerShard,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let sharded = filter_ruler_rule_set_for_shard(tenant, rules, shard);
    filter_ruler_rule_set_due_for_eval(tenant, &sharded, group_state, eval_time_ms)
}
