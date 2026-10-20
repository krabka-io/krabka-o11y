use std::collections::BTreeMap;

use krabka_units::prelude::*;

use super::{
    RulerGroupState,
    config::{stable_hash_parts, yaml_duration},
};
use crate::PromqlError;
mod ruler_shard;
pub use ruler_shard::RulerShard;

fn filter_ruler_rule_set(
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    mut keep_group: impl FnMut(&str, &str, &serde_yaml::Value) -> bool,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for namespace in rules.keys() {
        let namespace_groups = &rules[namespace];
        let mut groups = BTreeMap::new();
        for group_name in namespace_groups.keys() {
            let group = &namespace_groups[group_name];
            if keep_group(namespace, group_name, group) {
                groups.insert(group_name.clone(), group.clone());
            }
        }
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}

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

pub(crate) fn ruler_group_due_for_eval(
    tenant: &str,
    namespace: &str,
    group_name: &str,
    group: &serde_yaml::Value,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> bool {
    let Some(last_eval_ms) = group_state.last_eval_ms(tenant, namespace, group_name) else {
        return true;
    };
    // A malformed `interval` is a config error; skip the group rather than
    // treating an unparseable value as `0` and re-evaluating every tick. The
    // `for`/`expr` paths surface the same parse error as a hard failure.
    let Ok(interval) = yaml_duration(group, "interval") else {
        return false;
    };
    eval_time_ms.saturating_sub(last_eval_ms) >= interval.millis_i64()
}
