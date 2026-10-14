use std::collections::BTreeMap;

use krabka_units::prelude::*;

use super::{
    RulerGroupState,
    config::{stable_hash_parts, yaml_duration},
};
use crate::PromqlError;

mod filter_ruler_rule_set_due_for_eval;
mod filter_ruler_rule_set_for_shard;
mod filter_ruler_rule_set_for_shard_due_for_eval;
mod ruler_group_due_for_eval;
mod ruler_shard;

pub use filter_ruler_rule_set_due_for_eval::filter_ruler_rule_set_due_for_eval;
pub use filter_ruler_rule_set_for_shard::filter_ruler_rule_set_for_shard;
pub use filter_ruler_rule_set_for_shard_due_for_eval::filter_ruler_rule_set_for_shard_due_for_eval;
use ruler_group_due_for_eval::ruler_group_due_for_eval;
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
