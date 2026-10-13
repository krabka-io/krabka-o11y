use super::*;

#[test]
pub(crate) fn ruler_rule_set_filter_combines_shard_ownership_and_due_evaluation() {
    let rules = standard_due_interval_rule_set(true);
    let state = staggered_group_state();
    let shard = super::super::RulerShard::new(1, 2).expect("ruler shard");

    let sharded = super::super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
    let expected =
        super::super::filter_ruler_rule_set_due_for_eval("tenant-a", &sharded, &state, 180_000);
    let scheduled = super::super::filter_ruler_rule_set_for_shard_due_for_eval(
        "tenant-a", &rules, &state, shard, 180_000,
    );

    assert2::assert!(scheduled == expected);
}
