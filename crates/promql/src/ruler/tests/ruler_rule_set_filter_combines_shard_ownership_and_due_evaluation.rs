use super::*;

#[test]
pub(crate) fn ruler_rule_set_filter_combines_shard_ownership_and_due_evaluation() {
    let rules = interval_rule_set(&[
        IntervalGroup {
            namespace: "team-a",
            group_name: "new",
            interval: "30s",
        },
        IntervalGroup {
            namespace: "team-a",
            group_name: "not-yet",
            interval: "5m",
        },
        IntervalGroup {
            namespace: "team-b",
            group_name: "due",
            interval: "1m",
        },
        IntervalGroup {
            namespace: "team-c",
            group_name: "also-due",
            interval: "30s",
        },
    ]);
    let state = group_state(&[
        GroupLastEval {
            namespace: "team-a",
            group: "not-yet",
            last_eval_ms: 120_000,
        },
        GroupLastEval {
            namespace: "team-b",
            group: "due",
            last_eval_ms: 60_000,
        },
        GroupLastEval {
            namespace: "team-c",
            group: "also-due",
            last_eval_ms: 90_000,
        },
    ]);
    let shard = super::super::RulerShard::new(1, 2).expect("ruler shard");

    let sharded = super::super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
    let expected =
        super::super::filter_ruler_rule_set_due_for_eval("tenant-a", &sharded, &state, 180_000);
    let scheduled = super::super::filter_ruler_rule_set_for_shard_due_for_eval(
        "tenant-a", &rules, &state, shard, 180_000,
    );

    assert2::assert!(scheduled == expected);
}
