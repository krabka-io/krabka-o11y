use super::*;

#[test]
pub(crate) fn ruler_rule_set_filter_keeps_only_groups_due_for_evaluation() {
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
    ]);

    let due = super::super::filter_ruler_rule_set_due_for_eval("tenant-a", &rules, &state, 180_000);

    let due_group_names = due
        .iter()
        .map(|(namespace, groups)| {
            (
                namespace.clone(),
                groups.keys().cloned().collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert2::assert!(
        due_group_names
            == BTreeMap::from([
                ("team-a".to_string(), BTreeSet::from(["new".to_string()])),
                ("team-b".to_string(), BTreeSet::from(["due".to_string()])),
            ])
    );
}
