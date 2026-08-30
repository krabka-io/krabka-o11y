use super::*;

#[test]
pub(crate) fn ruler_rule_set_filter_combines_shard_ownership_and_due_evaluation() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name, interval) in [
        ("team-a", "new", "30s"),
        ("team-a", "not-yet", "5m"),
        ("team-b", "due", "1m"),
        ("team-c", "also-due", "30s"),
    ] {
        let group = serde_yaml::to_value(BTreeMap::from([
            ("name", group_name),
            ("interval", interval),
        ]))
        .expect("group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }
    let mut state = super::super::RulerGroupState::default();
    state.apply_records(vec![
        super::super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "not-yet".to_string(),
            last_eval_ms: 120_000,
        },
        super::super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "due".to_string(),
            last_eval_ms: 60_000,
        },
        super::super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-c".to_string(),
            group: "also-due".to_string(),
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
