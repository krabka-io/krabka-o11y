use super::*;

#[test]
pub(crate) fn ruler_rule_set_filter_keeps_only_groups_due_for_evaluation() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name, interval) in [
        ("team-a", "new", "30s"),
        ("team-a", "not-yet", "5m"),
        ("team-b", "due", "1m"),
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
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "not-yet".to_string(),
            last_eval_ms: 120_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "due".to_string(),
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
