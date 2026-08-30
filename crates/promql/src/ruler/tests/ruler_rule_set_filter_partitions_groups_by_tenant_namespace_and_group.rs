use super::*;

#[test]
pub(crate) fn ruler_rule_set_filter_partitions_groups_by_tenant_namespace_and_group() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name) in [
        ("team-a", "recording"),
        ("team-a", "alerting"),
        ("team-b", "recording"),
        ("team-c", "slo"),
    ] {
        let group =
            serde_yaml::to_value(BTreeMap::from([("name", group_name)])).expect("group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }

    let shard_count = 4;
    let mut assigned = BTreeSet::new();
    for index in 1..=shard_count {
        let shard = super::super::RulerShard::new(index, shard_count).expect("ruler shard");
        let filtered = super::super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
        for (namespace, groups) in filtered {
            for (group_name, group) in groups {
                check!(assigned.insert((namespace.clone(), group_name.clone())));
                check!(
                    group
                        == rules
                            .get(&namespace)
                            .expect("namespace")
                            .get(&group_name)
                            .expect("group")
                            .clone()
                );
                check!(shard.owns_group("tenant-a", &namespace, &group_name));
                check!(!shard.owns_group("tenant-b", &namespace, &group_name));
            }
        }
    }

    assert2::assert!(
        assigned
            == BTreeSet::from([
                ("team-a".to_string(), "alerting".to_string()),
                ("team-a".to_string(), "recording".to_string()),
                ("team-b".to_string(), "recording".to_string()),
                ("team-c".to_string(), "slo".to_string()),
            ])
    );
    for (index, total) in [(0, shard_count), (shard_count + 1, shard_count), (1, 0)] {
        assert2::assert!(super::RulerShard::new(index, total).is_err());
    }
}
