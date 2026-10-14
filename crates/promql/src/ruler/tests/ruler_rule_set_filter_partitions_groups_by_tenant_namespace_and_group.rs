use super::*;

struct RuleGroupCase {
    namespace: &'static str,
    group_name: &'static str,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RuleGroupKey {
    namespace: String,
    group_name: String,
}

struct InvalidShardCase {
    index: usize,
    total: usize,
}

#[test]
pub(crate) fn ruler_rule_set_filter_partitions_groups_by_tenant_namespace_and_group() {
    let mut rules = BTreeMap::new();
    for case in [
        RuleGroupCase {
            namespace: "team-a",
            group_name: "recording",
        },
        RuleGroupCase {
            namespace: "team-a",
            group_name: "alerting",
        },
        RuleGroupCase {
            namespace: "team-b",
            group_name: "recording",
        },
        RuleGroupCase {
            namespace: "team-c",
            group_name: "slo",
        },
    ] {
        let mut group_record = BTreeMap::new();
        group_record.insert("name", case.group_name);
        let group = serde_yaml::to_value(group_record).expect("group yaml");
        rules
            .entry(case.namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(case.group_name.to_string(), group);
    }

    let shard_count = 4;
    let mut assigned = BTreeSet::new();
    for index in 1..=shard_count {
        let shard = super::super::RulerShard::new(index, shard_count).expect("ruler shard");
        let filtered = super::super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
        for namespace_entry in filtered {
            let namespace = namespace_entry.0;
            let groups = namespace_entry.1;
            for group_entry in groups {
                let group_name = group_entry.0;
                let group = group_entry.1;
                check!(assigned.insert(RuleGroupKey {
                    namespace: namespace.clone(),
                    group_name: group_name.clone()
                }));
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
                RuleGroupKey {
                    namespace: "team-a".to_string(),
                    group_name: "alerting".to_string()
                },
                RuleGroupKey {
                    namespace: "team-a".to_string(),
                    group_name: "recording".to_string()
                },
                RuleGroupKey {
                    namespace: "team-b".to_string(),
                    group_name: "recording".to_string()
                },
                RuleGroupKey {
                    namespace: "team-c".to_string(),
                    group_name: "slo".to_string()
                },
            ])
    );
    for case in [
        InvalidShardCase {
            index: 0,
            total: shard_count,
        },
        InvalidShardCase {
            index: shard_count + 1,
            total: shard_count,
        },
        InvalidShardCase { index: 1, total: 0 },
    ] {
        assert2::assert!(super::super::RulerShard::new(case.index, case.total).is_err());
    }
}
