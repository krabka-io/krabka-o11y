use super::*;

mod ruler_rule_set_evaluation_persists_group_last_eval_state {
    use super::*;

    #[tokio::test]
    pub(crate) async fn ruler_rule_set_evaluation_persists_group_last_eval_state() {
        let rules = namespaced_rule_set(
            r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
",
        );

        let engine = up_api_engine(&[120_000]);
        let fixture = recording_sinks_and_alert_state();
        let sinks = fixture.sinks;
        let mut alert_state = fixture.alert_state;

        let evaluation = super::super::super::evaluate_and_persist_ruler_rule_set(
            super::super::super::RulerRuleSetEvaluationRequest::new(
                &engine,
                super::super::super::RulerSinks::new(
                    &sinks.wal,
                    &sinks.alertmanager,
                    &sinks.ruler_state,
                ),
                &mut alert_state,
                &tenant_id("tenant-a"),
                &rules,
                120_000,
            ),
        )
        .await
        .expect("rule-set evaluation with state persistence");

        assert2::assert!(
            evaluation
                == super::super::super::RulerGroupEvaluation {
                    recording_records: 1,
                    alerts_dispatched: 1,
                    last_eval_ms: 120_000,
                }
        );
        assert2::assert!(
            sinks.ruler_state.group_records()
                == vec![
                    super::super::super::RulerGroupStateRecord {
                        tenant: "tenant-a".to_string(),
                        namespace: "team-a".to_string(),
                        group: "recording".to_string(),
                        last_eval_ms: 120_000,
                    },
                    super::super::super::RulerGroupStateRecord {
                        tenant: "tenant-a".to_string(),
                        namespace: "team-b".to_string(),
                        group: "alerting".to_string(),
                        last_eval_ms: 120_000,
                    },
                ]
        );
        assert2::assert!(
            sinks.ruler_state.alert_records()
                == vec![super::super::super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: BTreeMap::from([
                        ("alertname".to_string(), "InstanceUp".to_string()),
                        ("job".to_string(), "api".to_string()),
                    ])
                    .into(),
                    active_since_ms: Some(120_000),
                    keep_firing_until_ms: Some(120_000),
                }]
        );
    }
}

mod ruler_rule_set_evaluation_runs_namespaced_groups {
    use super::*;

    #[tokio::test]
    pub(crate) async fn ruler_rule_set_evaluation_runs_namespaced_groups() {
        let rules = namespaced_rule_set(
            r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
        );

        let RulerEvaluationFixture {
            engine,
            wal_sink,
            alert_sink,
            mut state,
        } = RulerEvaluationFixture::up_api_pair();

        let pending = super::super::super::evaluate_ruler_rule_set(
            &engine,
            &wal_sink,
            &alert_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rules,
            60_000,
        )
        .await
        .expect("pending rule-set evaluation");
        super::assert_pending_rule_evaluation(&pending);

        let firing = super::super::super::evaluate_ruler_rule_set(
            &engine,
            &wal_sink,
            &alert_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &rules,
            360_000,
        )
        .await
        .expect("firing rule-set evaluation");
        assert_up_rules_fired_at_six_minutes(
            &firing,
            &UpRuleSinks {
                wal_sink: &wal_sink,
                alert_sink: &alert_sink,
            },
        );
    }
}

mod ruler_rule_set_filter_combines_shard_ownership_and_due_evaluation {
    use super::*;

    #[test]
    pub(crate) fn ruler_rule_set_filter_combines_shard_ownership_and_due_evaluation() {
        let rules = standard_due_interval_rule_set(true);
        let state = staggered_group_state();
        let shard = super::super::super::RulerShard::new(1, 2).expect("ruler shard");

        let sharded =
            super::super::super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
        let expected = super::super::super::filter_ruler_rule_set_due_for_eval(
            "tenant-a", &sharded, &state, 180_000,
        );
        let scheduled = super::super::super::filter_ruler_rule_set_for_shard_due_for_eval(
            "tenant-a", &rules, &state, shard, 180_000,
        );

        assert2::assert!(scheduled == expected);
    }
}

mod ruler_rule_set_filter_keeps_only_groups_due_for_evaluation {
    use super::*;

    #[test]
    pub(crate) fn ruler_rule_set_filter_keeps_only_groups_due_for_evaluation() {
        let rules = standard_due_interval_rule_set(false);
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

        let due = super::super::super::filter_ruler_rule_set_due_for_eval(
            "tenant-a", &rules, &state, 180_000,
        );

        let mut due_group_names = BTreeMap::new();
        for entry in &due {
            due_group_names.insert(
                entry.0.clone(),
                entry.1.keys().cloned().collect::<BTreeSet<_>>(),
            );
        }
        let mut expected_group_names = BTreeMap::new();
        expected_group_names.insert("team-a".to_string(), BTreeSet::from(["new".to_string()]));
        expected_group_names.insert("team-b".to_string(), BTreeSet::from(["due".to_string()]));
        assert2::assert!(due_group_names == expected_group_names);
    }
}

mod ruler_rule_set_filter_partitions_groups_by_tenant_namespace_and_group {
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
            let shard =
                super::super::super::RulerShard::new(index, shard_count).expect("ruler shard");
            let filtered =
                super::super::super::filter_ruler_rule_set_for_shard("tenant-a", &rules, shard);
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
            assert2::assert!(super::super::super::RulerShard::new(case.index, case.total).is_err());
        }
    }
}

mod ruler_rule_set_scheduled_evaluation_runs_only_owned_due_groups {
    use super::*;

    struct ScheduledGroupCase {
        namespace: &'static str,
        group_name: &'static str,
        interval: &'static str,
        record_name: &'static str,
    }

    #[tokio::test]
    pub(crate) async fn ruler_rule_set_scheduled_evaluation_runs_only_owned_due_groups() {
        let mut rules = BTreeMap::new();
        for case in [
            ScheduledGroupCase {
                namespace: "team-a",
                group_name: "new",
                interval: "30s",
                record_name: "job:up:new",
            },
            ScheduledGroupCase {
                namespace: "team-a",
                group_name: "not-yet",
                interval: "5m",
                record_name: "job:up:not_yet",
            },
            ScheduledGroupCase {
                namespace: "team-b",
                group_name: "due",
                interval: "1m",
                record_name: "job:up:due",
            },
            ScheduledGroupCase {
                namespace: "team-c",
                group_name: "also-due",
                interval: "30s",
                record_name: "job:up:also_due",
            },
        ] {
            let group: serde_yaml::Value = serde_yaml::from_str(&format!(
                r"
name: {group_name}
interval: {interval}
rules:
  - record: {record_name}
    expr: up
",
                group_name = case.group_name,
                interval = case.interval,
                record_name = case.record_name,
            ))
            .expect("recording group yaml");
            rules
                .entry(case.namespace.to_string())
                .or_insert_with(BTreeMap::new)
                .insert(case.group_name.to_string(), group);
        }
        let mut group_state = staggered_group_state();
        let shard = super::super::super::RulerShard::new(1, 2).expect("ruler shard");
        let expected = super::super::super::filter_ruler_rule_set_for_shard_due_for_eval(
            "tenant-a",
            &rules,
            &group_state,
            shard,
            180_000,
        );
        let expected_groups = expected
            .values()
            .flat_map(|groups| groups.keys().cloned())
            .collect::<BTreeSet<_>>();

        let engine = up_api_engine(&[180_000]);
        let fixture = recording_sinks_and_alert_state();
        let sinks = fixture.sinks;
        let mut alert_state = fixture.alert_state;

        let evaluation =
        super::super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
            &engine,
            super::super::super::RulerSinks::new(&sinks.wal, &sinks.alertmanager, &sinks.ruler_state),
            &mut alert_state,
            &tenant_id("tenant-a"),
            &rules,
            super::super::super::RulerEvaluationSchedule::new(&mut group_state, shard, 180_000),
        )
        .await
        .expect("scheduled rule-set evaluation")
        .evaluation;

        assert2::assert!(evaluation.recording_records == expected_groups.len());
        assert2::assert!(
            sinks
                .ruler_state
                .group_records()
                .iter()
                .map(|record| record.group.clone())
                .collect::<BTreeSet<_>>()
                == expected_groups
        );
        for record in sinks.ruler_state.group_records() {
            assert2::assert!(
                group_state.last_eval_ms(&record.tenant, &record.namespace, &record.group)
                    == Some(record.last_eval_ms)
            );
        }
        assert2::assert!(sinks.wal.records().len() == expected_groups.len());
    }

    #[tokio::test]
    async fn replayed_group_state_prevents_duplicate_output_after_replica_failover() {
        let group: serde_yaml::Value = serde_yaml::from_str(
            r"
name: availability
interval: 1m
rules:
  - record: job:up
    expr: up
",
        )
        .expect("recording group yaml");
        let mut availability_group = BTreeMap::new();
        availability_group.insert("availability".to_string(), group);
        let mut rules = BTreeMap::new();
        rules.insert("team-a".to_string(), availability_group);
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels("up", "api"), 120_000, 1.0);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let tenant = tenant_id("tenant-a");
        let shard = super::super::super::RulerShard::new(1, 1).expect("ruler shard");

        let first_wal = RecordingSink::default();
        let first_state_sink = RecordingRulerStateSink::default();
        let mut first_group_state = super::super::super::RulerGroupState::default();
        super::super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
        &engine,
        super::super::super::RulerSinks::new(
            &first_wal,
            &RecordingAlertmanagerSink::default(),
            &first_state_sink,
        ),
        &mut super::super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        super::super::super::RulerEvaluationSchedule::new(&mut first_group_state, shard, 120_000),
    )
    .await
    .expect("first replica evaluation");

        let mut replacement_group_state = super::super::super::RulerGroupState::default();
        replacement_group_state.apply_records(first_state_sink.group_records());
        let replacement_wal = RecordingSink::default();
        super::super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
        &engine,
        super::super::super::RulerSinks::new(
            &replacement_wal,
            &RecordingAlertmanagerSink::default(),
            &RecordingRulerStateSink::default(),
        ),
        &mut super::super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        super::super::super::RulerEvaluationSchedule::new(&mut replacement_group_state, shard, 120_000),
    )
    .await
    .expect("replacement replica evaluation");

        assert2::assert!(first_wal.records().len() == 1);
        assert2::assert!(replacement_wal.records().is_empty());
    }
}

mod ruler_group_state_replays_compacted_last_eval_records {
    #[derive(bon::Builder)]
    struct RulerEvalExpectation {
        tenant: &'static str,
        namespace: &'static str,
        group: &'static str,
        expected: Option<i64>,
    }

    #[test]
    pub(crate) fn ruler_group_state_replays_compacted_last_eval_records() {
        let mut state = super::super::super::RulerGroupState::default();
        state.apply_records(vec![
            super::super::super::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "availability".to_string(),
                last_eval_ms: 60_000,
            },
            super::super::super::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-b".to_string(),
                group: "latency".to_string(),
                last_eval_ms: 90_000,
            },
            super::super::super::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "availability".to_string(),
                last_eval_ms: 120_000,
            },
        ]);

        for expected in [
            RulerEvalExpectation::builder()
                .tenant("tenant-a")
                .namespace("team-a")
                .group("availability")
                .expected(120_000)
                .build(),
            RulerEvalExpectation::builder()
                .tenant("tenant-a")
                .namespace("team-b")
                .group("latency")
                .expected(90_000)
                .build(),
            RulerEvalExpectation::builder()
                .tenant("tenant-b")
                .namespace("team-a")
                .group("availability")
                .build(),
        ] {
            assert2::assert!(
                state.last_eval_ms(expected.tenant, expected.namespace, expected.group)
                    == expected.expected
            );
        }
    }
}

mod ruler_rule_group_evaluation_appends_recordings_and_dispatches_firing_alerts {
    use super::*;

    #[tokio::test]
    pub(crate) async fn ruler_rule_group_evaluation_appends_recordings_and_dispatches_firing_alerts()
     {
        let group = mixed_rule_group();
        let RulerEvaluationFixture {
            engine,
            wal_sink,
            alert_sink,
            mut state,
        } = RulerEvaluationFixture::up_api_pair();

        let pending = super::super::super::evaluate_ruler_rule_group(
            &engine,
            &wal_sink,
            &alert_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &group,
            60_000,
        )
        .await
        .expect("pending group evaluation");
        super::assert_pending_rule_evaluation(&pending);

        let firing = super::super::super::evaluate_ruler_rule_group(
            &engine,
            &wal_sink,
            &alert_sink,
            &mut state,
            &tenant_id("tenant-a"),
            &group,
            360_000,
        )
        .await
        .expect("firing group evaluation");
        assert_up_rules_fired_at_six_minutes(
            &firing,
            &UpRuleSinks {
                wal_sink: &wal_sink,
                alert_sink: &alert_sink,
            },
        );
    }
}
