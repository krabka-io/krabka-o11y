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
    let shard = super::super::RulerShard::new(1, 2).expect("ruler shard");
    let expected = super::super::filter_ruler_rule_set_for_shard_due_for_eval(
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
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut alert_state = super::super::RulerAlertState::default();

    let evaluation =
        super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
            &engine,
            super::super::RulerSinks::new(&wal_sink, &alert_sink, &state_sink),
            &mut alert_state,
            &tenant_id("tenant-a"),
            &rules,
            super::super::RulerEvaluationSchedule::new(&mut group_state, shard, 180_000),
        )
        .await
        .expect("scheduled rule-set evaluation")
        .evaluation;

    assert2::assert!(evaluation.recording_records == expected_groups.len());
    assert2::assert!(
        state_sink
            .group_records()
            .iter()
            .map(|record| record.group.clone())
            .collect::<BTreeSet<_>>()
            == expected_groups
    );
    for record in state_sink.group_records() {
        assert2::assert!(
            group_state.last_eval_ms(&record.tenant, &record.namespace, &record.group)
                == Some(record.last_eval_ms)
        );
    }
    assert2::assert!(wal_sink.records().len() == expected_groups.len());
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
    let shard = super::super::RulerShard::new(1, 1).expect("ruler shard");

    let first_wal = RecordingSink::default();
    let first_state_sink = RecordingRulerStateSink::default();
    let mut first_group_state = super::super::RulerGroupState::default();
    super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
        &engine,
        super::super::RulerSinks::new(
            &first_wal,
            &RecordingAlertmanagerSink::default(),
            &first_state_sink,
        ),
        &mut super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        super::super::RulerEvaluationSchedule::new(&mut first_group_state, shard, 120_000),
    )
    .await
    .expect("first replica evaluation");

    let mut replacement_group_state = super::super::RulerGroupState::default();
    replacement_group_state.apply_records(first_state_sink.group_records());
    let replacement_wal = RecordingSink::default();
    super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
        &engine,
        super::super::RulerSinks::new(
            &replacement_wal,
            &RecordingAlertmanagerSink::default(),
            &RecordingRulerStateSink::default(),
        ),
        &mut super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        super::super::RulerEvaluationSchedule::new(&mut replacement_group_state, shard, 120_000),
    )
    .await
    .expect("replacement replica evaluation");

    assert2::assert!(first_wal.records().len() == 1);
    assert2::assert!(replacement_wal.records().is_empty());
}
