use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_set_scheduled_evaluation_runs_only_owned_due_groups() {
    let mut rules = BTreeMap::new();
    for (namespace, group_name, interval, record_name) in [
        ("team-a", "new", "30s", "job:up:new"),
        ("team-a", "not-yet", "5m", "job:up:not_yet"),
        ("team-b", "due", "1m", "job:up:due"),
        ("team-c", "also-due", "30s", "job:up:also_due"),
    ] {
        let group: serde_yaml::Value = serde_yaml::from_str(&format!(
            r"
name: {group_name}
interval: {interval}
rules:
  - record: {record_name}
    expr: up
"
        ))
        .expect("recording group yaml");
        rules
            .entry(namespace.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(group_name.to_string(), group);
    }
    let mut group_state = super::super::RulerGroupState::default();
    group_state.apply_records(vec![
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

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 180_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut alert_state = super::super::RulerAlertState::default();

    let evaluation = super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval(
        &engine,
        (&wal_sink, &alert_sink, &state_sink),
        &mut alert_state,
        &tenant_id("tenant-a"),
        &rules,
        (&mut group_state, shard, 180_000),
    )
    .await
    .expect("scheduled rule-set evaluation");

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
    let rules = BTreeMap::from([(
        "team-a".to_string(),
        BTreeMap::from([("availability".to_string(), group)]),
    )]);
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 120_000, 1.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let tenant = tenant_id("tenant-a");
    let shard = super::super::RulerShard::new(1, 1).expect("ruler shard");

    let first_wal = RecordingSink::default();
    let first_state_sink = RecordingRulerStateSink::default();
    let mut first_group_state = super::super::RulerGroupState::default();
    super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval(
        &engine,
        (
            &first_wal,
            &RecordingAlertmanagerSink::default(),
            &first_state_sink,
        ),
        &mut super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        (&mut first_group_state, shard, 120_000),
    )
    .await
    .expect("first replica evaluation");

    let mut replacement_group_state = super::super::RulerGroupState::default();
    replacement_group_state.apply_records(first_state_sink.group_records());
    let replacement_wal = RecordingSink::default();
    super::super::evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval(
        &engine,
        (
            &replacement_wal,
            &RecordingAlertmanagerSink::default(),
            &RecordingRulerStateSink::default(),
        ),
        &mut super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        (&mut replacement_group_state, shard, 120_000),
    )
    .await
    .expect("replacement replica evaluation");

    assert2::assert!(first_wal.records().len() == 1);
    assert2::assert!(replacement_wal.records().is_empty());
}
