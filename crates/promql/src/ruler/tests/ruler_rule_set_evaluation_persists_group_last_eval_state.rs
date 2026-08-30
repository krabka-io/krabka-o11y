use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_set_evaluation_persists_group_last_eval_state() {
    let recording_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: recording
rules:
  - record: job:up:current
    expr: up
",
    )
    .expect("recording group yaml");
    let alerting_group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
",
    )
    .expect("alerting group yaml");
    let mut rules = BTreeMap::new();
    rules
        .entry("team-a".to_string())
        .or_insert_with(BTreeMap::new)
        .insert("recording".to_string(), recording_group);
    rules
        .entry("team-b".to_string())
        .or_insert_with(BTreeMap::new)
        .insert("alerting".to_string(), alerting_group);

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 120_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut alert_state = super::super::RulerAlertState::default();

    let evaluation = super::super::evaluate_and_persist_ruler_rule_set(
        &engine,
        (&wal_sink, &alert_sink, &state_sink),
        &mut alert_state,
        "tenant-a",
        &rules,
        120_000,
    )
    .await
    .expect("rule-set evaluation with state persistence");

    assert2::assert!(
        evaluation
            == super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 120_000,
            }
    );
    assert2::assert!(
        state_sink.group_records()
            == vec![
                super::RulerGroupStateRecord {
                    tenant: "tenant-a".to_string(),
                    namespace: "team-a".to_string(),
                    group: "recording".to_string(),
                    last_eval_ms: 120_000,
                },
                super::RulerGroupStateRecord {
                    tenant: "tenant-a".to_string(),
                    namespace: "team-b".to_string(),
                    group: "alerting".to_string(),
                    last_eval_ms: 120_000,
                },
            ]
    );
    assert2::assert!(
        state_sink.alert_records()
            == vec![super::RulerAlertStateRecord {
                tenant: "tenant-a".to_string(),
                rule_id: "InstanceUp\nup > 0".to_string(),
                labels: BTreeMap::from([
                    ("__name__".to_string(), "up".to_string()),
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                active_since_ms: Some(120_000),
                keep_firing_until_ms: Some(120_000),
            }]
    );
}
