use super::*;

#[tokio::test]
pub(crate) async fn ruler_alert_state_replays_compacted_records_before_evaluation() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 5m
",
    )
    .expect("alerting rule yaml");
    let alert_labels = BTreeMap::from([
        ("__name__".to_string(), "up".to_string()),
        ("alertname".to_string(), "InstanceUp".to_string()),
        ("job".to_string(), "api".to_string()),
    ]);
    let mut state = super::super::RulerAlertState::default();
    state.apply_record(super::super::RulerAlertStateRecord {
        tenant: "tenant-a".to_string(),
        rule_id: "InstanceUp\nup > 0".to_string(),
        labels: alert_labels.clone(),
        active_since_ms: Some(60_000),
        keep_firing_until_ms: None,
    });

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 360_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();

    let firing = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 360_000,
    )
    .await
    .expect("replayed alert state evaluation");

    assert2::assert!(firing == 1);
    assert2::assert!(sink.alerts()[0].starts_at_ms == 60_000);

    state.apply_record(super::super::RulerAlertStateRecord {
        tenant: "tenant-a".to_string(),
        rule_id: "InstanceUp\nup > 0".to_string(),
        labels: alert_labels,
        active_since_ms: None,
        keep_firing_until_ms: None,
    });
    let sink = RecordingAlertmanagerSink::default();
    let pending = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 360_000,
    )
    .await
    .expect("tombstoned alert state evaluation");

    assert2::assert!(pending == 0);
    assert2::assert!(sink.alerts().is_empty());
}
