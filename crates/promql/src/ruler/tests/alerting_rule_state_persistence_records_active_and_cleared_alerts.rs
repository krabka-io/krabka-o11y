use super::*;

#[tokio::test]
pub(crate) async fn alerting_rule_state_persistence_records_active_and_cleared_alerts() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 120_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut state = super::super::RulerAlertState::default();

    let pending = super::super::evaluate_and_persist_alerting_rule_with_state(
        &engine,
        &alert_sink,
        &state_sink,
        &mut state,
        "tenant-a",
        &rule,
        60_000,
    )
    .await
    .expect("pending alert state persistence");
    let cleared = super::super::evaluate_and_persist_alerting_rule_with_state(
        &engine,
        &alert_sink,
        &state_sink,
        &mut state,
        "tenant-a",
        &rule,
        120_000,
    )
    .await
    .expect("cleared alert state persistence");

    let alert_labels = BTreeMap::from([
        ("__name__".to_string(), "up".to_string()),
        ("alertname".to_string(), "InstanceUp".to_string()),
        ("job".to_string(), "api".to_string()),
    ]);
    check!(pending == 0);
    check!(cleared == 0);
    check!(
        state_sink.alert_records()
            == vec![
                super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: alert_labels.clone(),
                    active_since_ms: Some(60_000),
                    keep_firing_until_ms: None,
                },
                super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: alert_labels,
                    active_since_ms: None,
                    keep_firing_until_ms: None,
                },
            ]
    );
}
