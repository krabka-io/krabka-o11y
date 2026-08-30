use super::*;

#[tokio::test]
pub(crate) async fn alerting_rule_with_negative_for_is_a_hard_error() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: -5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    let result = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 60_000,
    )
    .await;
    assert2::assert!(result.is_err());
}
