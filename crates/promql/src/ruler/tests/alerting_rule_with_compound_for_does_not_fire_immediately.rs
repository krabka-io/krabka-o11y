use super::*;

#[tokio::test]
pub(crate) async fn alerting_rule_with_compound_for_does_not_fire_immediately() {
    // "1h30m" must parse to 90m; the alert may not fire until the series has
    // been active that long. The old single-unit parser coerced this to `0`
    // and fired on the first evaluation.
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 1h30m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 60_000 + 90 * 60_000, 1.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    // First evaluation: the alert becomes active now (active-since = this
    // eval time). With `for: 1h30m` it must NOT fire immediately — proving
    // the compound duration parsed as 90m rather than collapsing to 0.
    let pending = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink, &mut state, "tenant-a", &rule, 60_000,
    )
    .await
    .expect("pending evaluation");
    assert2::assert!(pending == 0);
    assert2::assert!(sink.alerts().is_empty());

    // 90 minutes later the `for: 1h30m` window is satisfied and it fires.
    let firing = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &sink,
        &mut state,
        "tenant-a",
        &rule,
        60_000 + 90 * 60_000,
    )
    .await
    .expect("firing evaluation");
    assert2::assert!(firing == 1);
}
