use super::*;

#[tokio::test]
pub(crate) async fn keep_firing_for_holds_alert_firing_then_resolves_after_window() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
keep_firing_for: 5m
",
    )
    .expect("alerting rule yaml");
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 0, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 120_000, 0.0);
    store.push_float("tenant-a", labels("up", "api"), 600_000, 0.0);
    let store = Arc::new(store);
    let engine = PromqlEngine::new(store, EngineOpts::default());
    let mut state = super::super::RulerAlertState::default();
    let state_sink = RecordingRulerStateSink::default();

    // t=0: fires; keep-firing deadline armed at 0 + 5m = 300_000.
    let sink0 = RecordingAlertmanagerSink::default();
    let fired = super::super::evaluate_and_persist_alerting_rule_with_state(
        &engine,
        &sink0,
        &state_sink,
        &mut state,
        "tenant-a",
        &rule,
        0,
    )
    .await
    .expect("initial firing");
    assert2::assert!(fired == 1);
    assert2::assert!(sink0.alerts()[0].ends_at_ms == None);
    let records = state_sink.alert_records();
    assert2::assert!(records.len() == 1);
    assert2::assert!(records[0].keep_firing_until_ms == Some(300_000));

    // Simulate a process restart by rebuilding only from the durable record.
    state = super::super::RulerAlertState::default();
    state.apply_record(records[0].clone());

    // t=120s after restart: series gone but within keep_firing_for; still
    // firing, no EndsAt.
    let sink1 = RecordingAlertmanagerSink::default();
    let kept = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink1, &mut state, "tenant-a", &rule, 120_000,
    )
    .await
    .expect("kept firing");
    let kept_alerts = sink1.alerts();
    assert2::assert!(kept == 1);
    assert2::assert!(kept_alerts[0].ends_at_ms == None);

    // t=600s: keep-firing window (deadline 300s) elapsed; resolves with EndsAt.
    let sink2 = RecordingAlertmanagerSink::default();
    let resolved = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine, &sink2, &mut state, "tenant-a", &rule, 600_000,
    )
    .await
    .expect("resolved after window");
    let resolved_alerts = sink2.alerts();
    assert2::assert!(resolved == 1);
    assert2::assert!(resolved_alerts[0].ends_at_ms == Some(600_000));
}
