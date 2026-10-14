use super::*;

#[tokio::test]
pub(crate) async fn alerting_rule_state_persistence_records_active_and_cleared_alerts() {
    let rule = up_alert_rule(true);
    let engine = up_transition_engine();
    let alert_sink = RecordingAlertmanagerSink::default();
    let state_sink = RecordingRulerStateSink::default();
    let mut state = super::super::RulerAlertState::default();

    let pending = super::super::evaluate_and_persist_alerting_rule_with_state(
        &engine,
        &alert_sink,
        &state_sink,
        &mut state,
        &tenant_id("tenant-a"),
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
        &tenant_id("tenant-a"),
        &rule,
        120_000,
    )
    .await
    .expect("cleared alert state persistence");

    let mut alert_labels = BTreeMap::new();
    alert_labels.insert("alertname".to_string(), "InstanceUp".to_string());
    alert_labels.insert("job".to_string(), "api".to_string());
    check!(pending == 0);
    check!(cleared == 0);
    check!(
        state_sink.alert_records()
            == vec![
                super::super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: alert_labels.clone().into(),
                    active_since_ms: Some(60_000),
                    keep_firing_until_ms: None,
                },
                super::super::RulerAlertStateRecord {
                    tenant: "tenant-a".to_string(),
                    rule_id: "InstanceUp\nup > 0".to_string(),
                    labels: alert_labels.into(),
                    active_since_ms: None,
                    keep_firing_until_ms: None,
                },
            ]
    );
}
