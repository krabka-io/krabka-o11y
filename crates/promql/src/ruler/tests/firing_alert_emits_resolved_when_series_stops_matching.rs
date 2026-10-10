use super::*;

#[tokio::test]
pub(crate) async fn firing_alert_emits_resolved_when_series_stops_matching() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
",
    )
    .expect("alerting rule yaml");
    let engine = up_samples_engine(&[
        UpSample {
            job: "api",
            ts_ms: 60_000,
            value: 1.0,
        },
        UpSample {
            job: "api",
            ts_ms: 120_000,
            value: 0.0,
        },
    ]);
    let mut state = super::super::RulerAlertState::default();

    // First tick: fires immediately (no `for`).
    let firing_sink = RecordingAlertmanagerSink::default();
    let firing = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &firing_sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rule,
        60_000,
    )
    .await
    .expect("firing evaluation");
    assert2::assert!(firing == 1);
    assert2::assert!(firing_sink.alerts()[0].ends_at_ms == None);

    // Second tick: series drops; a resolved alert with EndsAt is emitted.
    let resolved_sink = RecordingAlertmanagerSink::default();
    let resolved = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &resolved_sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rule,
        120_000,
    )
    .await
    .expect("resolved evaluation");
    assert2::assert!(resolved == 1);
    assert2::assert!(
        resolved_sink.alerts()
            == vec![super::super::AlertmanagerAlert {
                labels: BTreeMap::from([
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ]),
                annotations: BTreeMap::new(),
                starts_at_ms: 60_000,
                ends_at_ms: Some(120_000),
                generator_url: String::new(),
            }]
    );
}
