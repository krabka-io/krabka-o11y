use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_group_evaluation_appends_recordings_and_dispatches_firing_alerts() {
    let group: serde_yaml::Value = serde_yaml::from_str(
        r"
name: mixed
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
    )
    .expect("rule group yaml");
    let engine = up_api_engine(&[60_000, 360_000]);
    let wal_sink = RecordingSink::default();
    let alert_sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    let pending = super::super::evaluate_ruler_rule_group(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        &tenant_id("tenant-a"),
        &group,
        60_000,
    )
    .await
    .expect("pending group evaluation");
    assert2::assert!(
        pending
            == super::super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 0,
                last_eval_ms: 60_000,
            }
    );

    let firing = super::super::evaluate_ruler_rule_group(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        &tenant_id("tenant-a"),
        &group,
        360_000,
    )
    .await
    .expect("firing group evaluation");
    assert_up_rules_fired_at_six_minutes(
        &firing,
        &UpRuleSinks {
            wal_sink: &wal_sink,
            alert_sink: &alert_sink,
        },
    );
}
