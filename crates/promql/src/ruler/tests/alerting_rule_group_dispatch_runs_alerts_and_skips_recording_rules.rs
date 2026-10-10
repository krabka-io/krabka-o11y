use super::*;

#[tokio::test]
pub(crate) async fn alerting_rule_group_dispatch_runs_alerts_and_skips_recording_rules() {
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
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    let pending = super::super::evaluate_and_dispatch_alerting_rule_group(
        &engine,
        &sink,
        &mut state,
        &tenant_id("tenant-a"),
        &group,
        60_000,
    )
    .await
    .expect("pending group alert evaluation");
    assert2::assert!(pending == 0);
    assert2::assert!(sink.alerts().is_empty());

    let firing = super::super::evaluate_and_dispatch_alerting_rule_group(
        &engine,
        &sink,
        &mut state,
        &tenant_id("tenant-a"),
        &group,
        360_000,
    )
    .await
    .expect("firing group alert evaluation");
    assert2::assert!(firing == 1);
    assert2::assert!(sink.alerts() == vec![instance_up_alert(60_000)]);
}
