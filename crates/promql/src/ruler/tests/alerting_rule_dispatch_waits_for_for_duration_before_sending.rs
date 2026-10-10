use super::*;

#[tokio::test]
pub(crate) async fn alerting_rule_dispatch_waits_for_for_duration_before_sending() {
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r"
alert: InstanceUp
expr: up > 0
for: 5m
",
    )
    .expect("alerting rule yaml");
    let engine = up_api_engine(&[60_000, 360_000]);
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    let pending = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rule,
        60_000,
    )
    .await
    .expect("pending alert evaluation");
    assert2::assert!(pending == 0);
    assert2::assert!(sink.alerts().is_empty());

    let firing = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rule,
        360_000,
    )
    .await
    .expect("firing alert evaluation");
    assert2::assert!(firing == 1);
    assert2::assert!(sink.alerts() == vec![instance_up_alert(60_000)]);
}
