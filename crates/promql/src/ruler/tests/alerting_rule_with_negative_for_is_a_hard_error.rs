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
    let engine = up_api_engine(&[60_000]);
    let sink = RecordingAlertmanagerSink::default();
    let mut state = super::super::RulerAlertState::default();

    let result = super::super::evaluate_and_dispatch_alerting_rule_with_state(
        &engine,
        &sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rule,
        60_000,
    )
    .await;
    assert2::assert!(result.is_err());
}
