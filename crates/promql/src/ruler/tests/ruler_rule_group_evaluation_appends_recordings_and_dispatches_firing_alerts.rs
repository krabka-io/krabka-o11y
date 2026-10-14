use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_group_evaluation_appends_recordings_and_dispatches_firing_alerts() {
    let group = mixed_rule_group();
    let RulerEvaluationFixture {
        engine,
        wal_sink,
        alert_sink,
        mut state,
    } = RulerEvaluationFixture::up_api_pair();

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
    super::assert_pending_rule_evaluation(&pending);

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
