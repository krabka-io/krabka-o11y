use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_set_evaluation_runs_namespaced_groups() {
    let rules = namespaced_rule_set(
        r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
    for: 5m
",
    );

    let RulerEvaluationFixture {
        engine,
        wal_sink,
        alert_sink,
        mut state,
    } = RulerEvaluationFixture::up_api_pair();

    let pending = super::super::evaluate_ruler_rule_set(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rules,
        60_000,
    )
    .await
    .expect("pending rule-set evaluation");
    super::assert_pending_rule_evaluation(&pending);

    let firing = super::super::evaluate_ruler_rule_set(
        &engine,
        &wal_sink,
        &alert_sink,
        &mut state,
        &tenant_id("tenant-a"),
        &rules,
        360_000,
    )
    .await
    .expect("firing rule-set evaluation");
    assert_up_rules_fired_at_six_minutes(
        &firing,
        &UpRuleSinks {
            wal_sink: &wal_sink,
            alert_sink: &alert_sink,
        },
    );
}
