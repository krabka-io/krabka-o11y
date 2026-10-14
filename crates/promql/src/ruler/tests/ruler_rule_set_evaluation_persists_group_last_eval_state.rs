use super::*;

#[tokio::test]
pub(crate) async fn ruler_rule_set_evaluation_persists_group_last_eval_state() {
    let rules = namespaced_rule_set(
        r"
name: alerting
rules:
  - alert: InstanceUp
    expr: up > 0
",
    );

    let engine = up_api_engine(&[120_000]);
    let fixture = recording_sinks_and_alert_state();
    let sinks = fixture.sinks;
    let mut alert_state = fixture.alert_state;

    let evaluation = super::super::evaluate_and_persist_ruler_rule_set(
        super::super::RulerRuleSetEvaluationRequest::new(
            &engine,
            super::super::RulerSinks::new(&sinks.wal, &sinks.alertmanager, &sinks.ruler_state),
            &mut alert_state,
            &tenant_id("tenant-a"),
            &rules,
            120_000,
        ),
    )
    .await
    .expect("rule-set evaluation with state persistence");

    assert2::assert!(
        evaluation
            == super::super::RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 1,
                last_eval_ms: 120_000,
            }
    );
    assert2::assert!(
        sinks.ruler_state.group_records()
            == vec![
                super::super::RulerGroupStateRecord {
                    tenant: "tenant-a".to_string(),
                    namespace: "team-a".to_string(),
                    group: "recording".to_string(),
                    last_eval_ms: 120_000,
                },
                super::super::RulerGroupStateRecord {
                    tenant: "tenant-a".to_string(),
                    namespace: "team-b".to_string(),
                    group: "alerting".to_string(),
                    last_eval_ms: 120_000,
                },
            ]
    );
    assert2::assert!(
        sinks.ruler_state.alert_records()
            == vec![super::super::RulerAlertStateRecord {
                tenant: "tenant-a".to_string(),
                rule_id: "InstanceUp\nup > 0".to_string(),
                labels: BTreeMap::from([
                    ("alertname".to_string(), "InstanceUp".to_string()),
                    ("job".to_string(), "api".to_string()),
                ])
                .into(),
                active_since_ms: Some(120_000),
                keep_firing_until_ms: Some(120_000),
            }]
    );
}
