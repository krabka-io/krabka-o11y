use super::{
    AlertmanagerSink, BTreeMap, MetricStore, PromqlEngine, PromqlError, RecordingRuleWalSink,
    RulerAlertState, RulerEvaluationReport, RulerGroupEvaluation, RulerGroupStateRecord,
    RulerStateSink, TenantId,
    evaluate_and_persist_ruler_rule_group::evaluate_and_persist_ruler_rule_group_with_report,
};

/// Evaluates all ruler rule groups for one tenant and persists compactable group state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    Ok(evaluate_and_persist_ruler_rule_set_with_report(
        engine,
        sinks,
        alert_state,
        tenant,
        rules,
        eval_time_ms,
    )
    .await?
    .evaluation)
}

/// Evaluates every rule independently and returns detailed health information.
///
/// A bad rule or failed group-state write is recorded in the report and does
/// not prevent later rules or groups from running.
pub async fn evaluate_and_persist_ruler_rule_set_with_report<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &TenantId,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerEvaluationReport, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    let mut total = RulerEvaluationReport::default();
    for (namespace, namespace_groups) in rules {
        for (group_name, group) in namespace_groups {
            let group_report = evaluate_and_persist_ruler_rule_group_with_report(
                engine,
                (wal_sink, alert_sink, state_sink),
                alert_state,
                tenant,
                (namespace, group_name, group),
                eval_time_ms,
            )
            .await;
            let mut group_report = match group_report {
                Ok(report) => report,
                Err(error) => {
                    let error = error.to_string();
                    RulerEvaluationReport {
                        evaluation: RulerGroupEvaluation {
                            last_eval_ms: eval_time_ms,
                            ..RulerGroupEvaluation::default()
                        },
                        groups: vec![super::RulerGroupEvaluationStatus {
                            tenant: tenant.to_string(),
                            namespace: namespace.clone(),
                            group: group_name.clone(),
                            last_error: error,
                            last_evaluation_ms: eval_time_ms,
                            evaluation_time_seconds: 0.0,
                        }],
                        rules: Vec::new(),
                    }
                }
            };
            if let Err(error) = state_sink
                .persist_ruler_group_state(RulerGroupStateRecord {
                    tenant: tenant.to_string(),
                    namespace: namespace.clone(),
                    group: group_name.clone(),
                    last_eval_ms: group_report.evaluation.last_eval_ms,
                })
                .await
            {
                let error = error.to_string();
                if let Some(group) = group_report.groups.last_mut() {
                    if !group.last_error.is_empty() {
                        group.last_error.push_str("; ");
                    }
                    group.last_error.push_str(&error);
                }
            }
            total.evaluation.recording_records += group_report.evaluation.recording_records;
            total.evaluation.alerts_dispatched += group_report.evaluation.alerts_dispatched;
            total.evaluation.last_eval_ms = group_report.evaluation.last_eval_ms;
            total.rules.extend(group_report.rules);
            total.groups.extend(group_report.groups);
        }
    }
    Ok(total)
}
