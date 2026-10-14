use super::{
    AlertmanagerSink, BTreeMap, MetricStore, PromqlEngine, PromqlError, RecordingRuleWalSink,
    RulerAlertState, RulerEvaluationReport, RulerGroupEvaluation, RulerGroupRef,
    RulerGroupStateRecord, RulerSinks, RulerStateSink, TenantId,
    evaluate_and_persist_ruler_rule_group::evaluate_and_persist_ruler_rule_group_with_report,
};

pub type RulerRuleSet = BTreeMap<String, BTreeMap<String, serde_yaml::Value>>;

pub struct RulerRuleSetEvaluationRequest<'a, S: MetricStore, W, A, R> {
    pub engine: &'a PromqlEngine<S>,
    pub sinks: RulerSinks<'a, W, A, R>,
    pub alert_state: &'a mut RulerAlertState,
    pub tenant: &'a TenantId,
    pub rules: &'a RulerRuleSet,
    pub eval_time_ms: i64,
}

impl<'a, S: MetricStore, W, A, R> RulerRuleSetEvaluationRequest<'a, S, W, A, R> {
    pub fn new(
        engine: &'a PromqlEngine<S>,
        sinks: RulerSinks<'a, W, A, R>,
        alert_state: &'a mut RulerAlertState,
        tenant: &'a TenantId,
        rules: &'a RulerRuleSet,
        eval_time_ms: i64,
    ) -> Self {
        Self {
            engine,
            sinks,
            alert_state,
            tenant,
            rules,
            eval_time_ms,
        }
    }
}

/// Evaluates all ruler rule groups for one tenant and persists compactable group state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set<S, W, A, R>(
    request: RulerRuleSetEvaluationRequest<'_, S, W, A, R>,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    Ok(evaluate_and_persist_ruler_rule_set_with_report(request)
        .await?
        .evaluation)
}

/// Evaluates every rule independently and returns detailed health information.
///
/// A bad rule or failed group-state write is recorded in the report and does
/// not prevent later rules or groups from running.
///
/// # Errors
/// Returns an error when metric input is malformed or a limit is exceeded.
pub async fn evaluate_and_persist_ruler_rule_set_with_report<S, W, A, R>(
    request: RulerRuleSetEvaluationRequest<'_, S, W, A, R>,
) -> Result<RulerEvaluationReport, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let RulerRuleSetEvaluationRequest {
        engine,
        sinks,
        alert_state,
        tenant,
        rules,
        eval_time_ms,
    } = request;
    let wal_sink = sinks.wal;
    let alert_sink = sinks.alertmanager;
    let state_sink = sinks.ruler_state;
    let mut total = RulerEvaluationReport::default();
    for namespace_entry in rules {
        let namespace = namespace_entry.0;
        let namespace_groups = namespace_entry.1;
        for group_entry in namespace_groups {
            let group_name = group_entry.0;
            let group = group_entry.1;
            let group_report = evaluate_and_persist_ruler_rule_group_with_report(
                engine,
                RulerSinks::new(wal_sink, alert_sink, state_sink),
                alert_state,
                tenant,
                RulerGroupRef {
                    namespace,
                    group_name,
                    group,
                },
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
