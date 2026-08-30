use super::{MetricStore, RecordingRuleWalSink, AlertmanagerSink, RulerStateSink, PromqlEngine, RulerAlertState, BTreeMap, RulerGroupEvaluation, PromqlError, evaluate_and_persist_ruler_rule_group, RulerGroupStateRecord};

/// Evaluates all ruler rule groups for one tenant and persists compactable group state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    let mut total = RulerGroupEvaluation::default();
    for (namespace, namespace_groups) in rules {
        for (group_name, group) in namespace_groups {
            let evaluation = evaluate_and_persist_ruler_rule_group(
                engine,
                (wal_sink, alert_sink, state_sink),
                alert_state,
                tenant,
                group,
                eval_time_ms,
            )
            .await?;
            state_sink
                .persist_ruler_group_state(RulerGroupStateRecord {
                    tenant: tenant.to_string(),
                    namespace: namespace.clone(),
                    group: group_name.clone(),
                    last_eval_ms: evaluation.last_eval_ms,
                })
                .await?;
            total.recording_records += evaluation.recording_records;
            total.alerts_dispatched += evaluation.alerts_dispatched;
            total.last_eval_ms = evaluation.last_eval_ms;
        }
    }
    Ok(total)
}
