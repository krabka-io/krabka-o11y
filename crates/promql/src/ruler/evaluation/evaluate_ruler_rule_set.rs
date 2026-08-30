use super::{MetricStore, RecordingRuleWalSink, AlertmanagerSink, PromqlEngine, RulerAlertState, BTreeMap, RulerGroupEvaluation, PromqlError, evaluate_ruler_rule_group};

/// Evaluates all ruler rule groups for one tenant.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_ruler_rule_set<S, W, A>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
{
    let mut total = RulerGroupEvaluation::default();
    for namespace_groups in rules.values() {
        for group in namespace_groups.values() {
            let evaluation = evaluate_ruler_rule_group(
                engine,
                wal_sink,
                alert_sink,
                alert_state,
                tenant,
                group,
                eval_time_ms,
            )
            .await?;
            total.recording_records += evaluation.recording_records;
            total.alerts_dispatched += evaluation.alerts_dispatched;
            total.last_eval_ms = evaluation.last_eval_ms;
        }
    }
    Ok(total)
}
