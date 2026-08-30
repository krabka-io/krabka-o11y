use super::{MetricStore, RecordingRuleWalSink, AlertmanagerSink, RulerStateSink, PromqlEngine, RulerAlertState, RulerGroupEvaluation, PromqlError, evaluate_and_append_recording_rule_group, evaluate_and_persist_alerting_rule_group};

/// Evaluates one mixed ruler rule group and persists alert state records.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_group<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    let recording_records =
        evaluate_and_append_recording_rule_group(engine, wal_sink, tenant, group, eval_time_ms)
            .await?;
    let alerts_dispatched = evaluate_and_persist_alerting_rule_group(
        engine,
        alert_sink,
        state_sink,
        alert_state,
        tenant,
        group,
        eval_time_ms,
    )
    .await?;
    Ok(RulerGroupEvaluation {
        recording_records,
        alerts_dispatched,
        last_eval_ms: eval_time_ms,
    })
}
