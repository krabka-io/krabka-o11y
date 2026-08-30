use super::{MetricStore, RecordingRuleWalSink, AlertmanagerSink, PromqlEngine, RulerAlertState, RulerGroupEvaluation, PromqlError, evaluate_and_append_recording_rule_group, evaluate_and_dispatch_alerting_rule_group};

/// Evaluates one mixed ruler rule group: recording outputs, then alert dispatch.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_ruler_rule_group<S, W, A>(
    engine: &PromqlEngine<S>,
    wal_sink: &W,
    alert_sink: &A,
    alert_state: &mut RulerAlertState,
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
{
    let recording_records =
        evaluate_and_append_recording_rule_group(engine, wal_sink, tenant, group, eval_time_ms)
            .await?;
    let alerts_dispatched = evaluate_and_dispatch_alerting_rule_group(
        engine,
        alert_sink,
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
