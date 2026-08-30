use super::{MetricStore, AlertmanagerSink, RulerStateSink, PromqlEngine, RulerAlertState, PromqlError, evaluate_alerting_rule_with_state_and_sink};

/// Evaluates one alerting rule, persists the alert state, and dispatches only the firing alerts.
///
/// # Errors
///
/// Returns an error if the metric input is malformed, if a limit is exceeded,
/// or if the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_alerting_rule_with_state<S, A, R>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state_sink: &R,
    state: &mut RulerAlertState,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    evaluate_alerting_rule_with_state_and_sink(
        engine,
        sink,
        state_sink,
        state,
        tenant,
        rule,
        eval_time_ms,
    )
    .await
}
