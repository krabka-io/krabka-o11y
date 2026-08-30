use super::{
    AlertmanagerSink, MetricStore, NoopRulerStateSink, PromqlEngine, PromqlError, RulerAlertState,
    evaluate_alerting_rule_with_state_and_sink,
};

/// Evaluates one alerting rule, tracks the pending state, and dispatches only the firing alerts.
///
/// # Errors
///
/// Returns an error if the metric input is malformed, if a limit is exceeded,
/// or if the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_dispatch_alerting_rule_with_state<S, A>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state: &mut RulerAlertState,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    evaluate_alerting_rule_with_state_and_sink(
        engine,
        sink,
        &NoopRulerStateSink,
        state,
        tenant,
        rule,
        eval_time_ms,
    )
    .await
}
