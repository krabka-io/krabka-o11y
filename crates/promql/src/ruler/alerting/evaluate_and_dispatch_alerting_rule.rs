use super::*;

/// Evaluates one alerting rule and dispatches the active alerts to Alertmanager.
///
/// # Errors
///
/// Returns an error if the metric input is malformed, if a limit is exceeded,
/// or if the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_dispatch_alerting_rule<S, A>(
    engine: &PromqlEngine<S>,
    sink: &A,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    let mut state = RulerAlertState::default();
    evaluate_and_dispatch_alerting_rule_with_state(
        engine,
        sink,
        &mut state,
        tenant,
        rule,
        eval_time_ms,
    )
    .await
}
