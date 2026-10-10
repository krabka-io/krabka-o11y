use super::{
    AlertmanagerSink, MetricStore, PromqlEngine, PromqlError, RulerAlertState, TenantId,
    alerting_rules, evaluate_and_dispatch_alerting_rule_with_state,
};

/// Evaluates all alerting rules in one rule group and dispatches the firing alerts.
///
/// # Errors
///
/// Returns an error if the metric input is malformed, if a limit is exceeded,
/// or if the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_dispatch_alerting_rule_group<S, A>(
    engine: &PromqlEngine<S>,
    sink: &A,
    state: &mut RulerAlertState,
    tenant: &TenantId,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    let mut dispatched = 0;
    for rule in alerting_rules(group)? {
        dispatched += evaluate_and_dispatch_alerting_rule_with_state(
            engine,
            sink,
            state,
            tenant,
            rule,
            eval_time_ms,
        )
        .await?;
    }
    Ok(dispatched)
}
