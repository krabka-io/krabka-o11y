use super::{
    AlertmanagerSink, MetricStore, PromqlEngine, PromqlError, RulerAlertState,
    evaluate_and_dispatch_alerting_rule_with_state, yaml_optional_string,
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
    tenant: &str,
    group: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<usize, PromqlError>
where
    S: MetricStore,
    A: AlertmanagerSink,
{
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "alerting rule group must contain rules".into(),
        ));
    };

    let mut dispatched = 0;
    for rule in rules {
        if yaml_optional_string(rule, "alert").is_none() {
            continue;
        }
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
