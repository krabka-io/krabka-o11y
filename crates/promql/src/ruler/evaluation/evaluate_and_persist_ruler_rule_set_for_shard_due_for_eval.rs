use super::*;

/// Evaluates this shard's due ruler rule groups for one tenant and persists state.
///
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval<S, W, A, R>(
    engine: &PromqlEngine<S>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    schedule: (&mut RulerGroupState, RulerShard, i64),
) -> Result<RulerGroupEvaluation, PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    let (group_state, shard, eval_time_ms) = schedule;
    let scheduled = filter_ruler_rule_set_for_shard_due_for_eval(
        tenant,
        rules,
        group_state,
        shard,
        eval_time_ms,
    );
    let evaluation = evaluate_and_persist_ruler_rule_set(
        engine,
        (wal_sink, alert_sink, state_sink),
        alert_state,
        tenant,
        &scheduled,
        eval_time_ms,
    )
    .await?;
    for (namespace, namespace_groups) in &scheduled {
        for group_name in namespace_groups.keys() {
            group_state.apply_record(RulerGroupStateRecord {
                tenant: tenant.to_string(),
                namespace: namespace.clone(),
                group: group_name.clone(),
                last_eval_ms: eval_time_ms,
            });
        }
    }
    Ok(evaluation)
}
