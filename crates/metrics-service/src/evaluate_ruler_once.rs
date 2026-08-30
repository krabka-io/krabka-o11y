use super::{
    AlertmanagerSink, Arc, MetricStore, PrometheusApiState, RecordingRuleWalSink, RulerAlertState,
    RulerGroupEvaluation, RulerGroupState, RulerShard, RulerStateSink,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.ruler.evaluate_once",
    skip_all,
    fields(tenant = %tenant, eval_time_ms),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn evaluate_ruler_once<S, W, A, R>(
    state: &Arc<PrometheusApiState<S>>,
    sinks: (&W, &A, &R),
    alert_state: &mut RulerAlertState,
    group_state: &mut RulerGroupState,
    tenant: &str,
    shard: RulerShard,
    eval_time_ms: i64,
) -> Result<RulerGroupEvaluation, krabka_promql::PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
{
    let (wal_sink, alert_sink, state_sink) = sinks;
    state.set_ruler_evaluation_time_ms(eval_time_ms);
    let rules = state.ruler_rule_set(tenant);
    let engine = state.engine_for_tenant(tenant);
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval(
        &engine,
        (wal_sink, alert_sink, state_sink),
        alert_state,
        tenant,
        &rules,
        (group_state, shard, eval_time_ms),
    )
    .await
}
