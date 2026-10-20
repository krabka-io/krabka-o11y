use super::{
    AlertmanagerSink, Arc, MetricStore, PrometheusApiState, RecordingRuleWalSink, RulerAlertState,
    RulerEvaluationSchedule, RulerGroupEvaluation, RulerGroupState, RulerShard, RulerSinks,
    RulerStateSink, TenantId,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report,
};

macro_rules! ruler_evaluation_fn {
    ($(#[$attribute:meta])* $name:ident, $output:ty,
     $state:ident, $sinks:ident, $alert_state:ident, $group_state:ident, $context:ident,
     $body:block) => {
        $(#[$attribute])*
        pub async fn $name<S, W, A, R>(
            $state: &Arc<PrometheusApiState<S>>,
            $sinks: RulerSinks<'_, W, A, R>,
            $alert_state: &mut RulerAlertState,
            $group_state: &mut RulerGroupState,
            $context: RulerEvaluationContext<'_>,
        ) -> Result<$output, krabka_promql::PromqlError>
        where
            S: MetricStore,
            W: RecordingRuleWalSink,
            A: AlertmanagerSink,
            R: RulerStateSink,
        {
            $body
        }
    };
}

#[derive(Clone, Copy)]
pub struct RulerEvaluationContext<'a> {
    pub tenant: &'a TenantId,
    pub shard: RulerShard,
    pub eval_time_ms: i64,
}

ruler_evaluation_fn!(
    #[tracing::instrument(
    level = "info",
    name = "metrics.ruler.evaluate_once",
    skip_all,
    fields(tenant = %context.tenant, eval_time_ms = context.eval_time_ms),
    err
)]
    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    evaluate_ruler_once,
    RulerGroupEvaluation,
    state,
    sinks,
    alert_state,
    group_state,
    context,
    {
        state.set_ruler_evaluation_time_ms(context.eval_time_ms);
        let report =
            evaluate_ruler_once_deferred(state, sinks, alert_state, group_state, context).await?;
        state.apply_ruler_evaluation_report(&report);
        Ok(report.evaluation)
    }
);

ruler_evaluation_fn!(
    /// Evaluates one tenant without publishing its status. A fenced ruler uses
    /// this form and publishes only after its epoch-bound Kafka transaction commits.
    ///
    /// # Errors
    /// Returns the evaluation or sink error without publishing the report.
    evaluate_ruler_once_deferred,
    krabka_promql::RulerEvaluationReport,
    state,
    sinks,
    alert_state,
    group_state,
    context,
    {
        let rules = state.ruler_rule_set(context.tenant);
        let engine = state.engine_for_tenant(context.tenant);
        let report = evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report(
            &engine,
            sinks,
            alert_state,
            context.tenant,
            &rules,
            RulerEvaluationSchedule::new(group_state, context.shard, context.eval_time_ms),
        )
        .await?;
        Ok(report)
    }
);
