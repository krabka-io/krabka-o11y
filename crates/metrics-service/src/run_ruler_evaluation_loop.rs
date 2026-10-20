use super::{
    AlertmanagerSink, Arc, MetricStore, PrometheusApiState, RecordingRuleWalSink, RulerShard,
    RulerSinks, RulerStateSink, Time, TimeExt, current_time_ms, evaluate_ruler_once,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_ruler_evaluation_loop<S, W, A, R, Stop>(
    state: Arc<PrometheusApiState<S>>,
    sinks: RulerSinks<'_, W, A, R>,
    shard: RulerShard,
    interval: Time,
    stop: Stop,
) -> Result<(), krabka_promql::PromqlError>
where
    S: MetricStore,
    W: RecordingRuleWalSink,
    A: AlertmanagerSink,
    R: RulerStateSink,
    Stop: std::future::Future<Output = ()>,
{
    tokio::pin!(stop);
    loop {
        let eval_time_ms = current_time_ms();
        let evaluation_state = state.ruler_evaluation_state();
        let mut alert_state = evaluation_state.alert_state;
        let mut group_state = evaluation_state.group_state;
        for tenant in state.ruler_tenants() {
            if let Err(error) = evaluate_ruler_once(
                &state,
                RulerSinks::new(sinks.wal, sinks.alertmanager, sinks.ruler_state),
                &mut alert_state,
                &mut group_state,
                super::RulerEvaluationContext {
                    tenant: &tenant,
                    shard,
                    eval_time_ms,
                },
            )
            .await
            {
                tracing::error!(tenant = %tenant, %error, "ruler tenant evaluation failed");
            }
        }

        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval.to_std()) => {}
        }
    }
    Ok(())
}
