use super::{
    AlertmanagerSink, Arc, MetricStore, PrometheusApiState, RecordingRuleWalSink, RulerAlertState,
    RulerGroupState, RulerShard, RulerStateSink, Time, TimeExt, current_time_ms,
    evaluate_ruler_once,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_ruler_evaluation_loop<S, W, A, R, Stop>(
    state: Arc<PrometheusApiState<S>>,
    sinks: (W, A, R),
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
    let (wal_sink, alert_sink, state_sink) = sinks;
    let mut alert_state = RulerAlertState::default();
    let mut group_state = RulerGroupState::default();
    tokio::pin!(stop);
    loop {
        let eval_time_ms = current_time_ms();
        for tenant in state.ruler_tenants() {
            if let Err(error) = evaluate_ruler_once(
                &state,
                (&wal_sink, &alert_sink, &state_sink),
                &mut alert_state,
                &mut group_state,
                &tenant,
                shard,
                eval_time_ms,
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
