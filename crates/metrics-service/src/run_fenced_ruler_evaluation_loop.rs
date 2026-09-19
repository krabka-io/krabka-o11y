use std::sync::Arc;

use krabka_client_coordination::{
    BrokerTransport, CoordinationTransport, LeaseConfig, MemberId, Role,
};
use krabka_client_producer::{Producer, ProducerError};
use krabka_units::{Time, convert::TimeExt as _};

use super::{
    AlertmanagerSink, BufferedRulerOutputs, KafkaRecordingRuleWalSink, KafkaRulerStateSink,
    MetricStore, PrometheusApiState, PrometheusRulerStateSink, RecordingRuleWalSink,
    RulerOutputBatch, RulerShard, RulerStateSink, RulerWalError, advance_ruler_lease,
    current_time_ms, evaluate_ruler_once_deferred,
};

struct CommittedPass {
    batch: RulerOutputBatch,
    reports: Vec<krabka_promql::RulerEvaluationReport>,
    eval_time_ms: i64,
}

/// Runs ruler evaluations only while this replica owns the shard's durable,
/// broker-fenced epoch.
///
/// # Errors
/// Returns an output error only when the loop cannot continue safely.
#[allow(clippy::too_many_arguments)]
pub async fn run_fenced_ruler_evaluation_loop<S, A, Stop>(
    state: Arc<PrometheusApiState<S>>,
    alert_sink: A,
    transport: Arc<BrokerTransport>,
    role: Role,
    member: MemberId,
    shard: RulerShard,
    topics: (String, String),
    eval_interval: Time,
    lease_config: LeaseConfig,
    metrics: krabka_promql::metrics::ServiceMetrics,
    stop: Stop,
) -> Result<(), RulerWalError>
where
    S: MetricStore + 'static,
    A: AlertmanagerSink,
    Stop: std::future::Future<Output = ()>,
{
    let mut held_token = None;
    let mut next_eval_ms = current_time_ms();
    let mut failover_started_ms = next_eval_ms;
    tokio::pin!(stop);

    loop {
        let now_ms = current_time_ms();
        let lease = advance_ruler_lease(
            transport.as_ref(),
            &role,
            &member,
            held_token,
            lease_config,
            now_ms,
        )
        .await;
        let wake_at_ms = match lease {
            Ok(super::RulerLeaseState::Active {
                token,
                acquired,
                renew_at_ms,
            }) => {
                held_token = Some(token);
                metrics.ruler_owner.set(1);
                metrics.ruler_producer_id.set(token.producer_id());
                metrics
                    .ruler_producer_epoch
                    .set(i64::from(token.producer_epoch()));
                if acquired {
                    metrics.ruler_failover_duration_seconds.set(
                        Time::from_millis(now_ms.saturating_sub(failover_started_ms)).secs_f64(),
                    );
                    tracing::info!(
                        %role,
                        %member,
                        %token,
                        failover_duration_ms = now_ms.saturating_sub(failover_started_ms),
                        "ruler shard ownership acquired"
                    );
                }
                if now_ms >= next_eval_ms {
                    if let Some(producer) = transport.bound_producer(&role).await {
                        match evaluate_and_commit(
                            &state,
                            &alert_sink,
                            producer,
                            shard,
                            &topics,
                            now_ms,
                        )
                        .await
                        {
                            Ok(pass) => publish_committed(&state, &alert_sink, pass).await,
                            Err(error) => {
                                tracing::error!(%role, %token, %error, "fenced ruler pass did not commit");
                                if transport.describe(&role).await.ok().flatten() != Some(token) {
                                    held_token = None;
                                    metrics.ruler_owner.set(0);
                                    failover_started_ms = current_time_ms();
                                }
                            }
                        }
                    }
                    next_eval_ms = now_ms.saturating_add(eval_interval.millis_i64());
                }
                renew_at_ms.min(next_eval_ms)
            }
            Ok(super::RulerLeaseState::Standby { wake_at_ms }) => {
                if held_token.take().is_some() {
                    metrics.ruler_owner.set(0);
                    failover_started_ms = now_ms;
                    tracing::warn!(%role, %member, "ruler shard ownership lost");
                }
                wake_at_ms
            }
            Err(error) => {
                metrics.ruler_lease_renew_failures.inc();
                if error.is_fenced() {
                    held_token = None;
                    metrics.ruler_owner.set(0);
                    failover_started_ms = now_ms;
                }
                tracing::warn!(%role, %member, %error, "ruler lease advance failed");
                now_ms.saturating_add(1_000)
            }
        };

        // Poll at least once per second so a newly written lease wakes a
        // standby without waiting for an old deadline.
        let delay_ms = wake_at_ms.saturating_sub(current_time_ms()).clamp(1, 1_000);
        tokio::select! {
            () = &mut stop => return Ok(()),
            () = tokio::time::sleep(std::time::Duration::from_millis(
                u64::try_from(delay_ms).unwrap_or(1),
            )) => {}
        }
    }
}

async fn evaluate_and_commit<S: MetricStore, A: AlertmanagerSink>(
    state: &Arc<PrometheusApiState<S>>,
    alert_sink: &A,
    producer: Arc<Producer>,
    shard: RulerShard,
    topics: &(String, String),
    eval_time_ms: i64,
) -> Result<CommittedPass, RulerWalError> {
    let (mut alert_state, mut group_state) = state.ruler_evaluation_state();
    let mut batch = RulerOutputBatch::default();
    let mut reports = Vec::new();
    for tenant in state.ruler_tenants() {
        let saved_alerts = alert_state.clone();
        let saved_groups = group_state.clone();
        let outputs = BufferedRulerOutputs::new(alert_sink);
        match evaluate_ruler_once_deferred(
            state,
            (&outputs, &outputs, &outputs),
            &mut alert_state,
            &mut group_state,
            &tenant,
            shard,
            eval_time_ms,
        )
        .await
        {
            Ok(report) => {
                batch.append(outputs.into_batch());
                reports.push(report);
            }
            Err(error) => {
                alert_state = saved_alerts;
                group_state = saved_groups;
                tracing::error!(tenant = %tenant, %error, "ruler tenant evaluation failed");
            }
        }
    }
    let transaction = producer
        .begin_transaction()
        .await
        .map_err(|error| transaction_error(&error))?;
    let wal = KafkaRecordingRuleWalSink::new(Arc::clone(&producer), topics.0.clone());
    let ruler_state = KafkaRulerStateSink::new(Arc::clone(&producer), topics.1.clone());
    let writes = async {
        for record in &batch.wal {
            wal.append_recording_rule_record(record.clone()).await?;
        }
        for record in &batch.groups {
            ruler_state
                .persist_ruler_group_state(record.clone())
                .await?;
        }
        for record in &batch.alert_states {
            ruler_state
                .persist_ruler_alert_state(record.clone())
                .await?;
        }
        Ok::<(), RulerWalError>(())
    }
    .await;
    if let Err(error) = writes {
        if let Err(abort) = transaction.abort().await {
            tracing::warn!(error = %abort.source, "failed ruler transaction did not abort cleanly");
        }
        return Err(error);
    }
    transaction
        .commit()
        .await
        .map_err(|error| transaction_error(&error.source))?;
    Ok(CommittedPass {
        batch,
        reports,
        eval_time_ms,
    })
}

async fn publish_committed<S: MetricStore + 'static, A: AlertmanagerSink>(
    state: &Arc<PrometheusApiState<S>>,
    alert_sink: &A,
    pass: CommittedPass,
) {
    state.set_ruler_evaluation_time_ms(pass.eval_time_ms);
    let local = PrometheusRulerStateSink::new(Arc::clone(state));
    for record in pass.batch.groups {
        if let Err(error) = local.persist_ruler_group_state(record).await {
            tracing::error!(%error, "committed ruler group state did not publish locally");
        }
    }
    for record in pass.batch.alert_states {
        if let Err(error) = local.persist_ruler_alert_state(record).await {
            tracing::error!(%error, "committed ruler alert state did not publish locally");
        }
    }
    for report in &pass.reports {
        state.apply_ruler_evaluation_report(report);
    }
    for (tenant, alerts) in pass.batch.alerts {
        let result = match tenant {
            Some(tenant) => alert_sink.dispatch_alerts_for_tenant(&tenant, alerts).await,
            None => alert_sink.dispatch_alerts(alerts).await,
        };
        if let Err(error) = result {
            tracing::error!(%error, "committed ruler alerts did not enqueue for delivery");
        }
    }
}

fn transaction_error(error: &ProducerError) -> RulerWalError {
    RulerWalError::Append(format!("ruler transaction: {error}"))
}
