use krabka_observability::{
    CancellationToken, CriticalTaskError, SupervisedTasks, compaction_schedule::CompactionSchedule,
};
use krabka_units::{Time, convert::TimeExt as _, fmt::Human as _};

use super::*;
use crate::{
    DeferredBlockDeletions, MetricCompactionError, MetricCompactionPass, compact_metric_blocks_once,
};

/// Runs compaction passes on `--compactor-interval` until `shutdown` fires.
///
/// Everything a pass needs is owned rather than borrowed, because the role
/// supervises this loop rather than awaiting it inline: a supervised task holds
/// its own state for as long as it runs. See
/// [`run_compactor`] for what supervision
/// buys.
///
/// The queue of retired input blocks lives here, across ticks, because the
/// blocks a pass retires are deleted by a *later* pass. See
/// [`DeferredBlockDeletions`].
// cargo-mutants: the wall-clock schedule is exercised through the pass itself.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn compactor_loop(
    store: Arc<dyn ObjectStore>,
    block_writer: BlockWriter,
    index_sink: ObjectStoreCompactionIndexSink,
    policy: CompactionPolicy,
    interval: Time,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) {
    let mut deferred = DeferredBlockDeletions::new();
    let mut schedule = CompactionSchedule::new(interval, shutdown);
    while schedule.next_pass_due().await {
        let outcome = metrics
            .compaction
            .time_run(run_compactor_once(
                &store,
                &block_writer,
                &index_sink,
                policy,
                &mut deferred,
                &metrics,
            ))
            .await;
        match outcome {
            Ok(pass) => {
                for failure in pass
                    .manifests_retired
                    .failures
                    .iter()
                    .chain(pass.blocks_deleted.failures.iter())
                {
                    tracing::warn!(
                        object = %failure.failed_key,
                        error = %failure.error,
                        "metrics compactor could not delete a retired object"
                    );
                }
                if !pass.is_empty() {
                    tracing::info!(
                        compacted_blocks = pass.outputs.len(),
                        manifests_retired = pass.manifests_retired.deleted,
                        blocks_deleted = pass.blocks_deleted.deleted,
                        deletions_deferred = deferred.keys().len(),
                        "metrics compactor finished one pass"
                    );
                }
            }
            // One failed pass is not a reason to lose the role. The next tick
            // reloads the manifests and replans from whatever is durable. The
            // counter is what makes that visible: without it a role whose every
            // pass fails exports what a role with nothing to do exports.
            Err(error) => {
                tracing::warn!(
                    %error,
                    "metrics compaction pass failed; retrying on the next tick"
                );
            }
        }
    }
}

// cargo-mutants: live block-builder I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run_block_builder(
    cli: WriterConfig,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    wal_security: Option<ClientSecurity>,
    stopping: CancellationToken,
    startup: krabka_observability::ReadinessGate,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The block builder serves no data port, so `/ready` on the admin port is
    // the only place an orchestrator can ask. It is ready once it holds the
    // two things it writes between: the WAL consumer and the object store.
    let object_store_gate = readiness.gate("object-store");
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let wal_catch_up_gate = readiness.gate("wal-catch-up");
    let store = build_object_store(&cli.object_store_url, metrics.object_store.clone()).await?;
    object_store_gate.mark_ready();
    // The same runtime overrides file the distributor reads. It holds the
    // retention window of every tenant, and without it every tenant keeps its
    // blocks forever -- the built-in window is zero, which means "keep".
    let overrides = Arc::new(
        load_runtime_overrides(cli.runtime_overrides.as_deref())?
            .unwrap_or_else(|| OverridesProvider::new(Limits::default())),
    );
    let sweep_interval = cli.block_builder_retention_sweep_interval;
    let mut config = MetricsCompactorConfig::new(cli.bootstrap);
    config.client_dispatch_queue_capacity =
        ConnectionDispatchQueueCapacity::new(cli.client_dispatch_queue_capacity)
            .expect("validated metrics client dispatch queue capacity");
    config.client_frame_max =
        ClientFrameMax::try_from(cli.client_frame_max).expect("validated metrics frame maximum");
    config.group_id = cli.block_builder_group_id;
    config.client_id = cli.block_builder_client_id;
    config.poll_timeout = cli.block_builder_poll_timeout;
    config.flush_max_rows = cli.block_builder_flush_max_rows;
    config.flush_max_age = cli.block_builder_flush_max_age;
    let runtime = config.build_runtime(store.clone(), metrics.object_store.clone())?;
    let mut consumer = config
        .build_consumer(
            &metrics.wal_consumer,
            wal_security.clone(),
            Some(wal_catch_up_gate),
        )
        .await?
        .drain_on_shutdown(stopping.clone());
    wal_consumer_gate.mark_ready();
    let mut tasks = SupervisedTasks::new(stopping.clone());
    // Always, and not only where a tenant has a retention window. Retention and
    // orphan reconciliation are two halves of one pass, and the orphan half is
    // the only thing in metrics that reclaims a block whose writer died before
    // it published the manifest. A deployment with no window configured has
    // those orphans too, and gating the sweeper on the window leaked every one
    // of them forever. A pass with no window expires nothing.
    tasks.adopt(
        "metrics block-builder retention sweeper",
        spawn_retention_sweeper(
            store,
            overrides,
            sweep_interval,
            stopping.clone(),
            metrics.clone(),
        ),
    );
    startup.mark_ready();
    let result = loop {
        let stop = stopping.clone();
        let attempt = tokio::select! {
            result = run_compactor_consumer_loop(
                &mut consumer,
                &runtime.block_writer,
                &runtime.index_sink,
                runtime.loop_config.clone(),
                move |_| stop.is_cancelled(),
                &metrics,
            ) => result,
            name = tasks.first_unexpected_exit() => {
                break Err(Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)));
            }
        };
        match attempt {
            Ok(result) => break Ok(result),
            Err(error @ crate::CompactionPollError::Poll(_)) if !stopping.is_cancelled() => {
                tracing::warn!(%error, "metrics block-builder loop failed; retrying");
                // Drop only cancels the coordinator. Await its shutdown before
                // a replacement joins the group; replay still starts at the
                // last durable committed cut, without flushing the failed poll.
                if let Err(error) = consumer.into_inner().close().await {
                    tracing::warn!(%error, "metrics block-builder consumer close failed before retry");
                }
                tokio::time::sleep(config.poll_timeout.to_std()).await;
                consumer = match config
                    .build_consumer(&metrics.wal_consumer, wal_security.clone(), None)
                    .await
                {
                    Ok(consumer) => consumer.drain_on_shutdown(stopping.clone()),
                    Err(error) => {
                        tasks.shutdown().await;
                        return Err(error.into());
                    }
                };
            }
            Err(error) => break Err(error.into()),
        }
    };
    tasks.shutdown().await;
    // The loop has drained and committed its final durable buffer. Await the
    // coordinator's shutdown before process exit can cancel its LeaveGroup attempt.
    let close = consumer.into_inner().close().await;
    let result = match result {
        Ok(result) => {
            close?;
            result
        }
        Err(error) => {
            if let Err(close_error) = close {
                tracing::warn!(%close_error, "metrics block-builder consumer close failed after a loop error");
            }
            return Err(error);
        }
    };
    // The block counter moves inside the flush, so a running block builder
    // reports what it has written rather than only what it wrote before it
    // stopped.
    tracing::info!(
        polls = result.polls,
        polled_records = result.polled_records,
        compacted_records = result.compacted_records,
        writes = result.writes,
        "metrics block builder stopped"
    );
    Ok(())
}

/// Runs compaction passes on a schedule until shutdown.
///
/// Compaction is not something an operator types a time window into: the
/// planner decides what merges from the levels and time ranges the `.index`
/// manifests already record, and a pass that plans nothing costs one listing.
///
/// The compactor reaches no broker. It reads and writes object storage only,
/// which is what separates it from the block builder, so the object store is
/// its one readiness gate. Each pass reads the manifests itself, so there is no
/// index gate to hold: a pass that cannot read one fails and the next tick
/// retries.
///
/// The schedule is [`compactor_loop`], and it is a supervised task rather than
/// the body of this function. The loop is the whole role, so a loop that
/// panicked, or that returned while the process was still up, would leave a
/// compactor that keeps its readiness gate and its `/metrics` endpoint and
/// compacts nothing. Neither probe can tell that from a compactor with nothing
/// to do. [`SupervisedTasks`] names the loop, reports either stop as a
/// [`CriticalTaskError`], and drains it on the way out.
// cargo-mutants: live compactor I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run_compactor(
    cli: WriterConfig,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    stopping: CancellationToken,
    startup: krabka_observability::ReadinessGate,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let object_store_gate = readiness.gate("object-store");
    let store = build_object_store(&cli.object_store_url, metrics.object_store.clone()).await?;
    object_store_gate.mark_ready();
    let policy = compactor_policy_from_cli(&cli);
    tracing::info!(
        interval = %cli.compactor_interval.human(),
        max_blocks_per_job = policy.max_blocks_per_job(),
        target_rows = policy.target_rows_per_block(),
        max_level = %policy.max_level(),
        level_window = %policy.level_window().human(),
        "metrics compactor scheduling passes"
    );
    let mut tasks = SupervisedTasks::new(stopping.clone());
    tasks.spawn(
        "metrics compactor pass scheduler",
        compactor_loop(
            store.clone(),
            BlockWriter::new(store.clone()).with_metrics(metrics.object_store.clone()),
            ObjectStoreCompactionIndexSink::new(store),
            policy,
            cli.compactor_interval,
            metrics,
            stopping.clone(),
        ),
    );
    startup.mark_ready();
    let outcome = tokio::select! {
        () = stopping.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    // A pass already under way finishes before this returns, so the process
    // never drops a half-written merge on the way out.
    tasks.shutdown().await;
    outcome
}

/// Runs one compaction pass and reports what it did.
///
/// The manifests are reloaded inside the pass, so nothing is carried across
/// ticks: the block builder publishes new blocks into the same prefix, and a
/// stale set would plan against blocks a previous pass replaced.
///
/// A pass that planned nothing publishes nothing. Counting its zero output
/// would leave a compactor with nothing to do exporting what a compactor that
/// merged a block exports.
///
/// # Errors
/// Returns an error when the manifests cannot be read, or when a merge cannot
/// read its inputs or write its output.
pub(crate) async fn run_compactor_once(
    store: &Arc<dyn ObjectStore>,
    block_writer: &BlockWriter,
    index_sink: &ObjectStoreCompactionIndexSink,
    policy: CompactionPolicy,
    deferred: &mut DeferredBlockDeletions,
    metrics: &ServiceMetrics,
) -> Result<MetricCompactionPass, MetricCompactionError> {
    let pass = compact_metric_blocks_once(
        store,
        block_writer,
        index_sink,
        policy,
        DEFAULT_BLOCK_READ_MAX,
        deferred,
    )
    .await?;
    metrics.compaction.record_output(pass.outputs.len() as u64);
    metrics.compaction.record_deleted(
        pass.blocks_deleted.deleted as u64,
        pass.manifests_retired.deleted as u64,
        (pass.blocks_deleted.failures.len() + pass.manifests_retired.failures.len()) as u64,
    );
    Ok(pass)
}

pub(crate) async fn run_distributor(
    cli: WriterConfig,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    security: &ServerSecurity,
    wal_security: Option<ClientSecurity>,
    stopping: CancellationToken,
    startup: krabka_observability::ReadinessGate,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A distributor with no broker behind it accepts a push and has nowhere to
    // put it. Both client builds below reach the broker, and neither has
    // happened yet.
    let wal_broker = readiness.gate("wal-broker");
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&cli.bootstrap)
            // Metrics writes enqueue many series before awaiting their acks.
            // Custom frame limits use the client's 16 KiB batch size.
            .batch_size(
                if cli.client_frame_max == krabka_client_core::DEFAULT_CLIENT_FRAME_MAX {
                    64 * 1024
                } else {
                    16 * 1024
                },
            )
            .maybe_security(wal_security.clone())
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build()
            .await?,
    );
    let mut ha_consumer = Consumer::builder()
        .bootstrap(&cli.bootstrap)
        .maybe_security(wal_security)
        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
        .frame_max(cli.client_frame_max)
        .group_id(cli.ha_tracker_group_id.clone())
        .client_id(cli.ha_tracker_client_id.clone())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ha_tracker_topic.clone()])
        .enable_auto_commit(false)
        .build()
        .await?;
    wal_broker.mark_ready();
    let mut state = DistributorState::new(Arc::new(KafkaSink::new(Arc::clone(&producer))))
        .with_ha_failover_timeout(cli.ha_failover_timeout)
        .with_max_rate_buckets(cli.ingest_rate_bucket_cap)
        .with_max_decompressed(cli.distributor_max_decompressed)
        .with_otlp_promote_resource_attributes(cli.distributor_otel_promote_resource_attributes)
        .with_ha_election_sink(Arc::new(KafkaHaElectionSink::new(
            Arc::clone(&producer),
            cli.ha_tracker_topic.clone(),
        )))
        .with_metrics(metrics);
    // Without a runtime overrides file every tenant keeps the built-in limits,
    // which is the documented default rather than a missing configuration.
    if let Some(overrides) = load_runtime_overrides(cli.runtime_overrides.as_deref())? {
        state = state.with_overrides(overrides);
    }
    let state = Arc::new(state);
    let ha_state = Arc::clone(&state);
    let ha_topic = cli.ha_tracker_topic.clone();
    let ha_poll_timeout = cli.ha_tracker_poll_timeout;
    let listener = ServerListener::bind(TcpListener::bind(cli.listen).await?, security)?;
    let ha_stop = stopping.clone();
    let mut ha_task = tokio::spawn(async move {
        let outcome = run_ha_election_consumer_loop(
            &mut ha_consumer,
            ha_state.tracker(),
            &ha_topic,
            ha_poll_timeout,
            move |_| ha_stop.is_cancelled(),
        )
        .await
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from);
        let close = ha_consumer.close().await;
        if outcome.is_err() {
            if let Err(error) = close {
                tracing::warn!(%error, "metrics HA consumer close failed after a loop error");
            }
            return outcome;
        }
        close?;
        outcome
    });
    let bound = listener.local_addr();
    startup.mark_ready();
    tracing::info!(
        %bound,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics distributor listening"
    );
    let server = std::future::IntoFuture::into_future(
        serve_router(
            listener,
            distributor_router(state).merge(readiness_router(readiness)),
            security,
        )
        .with_graceful_shutdown(stopping.clone().cancelled_owned()),
    );
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            stopping.cancel();
            let ha_outcome = ha_task.await;
            result?;
            ha_outcome??;
        }
        result = &mut ha_task => {
            let failure: Box<dyn std::error::Error + Send + Sync> = match result {
                Ok(Ok(_)) if stopping.is_cancelled() => {
                    server.await?;
                    return Ok(());
                }
                Ok(Ok(_)) => "metrics HA tracker consumer stopped unexpectedly".into(),
                Ok(Err(error)) => error,
                Err(error) => error.into(),
            };
            stopping.cancel();
            server.await?;
            return Err(failure);
        }
    }
    Ok(())
}
