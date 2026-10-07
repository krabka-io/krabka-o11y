use krabka_observability::{CancellationToken, CriticalTaskError, SupervisedTasks};
use krabka_units::convert::TimeExt as _;

use super::{
    Arc, ClientFrameMax, ClientSecurity, ConnectionDispatchQueueCapacity, Limits,
    MetricsCompactorConfig, OverridesProvider, RoleReadiness, ServiceMetrics, WriterConfig,
    build_object_store, load_runtime_overrides, run_compactor_consumer_loop,
    spawn_retention_sweeper,
};

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
