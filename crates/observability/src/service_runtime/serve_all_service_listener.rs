use super::{
    Arc, CancellationToken, CriticalTaskError, ObjectStore, ServerListener, ServiceConfig,
    ServiceDependencies, ServiceRuntimeError, StagedDrain, SupervisedTasks, TcpListener,
    all_in_one_router, distributor_state_for_config, limits_provider_for_config,
    run_compactor_until_idle, run_compactor_until_shutdown, serve_router, service_audit_for_config,
    start_runtime_security, with_service_audit,
};
use crate::{DRAINING_GATE, RoleKind};

/// Serves every logs role from one process, and stops them in an order that
/// does not lose a push.
///
/// The three roles share a port and a stop. The stop is staged, because the
/// obvious alternative -- one token for all of them -- ends the block builder
/// at the same instant as the distributor, and the records the distributor
/// accepted in its last second are then left in the WAL for a restart that,
/// on a laptop, never comes:
///
/// 1. **The data port drains.** Its graceful shutdown stops accepting and lets
///    the requests already in flight finish. A push is acknowledged only after
///    its WAL append has been, so a request that got a 204 is in the WAL
///    before this step ends. The drain gate is cleared as the listener closes,
///    so `/ready` reports the drain for whatever is still probing.
/// 2. **The querier's background tasks stop.** They read; there is nothing to
///    lose.
/// 3. **The block builder empties the WAL.** Nothing is adding to it by now,
///    so [`run_compactor_until_idle`] terminates: everything the process
///    accepted is in a block before it exits.
///
/// `shutdown` is the process's stop request, which the binary cancels on
/// `SIGTERM`. It is a token rather than a signal handler so that a test, or a
/// process that embeds the stack, can ask for the same staged stop and watch
/// what it leaves behind -- which is the only way to hold the order this
/// function exists to keep.
///
/// # Errors
/// Returns an error when a role cannot start, when the HTTP server fails, or
/// when a supervised task stops while the process was not shutting down.
pub async fn serve_all_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    shutdown: CancellationToken,
) -> Result<(), ServiceRuntimeError> {
    let security = Box::pin(start_runtime_security(&config, &dependencies)).await?;
    // Cancelled on every way out of this function, so an early error does not
    // leave the audit writer waiting for a stop that never comes.
    let _audit_stop = security.audit_stop.clone().drop_guard();
    let dependencies = dependencies.with_audit(security.audit.clone());
    let readiness = dependencies.readiness.clone().unwrap_or_default();
    let metrics = dependencies.metrics.clone().unwrap_or_default();
    // One gate for the process, registered here and handed to the router, so
    // that `POST /ingester/prepare_shutdown` and the first step of this stop
    // clear the same one.
    let accepting_writes = readiness
        .for_role(RoleKind::Distributor)
        .gate(DRAINING_GATE);
    // One provider for the process: the distributor and the querier below
    // share it, so an ingest gate and a read gate answer the same tenant with
    // the same numbers.
    let overrides = limits_provider_for_config(&config)?;
    let distributor_state = distributor_state_for_config(
        &config,
        &dependencies,
        metrics.clone(),
        accepting_writes.clone(),
        Arc::clone(&overrides),
    )?;
    let querier_token = CancellationToken::new();
    let (app, background_tasks) = all_in_one_router(
        &config,
        dependencies.clone(),
        object_store,
        querier_token.clone(),
        metrics,
        readiness,
        distributor_state,
    )
    .await?;
    let app = with_service_audit(app, service_audit_for_config(&config, &dependencies));
    let mut querier_tasks = SupervisedTasks::new(querier_token);
    for (name, handle) in background_tasks {
        querier_tasks.adopt(name, handle);
    }
    // Supervised with the querier's tasks, so a writer that stops early stops
    // the process. It is cancelled on its own token, after the data port.
    if let Some(writer) = security.audit_writer {
        querier_tasks.adopt("audit writer", writer);
    }

    let mut drain = StagedDrain::new(config.all_drain_stage_timeout);
    // Named for the distributor because the distributor's stop is the one the
    // order depends on. The querier's routes ride on the same listener and
    // stop with it, which costs nothing: they hold no write.
    let data_port = serve_router(
        ServerListener::bind(listener, &security.server)?,
        app,
        &security.server,
    );
    drain.stage(RoleKind::Distributor.as_str(), move |token| async move {
        let server = data_port.with_graceful_shutdown(async move {
            token.cancelled().await;
            // Before the listener closes, not after: a probe that is mid-
            // drain should read 503 rather than a connection refused.
            accepting_writes.mark_unready();
        });
        if let Err(error) = server.await {
            tracing::error!(%error, "all-in-one data port stopped");
        }
    });

    let builder_token = CancellationToken::new();
    let builder = run_compactor_until_shutdown(
        &config,
        dependencies.clone(),
        object_store,
        builder_token.clone().cancelled_owned(),
    );
    tokio::pin!(builder);

    // Held rather than awaited twice: a future that has already returned must
    // not be polled again, and the block builder is the one branch of the
    // select below that can finish before the drain starts.
    let mut finished_early = None;
    let outcome = tokio::select! {
        result = &mut builder => {
            finished_early = Some(result);
            // Whatever it reported, the process is now one role short with its
            // port still open, which is a fault whichever way the role ended.
            Err(CriticalTaskError(RoleKind::BlockBuilder.as_str()).into())
        }
        name = drain.first_unexpected_exit() => Err(CriticalTaskError(name).into()),
        name = querier_tasks.first_unexpected_exit() => Err(CriticalTaskError(name).into()),
        () = shutdown.cancelled() => Ok(()),
    };

    // The data port first, so nothing new enters the WAL, and only then the
    // roles that read it.
    for stage in drain.drain().await {
        tracing::warn!(stage, "role did not finish draining in time");
    }
    // The data port has stopped, so no request records an event after this.
    security.audit_stop.cancel();
    querier_tasks.shutdown().await;
    let builder_outcome = if let Some(result) = finished_early {
        result
    } else {
        builder_token.cancel();
        builder.await
    };
    if let Err(error) = &builder_outcome {
        tracing::error!(%error, "block builder stopped with an error");
    }
    // Last, and only now: nothing is writing the WAL any more, so this pass
    // ends rather than running forever, and it is what puts the pushes this
    // process accepted last into a block rather than leaving them in a WAL
    // that, in a one-process stack, nothing restarts to read.
    match run_compactor_until_idle(&config, dependencies, object_store).await {
        Ok(blocks) => tracing::info!(
            blocks = blocks.len(),
            "block builder drained the WAL before exit"
        ),
        Err(error) => {
            tracing::error!(%error, "block builder could not drain the WAL before exit");
        }
    }
    outcome.and(builder_outcome.map(|_| ()))
}
