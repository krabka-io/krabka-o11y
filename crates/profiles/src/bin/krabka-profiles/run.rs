use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};

use super::{
    Arc, AuditService, CancellationToken, Cli, ProcessSecurity, ServiceMetrics, Target,
    krabka_product, require_role_topics, role_shutdown_token, run_all, run_block_builder,
    run_compactor, run_distributor, run_querier, run_query_frontend, run_symbolizer,
};

pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Before anything binds or connects, so a set of security flags that
    // cannot work stops the start with an error that names the flag.
    let loaded = ProcessSecurity::load(&cli)?;
    let metrics = ServiceMetrics::new();
    // The admin port binds first, before the role reaches its object store or
    // its broker, so `/ready` there reports the rest of the start rather than
    // "a listener exists". The roles with a data port echo the same gates on
    // it.
    let readiness = RoleReadiness::new();
    let admin = krabka_telemetry::profiling::spawn_admin_with_config(
        cli.admin_listen_addr,
        krabka_profiles::metrics::metrics_router(metrics.registry.clone())
            .merge(krabka_observability::readiness_router(readiness.clone())),
        cli.profiling.clone(),
    )
    .await?;
    // The role, spelled as the shared vocabulary spells it, so that a log line
    // and a deployment manifest name the same stage.
    tracing::info!(
        role = %cli.target.kind(),
        version = env!("CARGO_PKG_VERSION"),
        "krabka-profiles starting"
    );
    // Before any role reaches a broker or an object store: until this token
    // exists no `SIGTERM` handler is installed, and a role blocked on a
    // bootstrap address that never answers would have nothing to hear the
    // signal with.
    let shutdown = role_shutdown_token();

    // The audit writer has its own token, and this function cancels it only
    // after the role has returned. A request that a listener still answers
    // during the stop can then record its event. The producer connect races
    // the shutdown, as every other broker connect of a role does.
    let audit_stop = CancellationToken::new();
    let audit = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(()),
        started = AuditService::start(
            &cli.audit,
            krabka_product("krabka-profiles", env!("CARGO_PKG_VERSION")),
            Some(&cli.bootstrap),
            loaded.wal.as_ref(),
            audit_stop.clone(),
        ) => started?,
    };
    let (audit_handle, audit_writer) = audit.into_parts();
    let mut audit_tasks = SupervisedTasks::new(audit_stop);
    if let Some(writer) = audit_writer {
        audit_tasks.adopt("profiles audit writer", writer);
    }
    // Profiles has no destructive admin operation. Its audit trail is the
    // refused credentials and the denied tenants that the listeners report.
    let security = ProcessSecurity {
        server: loaded.server.with_security_events(Arc::new(audit_handle)),
        wal: loaded.wal,
    };

    // Boxed, because the `--target all` arm alone makes the future of `run`
    // larger than Clippy's `large_futures` limit at every caller.
    let role = Box::pin(async move {
        // Before any producer or consumer exists. A WAL topic's partition
        // count is the write-path shard count, so a role that started against
        // the wrong one would re-map every key it routes and report nothing;
        // this is where it refuses instead.
        require_role_topics(&cli, security.wal.as_ref()).await?;
        match cli.target {
            Target::Distributor => {
                run_distributor(cli, metrics, readiness, shutdown, security).await?;
            }
            Target::BlockBuilder => {
                run_block_builder(cli, metrics, readiness, shutdown, security.wal).await?;
            }
            Target::Querier => run_querier(cli, metrics, readiness, shutdown, security).await?,
            Target::QueryFrontend => {
                run_query_frontend(cli, metrics, readiness, shutdown, security).await?;
            }
            Target::Compactor => run_compactor(cli, metrics, readiness, shutdown).await?,
            Target::Symbolizer => run_symbolizer(cli).await?,
            Target::All => run_all(cli, metrics, readiness, shutdown, security).await?,
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    });

    let outcome = tokio::select! {
        result = role => result,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => Ok(result?),
        name = audit_tasks.first_unexpected_exit() => {
            Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
        }
    };
    audit_tasks.shutdown().await;
    outcome
}
