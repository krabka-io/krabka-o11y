use krabka_observability::{
    CancellationToken, CriticalTaskError, SupervisedTasks,
    audit::{AuditService, krabka_product},
};

use super::{
    Arc, Cli, RoleReadiness, ServiceMetrics, Target, readiness_router, require_role_topics,
    run_block_builder, run_distributor,
};

/// Starts the role `cli` selects and serves until it stops.
///
/// Separate from `main` so telemetry is installed exactly once, by `main`,
/// while the startup this returns from -- including the refusal to start on a
/// broken topic contract -- can be driven by a test.
///
/// # Errors
/// Returns an error when a security flag set cannot work, when the topic
/// contract does not hold, when the admin port cannot bind, when the audit
/// layer cannot start, or when the role itself fails.
pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    // The role in the vocabulary every signal shares, not this binary's own
    // spelling of it. An operator reading four services' logs should see one
    // word for one stage.
    tracing::info!(role = %cli.target.kind(), "krabka-metrics starting");
    // Every security flag is checked, and every file it names is read, before
    // the process binds a port or reaches a broker. With no security flag
    // set, both values are the upstream default: plain text, no credentials.
    let server_security = cli.server_security.load()?;
    let wal_security = cli.wal_security.load()?;
    let metrics = ServiceMetrics::new();
    // The admin port binds before the role reaches its broker or object
    // store, so `/ready` there is 503 for exactly as long as the role's
    // remaining startup takes. The block builder has no data port at all, and
    // this is the only place it can be asked.
    let readiness = RoleReadiness::new();
    let admin = krabka_telemetry::profiling::spawn_admin_with_config(
        cli.admin_listen_addr,
        krabka_metrics::metrics::metrics_router(metrics.registry.clone())
            .merge(readiness_router(readiness.clone())),
        cli.profiling.clone(),
    )
    .await?;

    let role = async {
        // Before any producer or consumer exists. A WAL topic's partition
        // count is the write-path shard count, so a role that started against
        // the wrong one would re-map every key it routes and report nothing;
        // this is where it refuses instead.
        require_role_topics(&cli, wal_security.clone()).await?;
        // With no `--audit-topic` this spawns nothing and reaches no broker.
        let audit_stop = CancellationToken::new();
        let (audit, audit_writer) = AuditService::start(
            &cli.audit,
            krabka_product("krabka-metrics", env!("CARGO_PKG_VERSION")),
            Some(&cli.bootstrap),
            wal_security.as_ref(),
            audit_stop.clone(),
        )
        .await?
        .into_parts();
        let mut audit_tasks = SupervisedTasks::new(audit_stop);
        if let Some(writer) = audit_writer {
            audit_tasks.adopt("audit writer", writer);
        }
        let server_security = server_security.with_security_events(Arc::new(audit));
        // Boxed, so the start-up future stays small: the role's own future
        // holds its whole serving state.
        let serve_role = Box::pin(async {
            match cli.target {
                Target::Distributor => {
                    run_distributor(cli, metrics, readiness, &server_security, wal_security).await
                }
                Target::BlockBuilder => {
                    run_block_builder(cli, metrics, readiness, wal_security).await
                }
            }
        });
        let outcome: Result<(), Box<dyn std::error::Error>> = tokio::select! {
            result = serve_role => result,
            name = audit_tasks.first_unexpected_exit() => Err(CriticalTaskError(name).into()),
        };
        // The role has stopped its listener, so no request emits an audit
        // event after the writer closes its queue.
        audit_tasks.shutdown().await;
        outcome
    };
    tokio::select! {
        result = role => result?,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => result?,
    }
    Ok(())
}
