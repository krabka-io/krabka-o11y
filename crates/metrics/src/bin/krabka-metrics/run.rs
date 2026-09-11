use super::{
    Cli, RoleReadiness, ServiceMetrics, Target, readiness_router, require_role_topics,
    run_compactor, run_distributor, run_querier, run_query_frontend, run_ruler,
};

/// Starts the role `cli` selects and serves until it stops.
///
/// Separate from `main` so telemetry is installed exactly once, by `main`,
/// while the startup this returns from -- including the refusal to start on a
/// broken topic contract -- can be driven by a test.
///
/// # Errors
/// Returns an error when the topic contract does not hold, when the admin port
/// cannot bind, or when the role itself fails.
pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = ServiceMetrics::new();
    // The admin port binds before the role reaches its broker or object
    // store, so `/ready` there is 503 for exactly as long as the role's
    // remaining startup takes. The compactor has no data port at all, and
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
        require_role_topics(&cli).await?;
        match cli.target {
            Target::Distributor => run_distributor(cli, metrics, readiness).await?,
            Target::Compactor => run_compactor(cli, metrics, readiness).await?,
            Target::Querier => run_querier(cli, readiness).await?,
            Target::QueryFrontend => run_query_frontend(cli, readiness).await?,
            Target::Ruler => run_ruler(cli, readiness).await?,
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        result = role => result?,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => result?,
    }
    Ok(())
}
