use super::{
    Cli, RoleReadiness, ServiceMetrics, Target, readiness_router, require_role_topics,
    run_block_builder, run_distributor,
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
    // The role in the vocabulary every signal shares, not this binary's own
    // spelling of it. An operator reading four services' logs should see one
    // word for one stage.
    tracing::info!(role = %cli.target.kind(), "krabka-metrics starting");
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
        require_role_topics(&cli).await?;
        match cli.target {
            Target::Distributor => run_distributor(cli, metrics, readiness).await?,
            Target::BlockBuilder => run_block_builder(cli, metrics, readiness).await?,
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        result = role => result?,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => result?,
    }
    Ok(())
}
