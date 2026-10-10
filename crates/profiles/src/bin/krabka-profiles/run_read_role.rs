use krabka_observability::{RoleReadiness, SupervisedTasks};
use krabka_units::fmt::Human as _;

use super::{
    Arc, CancellationToken, Cli, FrontendConfig, ProcessSecurity, QuerierState, ServiceMetrics,
    build_object_store, build_profile_read_path, debuginfod_config,
    load_profiles_limits_overrides_config, serve_querier, supervise_until_shutdown,
};

/// The two roles that answer queries from the same read path.
#[derive(Clone, Copy)]
pub(crate) enum ReadRole {
    /// Executes each query directly.
    Querier,
    /// Splits each query's range into `--query-frontend-shard-width` shards.
    QueryFrontend,
}

/// What a read role runs with.
pub(crate) struct ReadRoleInputs {
    pub(crate) cli: Cli,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) shutdown: CancellationToken,
    /// Sets the TLS and authentication of `--listen`, and the TLS and SASL of
    /// the WAL tail.
    pub(crate) security: ProcessSecurity,
}

/// Answers a query from the WAL tail this role keeps and the blocks its index
/// names, executing it as `role` does.
///
/// # Errors
/// Returns an error when the object store or the block index cannot be
/// reached, when `--listen` cannot be bound, or when a supervised task ends
/// before the role was asked to stop.
pub(crate) async fn run_read_role(
    role: ReadRole,
    inputs: ReadRoleInputs,
) -> Result<(), Box<dyn std::error::Error>> {
    let ReadRoleInputs {
        cli,
        metrics,
        readiness,
        shutdown,
        security,
    } = inputs;
    let object_store_gate = readiness.gate("object-store");
    let profile_index_gate = readiness.gate("profile-index");
    let overrides =
        load_profiles_limits_overrides_config(cli.profiles_limits_overrides_config.as_deref())?;
    let debuginfod = debuginfod_config(&cli)?;
    let configured = build_object_store(&cli.object_store_url, metrics.object_store.clone())
        .await
        .map_err(|e| format!("object store: {e}"))?;
    object_store_gate.mark_ready();
    let index_key = cli.index_object_key.clone();
    let read = build_profile_read_path(
        &cli,
        Arc::clone(&configured.store),
        index_key,
        debuginfod,
        profile_index_gate,
        &shutdown,
    )
    .await?;
    let Some(read) = read else { return Ok(()) };
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    for (name, handle) in read.spawn_background(
        &cli,
        &metrics,
        &shutdown,
        security.wal.as_ref(),
        readiness.gate("wal-catch-up"),
    ) {
        tasks.adopt(name, handle);
    }
    let state = match role {
        ReadRole::Querier => QuerierState::new_with_overrides(Arc::clone(&read.union), overrides),
        ReadRole::QueryFrontend => QuerierState::new_frontend_with_overrides(
            Arc::clone(&read.union),
            FrontendConfig {
                shard_width: cli.query_frontend_shard_width,
            },
            overrides,
        ),
    };
    let state = Arc::new(
        state
            .with_admin_store(configured.store)
            .with_query_architecture(cli.query_architecture)
            .with_async_queries_enabled(cli.async_queries_enabled)
            .with_query_analysis_series_enabled(cli.query_analysis_series_enabled)
            .with_heatmap_policy(cli.heatmap_value_buckets, cli.heatmap_time_buckets_max)
            .with_metrics(metrics.clone()),
    );
    let (bound, server) = serve_querier(
        cli.listen,
        state,
        readiness,
        &security.server,
        shutdown.clone(),
    )
    .await?;
    match role {
        ReadRole::Querier => {
            tasks.adopt("profiles querier HTTP", server);
            tracing::info!(%bound, "profiles querier listening");
        }
        ReadRole::QueryFrontend => {
            tasks.adopt("profiles query-frontend HTTP", server);
            tracing::info!(
                %bound,
                shard_width = %cli.query_frontend_shard_width.human(),
                "profiles query-frontend listening"
            );
        }
    }
    supervise_until_shutdown(tasks, &shutdown).await
}
