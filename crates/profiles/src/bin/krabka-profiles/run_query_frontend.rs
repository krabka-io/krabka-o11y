use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};
use krabka_units::fmt::Human as _;

use super::{
    Arc, CancellationToken, Cli, FrontendConfig, ProcessSecurity, QuerierState, ServiceMetrics,
    build_object_store, build_profile_read_path, debuginfod_config,
    load_profiles_limits_overrides_config, serve_querier,
};

/// Answers a query by splitting its range into `--query-frontend-shard-width`
/// shards, executing them through the shared bounded fan-out and result-cache
/// pipeline, and merging what each returns.
///
/// `security` sets the TLS and authentication of `--listen`, and the TLS and
/// SASL of the WAL tail.
///
/// # Errors
/// Returns an error when the object store or the block index cannot be
/// reached, when `--listen` cannot be bound, or when a supervised task ends
/// before the role was asked to stop.
pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_gate = readiness.gate("object-store");
    let profile_index_gate = readiness.gate("profile-index");
    let overrides =
        load_profiles_limits_overrides_config(cli.profiles_limits_overrides_config.as_deref())?;
    let debuginfod = debuginfod_config(&cli)?;
    let configured = build_object_store(&cli.object_store_url, metrics.object_store.clone())
        .map_err(|e| format!("object store: {e}"))?;
    object_store_gate.mark_ready();
    let index_key = cli.index_object_key.clone();
    let read = build_profile_read_path(
        &cli,
        configured.store,
        index_key,
        debuginfod,
        profile_index_gate,
        &shutdown,
    )
    .await?;
    let Some(read) = read else { return Ok(()) };
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    for (name, handle) in read.spawn_background(&cli, &metrics, &shutdown, security.wal.as_ref()) {
        tasks.adopt(name, handle);
    }
    let state = Arc::new(
        QuerierState::new_frontend_with_overrides(
            Arc::clone(&read.union),
            FrontendConfig {
                shard_width: cli.query_frontend_shard_width,
            },
            overrides,
        )
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
    tasks.adopt("profiles query-frontend HTTP", server);
    tracing::info!(
        %bound,
        shard_width = %cli.query_frontend_shard_width.human(),
        "profiles query-frontend listening"
    );
    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => {
            Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
        }
    };
    tasks.shutdown().await;
    outcome
}
