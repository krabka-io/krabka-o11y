use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{
    Arc, MimirTenantAdminState, QueryFrontendOptions, RoleLaunch, RoleObjectStore, ServerSecurity,
    ServingWalHead, Shutdown, TimeExt, load_runtime_overrides, mimir_tenant_admin_router,
    prometheus_router, readiness_router, serve_prometheus_router_joinable, serving_api_state,
    spawn_role_wal_head_consumer, spawn_shutdown_signal_listener,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_query_frontend",
    skip_all,
    fields(listen = %launch.cli.listen, object_store = %launch.cli.object_store_url, manifest_prefix = %launch.cli.manifest_prefix),
    err
)]
pub(crate) async fn run_query_frontend(
    launch: RoleLaunch,
    security: &ServerSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let RoleLaunch {
        cli,
        metrics,
        readiness,
        wal_security,
        audit,
    } = launch;
    let role_store = RoleObjectStore::open(&cli, &metrics, &readiness)
        .map_err(|error| -> Box<dyn std::error::Error> { error })?;
    let store = Arc::clone(&role_store.store);
    let mut wal_head = ServingWalHead::open(&cli, &metrics, &readiness);
    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());
    let mut tasks = SupervisedTasks::new(shutdown.token().clone());
    // Every frontend needs the complete recent window. A shared consumer group
    // would split partitions between replicas and make the public Service
    // return different answers depending on which pod it chose.
    let group_id = format!(
        "{}-{}-{}",
        cli.wal_group_id,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    if let Some(feed) = wal_head.take_feed(wal_security, group_id) {
        tasks.adopt(
            "metrics query-frontend WAL head",
            spawn_role_wal_head_consumer(&cli, feed, shutdown.clone()),
        );
    }
    let ServingWalHead {
        head,
        status: status_wal,
        ..
    } = wal_head;
    let metric_store = Arc::new(role_store.refreshing_metric_store(&cli, head.clone()));
    let query_cache = krabka_promql::ObjectStoreQueryFrontendCache::new(
        Arc::clone(&store),
        cli.query_frontend_cache_prefix.clone(),
    )
    .with_ttl(cli.query_frontend_cache_ttl)
    .with_execution_options(krabka_query_frontend::ExecutionOptions {
        max_parallelism: std::num::NonZeroUsize::new(cli.query_frontend_max_parallelism)
            .expect("clap rejects zero query-frontend parallelism"),
        max_retries: cli.query_frontend_max_retries,
        max_cache_freshness: cli.query_frontend_max_cache_freshness.to_std(),
    });
    {
        let mut registry = metrics.registry.lock().await;
        query_cache.metrics().register(&mut registry);
    }
    krabka_query_frontend::QueryCache::sweep(&query_cache).await?;
    let state = serving_api_state(Arc::clone(&metric_store), &cli)
        .with_erasure_store(Arc::clone(&store))
        .with_metrics(metrics)
        .with_audit(audit)
        .with_query_frontend_cache(
            QueryFrontendOptions {
                split_interval: cli.query_frontend_split,
                shard_count: cli.query_frontend_shards,
            },
            Arc::new(query_cache),
        );
    let state = if let Some((head, readiness)) = status_wal {
        state.with_wal_head_status(head, readiness)
    } else {
        state
    };
    let state = state.with_query_limits(load_runtime_overrides(cli.runtime_overrides.as_deref())?);
    let router = prometheus_router(Arc::new(state))
        .merge(mimir_tenant_admin_router(MimirTenantAdminState::new(
            store,
            metric_store,
            head,
        )))
        .merge(readiness_router(readiness));
    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, security, shutdown.signalled())
            .await?;
    tracing::info!(
        %bound,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics-service query-frontend listening"
    );
    let outcome = tokio::select! {
        result = server => result.map_err(Into::into),
        name = tasks.first_unexpected_exit() => Err(Box::<dyn std::error::Error>::from(
            CriticalTaskError(name),
        )),
    };
    tasks.shutdown().await;
    outcome
}
