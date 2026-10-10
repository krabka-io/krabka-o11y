use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{
    Arc, MimirTenantAdminState, PrometheusApiState, QueryFrontendOptions, RoleLaunch,
    RoleObjectStore, ServerSecurity, Shutdown, TimeExt, WalHead, WalHeadConsumerRecovery,
    WalHeadFeed, load_runtime_overrides, mimir_tenant_admin_router, prometheus_router,
    query_engine_opts, readiness_router, serve_prometheus_router_joinable,
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
    let head = WalHead::with_retention(cli.wal_head_retention);
    let recovery_metrics = metrics.wal_consumer.clone();
    readiness.track_wal_consumer(recovery_metrics.clone());
    let status_wal = cli
        .wal_bootstrap
        .as_ref()
        .map(|_| (head.clone(), readiness.gate("wal-head")));
    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());
    let mut tasks = SupervisedTasks::new(shutdown.token().clone());
    if let Some(bootstrap) = cli.wal_bootstrap.clone() {
        let wal_head_gate = status_wal
            .as_ref()
            .expect("configured WAL bootstrap registers a readiness gate")
            .1
            .clone();
        // Every frontend needs the complete recent window. A shared consumer
        // group would split partitions between replicas and make the public
        // Service return different answers depending on which pod it chose.
        let group_id = format!(
            "{}-{}-{}",
            cli.wal_group_id,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        tasks.adopt(
            "metrics query-frontend WAL head",
            spawn_role_wal_head_consumer(
                &cli,
                WalHeadFeed {
                    bootstrap,
                    security: wal_security,
                    group_id,
                    head: head.clone(),
                    gate: wal_head_gate,
                    recovery: WalHeadConsumerRecovery::for_serving_role(
                        recovery_metrics,
                        &readiness,
                    ),
                },
                shutdown.clone(),
            ),
        );
    }
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
    let state = PrometheusApiState::new(Arc::clone(&metric_store), query_engine_opts(&cli))
        .with_erasure_store(Arc::clone(&store))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_query_timeout(cli.query_timeout)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_runtime_status(
            krabka_observability::LogLevelControl::process().level(),
            None,
        )
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
