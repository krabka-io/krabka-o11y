use krabka_blockstore::MeteredObjectStore;
use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{
    Arc, AuditHandle, AutoOffsetReset, Cli, ClientSecurity, Consumer, MimirTenantAdminState,
    ObjectStore, PrometheusApiState, QueryFrontendOptions, RoleReadiness, ServerSecurity, Shutdown,
    TimeExt, WalHead, WalHeadConsumerRecovery, load_runtime_overrides, mimir_tenant_admin_router,
    prometheus_router, query_engine_opts, readiness_router, serve_prometheus_router_joinable,
    spawn_shutdown_signal_listener, spawn_wal_head_consumer_task,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_query_frontend",
    skip_all,
    fields(listen = %cli.listen, object_store = %cli.object_store_url, manifest_prefix = %cli.manifest_prefix),
    err
)]
pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: krabka_promql::metrics::ServiceMetrics,
    readiness: RoleReadiness,
    security: &ServerSecurity,
    wal_security: Option<ClientSecurity>,
    audit: AuditHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(store, prefix));
    let object_store_metrics = metrics.object_store.clone();
    readiness.track_object_store(object_store_metrics.clone());
    let store = MeteredObjectStore::wrap(store, object_store_metrics);
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
        let wal_head = head.clone();
        let wal_topic = cli.wal_topic.clone();
        let poll_timeout = cli.wal_poll_timeout;
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
        let client_id = cli.wal_client_id.clone();
        let subscribe_topic = cli.wal_topic.clone();
        tasks.adopt(
            "metrics query-frontend WAL head",
            spawn_wal_head_consumer_task(
                move || async move {
                    Consumer::builder()
                        .bootstrap(bootstrap)
                        .maybe_security(wal_security)
                        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
                        .frame_max(cli.client_frame_max)
                        .group_id(group_id)
                        .client_id(client_id)
                        .auto_offset_reset(AutoOffsetReset::Earliest)
                        .subscribe([subscribe_topic])
                        .build()
                        .await
                        .map_err(|error| error.to_string())
                },
                wal_head,
                wal_topic,
                poll_timeout,
                shutdown.clone(),
                wal_head_gate,
                WalHeadConsumerRecovery {
                    metrics: Some(recovery_metrics),
                    catch_up_gate: Some(readiness.gate("wal-catch-up")),
                },
            ),
        );
    }
    let metric_store = Arc::new(
        krabka_metrics_service::RefreshingMetricBlockStore::new(
            Arc::clone(&store),
            object_store_url.clone(),
            &cli.manifest_prefix,
            head.clone(),
        )
        .with_cold_cache_ttl(cli.cold_cache_ttl)
        .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback),
    );
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
