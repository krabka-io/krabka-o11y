use super::{
    Arc, AuditHandle, Cli, ObjectStore, PrometheusApiState, QueryFrontendOptions, RoleReadiness,
    ServerSecurity, Shutdown, TimeExt, WalHead, load_runtime_overrides, prometheus_router,
    query_engine_opts, readiness_router, serve_prometheus_router_joinable,
    spawn_shutdown_signal_listener,
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
    audit: AuditHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let metric_store = krabka_metrics_service::RefreshingMetricBlockStore::new(
        Arc::clone(&store),
        object_store_url.clone(),
        &cli.manifest_prefix,
        WalHead::new(),
    )
    .with_cold_cache_ttl(cli.cold_cache_ttl)
    .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
    let state = PrometheusApiState::new(Arc::new(metric_store), query_engine_opts(&cli))
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
            Arc::new(
                krabka_promql::ObjectStoreQueryFrontendCache::new(
                    store,
                    cli.query_frontend_cache_prefix.clone(),
                )
                .with_ttl(cli.query_frontend_cache_ttl)
                .with_execution_options(krabka_query_frontend::ExecutionOptions {
                    max_parallelism: std::num::NonZeroUsize::new(
                        cli.query_frontend_max_parallelism,
                    )
                    .expect("clap rejects zero query-frontend parallelism"),
                    max_retries: cli.query_frontend_max_retries,
                    max_cache_freshness: cli.query_frontend_max_cache_freshness.to_std(),
                }),
            ),
        );
    let state = state.with_query_limits(load_runtime_overrides(cli.runtime_overrides.as_deref())?);
    let router = prometheus_router(Arc::new(state)).merge(readiness_router(readiness));
    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());
    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, security, shutdown.signalled())
            .await?;
    tracing::info!(
        %bound,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics-service query-frontend listening"
    );
    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    server.await?;
    Ok(())
}
