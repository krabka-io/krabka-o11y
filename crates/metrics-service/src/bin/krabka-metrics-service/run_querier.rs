use super::{
    Arc, AutoOffsetReset, Cli, Consumer, ObjectStore, PrometheusApiState, Shutdown, WalHead,
    load_runtime_overrides, prometheus_router, query_engine_opts, serve_prometheus_router_joinable,
    spawn_shutdown_signal_listener, spawn_wal_head_consumer_task,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_querier",
    skip_all,
    fields(listen = %cli.listen, object_store = %cli.object_store_url, manifest_prefix = %cli.manifest_prefix, wal_topic = %cli.wal_topic),
    err
)]
pub(crate) async fn run_querier(
    cli: Cli,
    metrics: krabka_promql::metrics::ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let head = WalHead::with_retention(cli.wal_head_retention);
    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());
    if let Some(bootstrap) = cli.wal_bootstrap.clone() {
        let wal_head = head.clone();
        let wal_topic = cli.wal_topic.clone();
        let poll_timeout = cli.wal_poll_timeout;
        let group_id = cli.wal_group_id.clone();
        let client_id = cli.wal_client_id.clone();
        let subscribe_topic = cli.wal_topic.clone();
        spawn_wal_head_consumer_task(
            move || async move {
                Consumer::builder()
                    .bootstrap(bootstrap)
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
        );
    }
    let metric_store = krabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        head,
    )
    .with_cold_cache_ttl(cli.cold_cache_ttl)
    .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
    let mut state = PrometheusApiState::new(Arc::new(metric_store), query_engine_opts(&cli))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_metrics(metrics);
    if let Some(overrides) = load_runtime_overrides(cli.runtime_overrides.as_deref())? {
        state = state.with_query_limits(overrides);
    }
    let router = prometheus_router(Arc::new(state));
    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, shutdown.signalled()).await?;
    tracing::info!(%bound, "metrics-service querier listening");
    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    server.await?;
    Ok(())
}
