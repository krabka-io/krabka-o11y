use super::{
    Arc, AutoOffsetReset, Cli, Consumer, KafkaRecordingRuleWalSink, KafkaRulerStateSink,
    ObjectStore, Producer, PrometheusApiState, PrometheusRulerStateSink, RulerAlertmanagerSink,
    RulerShard, RulerStateFanoutSink, Shutdown, WalHead, install_bundled_rule_groups,
    load_runtime_overrides, prometheus_router, query_engine_opts, run_ruler_evaluation_loop,
    run_ruler_state_consumer_loop, serve_prometheus_router_joinable,
    spawn_shutdown_signal_listener,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_ruler",
    skip_all,
    fields(listen = %cli.listen, tenant = %cli.ruler_tenant, shard_index = cli.ruler_shard_index, shard_total = cli.ruler_shard_total),
    err
)]
pub(crate) async fn run_ruler(
    cli: Cli,
    metrics: krabka_promql::metrics::ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let metric_store = krabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        WalHead::new(),
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
    let state = Arc::new(state);
    let router = prometheus_router(Arc::clone(&state));
    let shard = RulerShard::new(cli.ruler_shard_index, cli.ruler_shard_total)?;

    // Install the bundled rules before the ruler reaches Kafka, so a rule file
    // an operator names but the ruler cannot install stops the start early.
    if let Some(path) = cli.ruler_bundled_rules.as_deref() {
        let groups = install_bundled_rule_groups(&router, path, &cli.ruler_tenant).await?;
        tracing::info!(
            path = %path.display(),
            groups = groups.len(),
            "metrics ruler installed the bundled rule groups"
        );
    }

    let bootstrap = cli.wal_bootstrap.clone().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--wal-bootstrap is required for --target ruler",
        )
    })?;
    let mut state_consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
        .frame_max(cli.client_frame_max)
        .group_id(format!("{}-ruler-state", cli.wal_group_id))
        .client_id(format!("{}-ruler-state", cli.wal_client_id))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ruler_state_topic.clone()])
        .build()
        .await?;
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build()
            .await?,
    );
    let wal_sink = KafkaRecordingRuleWalSink::new(Arc::clone(&producer), cli.wal_topic.clone());
    let state_sink = RulerStateFanoutSink::new(
        PrometheusRulerStateSink::new(Arc::clone(&state)),
        KafkaRulerStateSink::new(producer, cli.ruler_state_topic.clone()),
    );
    let tenant = cli.ruler_tenant.clone();
    let interval = cli.ruler_eval_interval;
    let alertmanager_url = cli.ruler_alertmanager_url.clone();
    let state_for_replay = Arc::clone(&state);
    let state_topic = cli.ruler_state_topic.clone();
    let poll_timeout = cli.wal_poll_timeout;

    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());

    // The ruler state consumer and evaluation loop are critical: both feed
    // ruler correctness. Their stop predicate observes the shared shutdown, and
    // if either returns (the loops only return on error, never voluntarily) we
    // surface it with `error!` and trigger shutdown so the process winds down
    // loudly rather than silently running headless.
    let consumer_shutdown = shutdown.clone();
    let consumer_stop = consumer_shutdown.rx.clone();
    tokio::spawn(async move {
        let result = run_ruler_state_consumer_loop(
            &mut state_consumer,
            &state_for_replay,
            &state_topic,
            poll_timeout,
            move |_| *consumer_stop.borrow(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics ruler state consumer stopped; shutting down");
        }
        consumer_shutdown.trigger();
    });
    let eval_shutdown = shutdown.clone();
    let eval_stop = eval_shutdown.rx.clone();
    tokio::spawn(async move {
        let result = run_ruler_evaluation_loop(
            state,
            (
                wal_sink,
                RulerAlertmanagerSink::from_endpoint(alertmanager_url),
                state_sink,
            ),
            tenant,
            shard,
            interval,
            move || *eval_stop.borrow(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics ruler evaluation loop stopped; shutting down");
        }
        eval_shutdown.trigger();
    });

    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, shutdown.signalled()).await?;
    tracing::info!(%bound, "metrics-service ruler listening");
    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    server.await?;
    Ok(())
}
