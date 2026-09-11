use super::{
    Arc, AutoOffsetReset, Cli, ClientSecurity, Consumer, DistributorState, KafkaHaElectionSink,
    KafkaSink, Producer, RoleReadiness, ServerListener, ServerSecurity, ServiceMetrics,
    TcpListener, distributor_router, load_runtime_overrides, readiness_router,
    run_ha_election_consumer_loop, serve_router,
};

pub(crate) async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    security: &ServerSecurity,
    wal_security: Option<ClientSecurity>,
) -> Result<(), Box<dyn std::error::Error>> {
    // A distributor with no broker behind it accepts a push and has nowhere to
    // put it. Both client builds below reach the broker, and neither has
    // happened yet.
    let wal_broker = readiness.gate("wal-broker");
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&cli.bootstrap)
            .maybe_security(wal_security.clone())
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build()
            .await?,
    );
    let mut ha_consumer = Consumer::builder()
        .bootstrap(&cli.bootstrap)
        .maybe_security(wal_security)
        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
        .frame_max(cli.client_frame_max)
        .group_id(cli.ha_tracker_group_id.clone())
        .client_id(cli.ha_tracker_client_id.clone())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ha_tracker_topic.clone()])
        .build()
        .await?;
    wal_broker.mark_ready();
    let mut state = DistributorState::new(Arc::new(KafkaSink::new(Arc::clone(&producer))))
        .with_ha_failover_timeout(cli.ha_failover_timeout)
        .with_max_rate_buckets(cli.ingest_rate_bucket_cap)
        .with_max_decompressed(cli.distributor_max_decompressed)
        .with_ha_election_sink(Arc::new(KafkaHaElectionSink::new(
            Arc::clone(&producer),
            cli.ha_tracker_topic.clone(),
        )))
        .with_metrics(metrics);
    // Without a runtime overrides file every tenant keeps the built-in limits,
    // which is the documented default rather than a missing configuration.
    if let Some(overrides) = load_runtime_overrides(cli.runtime_overrides.as_deref())? {
        state = state.with_overrides(overrides);
    }
    let state = Arc::new(state);
    let ha_state = Arc::clone(&state);
    let ha_topic = cli.ha_tracker_topic.clone();
    let ha_poll_timeout = cli.ha_tracker_poll_timeout;
    let mut ha_task = tokio::spawn(async move {
        run_ha_election_consumer_loop(
            &mut ha_consumer,
            ha_state.tracker(),
            &ha_topic,
            ha_poll_timeout,
            |_| false,
        )
        .await
    });
    let listener = ServerListener::bind(TcpListener::bind(cli.listen).await?, security)?;
    let bound = listener.local_addr();
    tracing::info!(
        %bound,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics distributor listening"
    );
    let server = std::future::IntoFuture::into_future(
        serve_router(
            listener,
            distributor_router(state).merge(readiness_router(readiness)),
            security,
        )
        .with_graceful_shutdown(async {
            krabka_observability::shutdown_signal().await;
        }),
    );
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            ha_task.abort();
            result?;
        }
        result = &mut ha_task => {
            match result {
                Ok(Ok(_)) => return Err("metrics HA tracker consumer stopped unexpectedly".into()),
                Ok(Err(error)) => return Err(error.into()),
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}
