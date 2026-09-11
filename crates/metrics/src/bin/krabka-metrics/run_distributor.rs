use super::{
    Arc, AutoOffsetReset, Cli, Consumer, DistributorState, KafkaHaElectionSink, KafkaSink,
    Producer, RoleReadiness, ServiceMetrics, TcpListener, distributor_router, readiness_router,
    run_ha_election_consumer_loop,
};

pub(crate) async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
) -> Result<(), Box<dyn std::error::Error>> {
    // A distributor with no broker behind it accepts a push and has nowhere to
    // put it. Both client builds below reach the broker, and neither has
    // happened yet.
    let wal_broker = readiness.gate("wal-broker");
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&cli.bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build()
            .await?,
    );
    let mut ha_consumer = Consumer::builder()
        .bootstrap(&cli.bootstrap)
        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
        .frame_max(cli.client_frame_max)
        .group_id(cli.ha_tracker_group_id.clone())
        .client_id(cli.ha_tracker_client_id.clone())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ha_tracker_topic.clone()])
        .build()
        .await?;
    wal_broker.mark_ready();
    let state = Arc::new(
        DistributorState::new(Arc::new(KafkaSink::new(Arc::clone(&producer))))
            .with_ha_failover_timeout(cli.ha_failover_timeout)
            .with_max_rate_buckets(cli.ingest_rate_bucket_cap)
            .with_max_decompressed(cli.distributor_max_decompressed)
            .with_ha_election_sink(Arc::new(KafkaHaElectionSink::new(
                Arc::clone(&producer),
                cli.ha_tracker_topic.clone(),
            )))
            .with_metrics(metrics),
    );
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
    let listener = TcpListener::bind(cli.listen).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "metrics distributor listening");
    let server = std::future::IntoFuture::into_future(
        axum::serve(
            listener,
            krabka_observability::contain_handler_panics(
                distributor_router(state).merge(readiness_router(readiness)),
            ),
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
