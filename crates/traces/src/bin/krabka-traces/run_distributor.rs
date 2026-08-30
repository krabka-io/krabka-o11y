use super::*;

pub(crate) async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Boxed: the producer-startup future is several KB and would otherwise be
    // inlined into this role's future (and from there into `run`'s). One
    // allocation at startup keeps the role futures small.
    let producer = Box::pin(
        Producer::builder()
            .bootstrap(cli.bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build(),
    )
    .await?;
    let mut state =
        DistributorState::with_metrics(Arc::new(KafkaSink::new(Arc::new(producer))), metrics);
    state.limits.max_spans_per_request = cli.max_spans_per_request;
    state.limits.max_spans_per_trace = cli.max_spans_per_trace;
    state.limits.max_ingest_rate = ingest_rate_from_cli(cli.max_ingest_spans_per_second);
    state.limits.ingest_rate_burst = cli.ingest_rate_burst;
    state.limits.max_attr_value = cli.max_attr_value_len;
    state.shared_limits = state.limits.to_shared_limits();
    state.max_decompressed = cli.max_decompressed_bytes;
    let state = Arc::new(state);
    let addr: SocketAddr = cli.listen.parse()?;
    let grpc_addr: SocketAddr = cli.grpc_listen.parse()?;
    let otlp_http_addr: SocketAddr = cli.otlp_http_listen.parse()?;
    let jaeger_grpc_addr: SocketAddr = cli.jaeger_grpc_listen.parse()?;
    let jaeger_compact_addr: SocketAddr = cli.jaeger_compact_listen.parse()?;
    let jaeger_http_addr: SocketAddr = cli.jaeger_http_listen.parse()?;
    let zipkin_addr: SocketAddr = cli.zipkin_listen.parse()?;
    let grpc_shutdown = shutdown.clone();
    let grpc_failure = shutdown.clone();
    let grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(err) = distributor::serve_otlp_grpc(grpc_addr, grpc_state, grpc_shutdown).await {
            tracing::error!(error = %err, "traces distributor OTLP/gRPC server stopped");
            grpc_failure.cancel();
        }
    });
    let jaeger_grpc_shutdown = shutdown.clone();
    let jaeger_grpc_failure = shutdown.clone();
    let jaeger_grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(err) = distributor::serve_jaeger_grpc(
            jaeger_grpc_addr,
            jaeger_grpc_state,
            jaeger_grpc_shutdown,
        )
        .await
        {
            tracing::error!(error = %err, "traces distributor Jaeger gRPC server stopped");
            jaeger_grpc_failure.cancel();
        }
    });
    let jaeger_compact_bound = distributor::serve_jaeger_compact_udp(
        jaeger_compact_addr,
        Arc::clone(&state),
        shutdown.clone(),
    )
    .await?;
    tracing::info!(%jaeger_compact_bound, "traces distributor Jaeger compact UDP listening");
    let otlp_http_bound =
        distributor::serve(otlp_http_addr, Arc::clone(&state), shutdown.clone()).await?;
    tracing::info!(%otlp_http_bound, "traces distributor OTLP/HTTP listening");
    let jaeger_http_bound =
        distributor::serve(jaeger_http_addr, Arc::clone(&state), shutdown.clone()).await?;
    tracing::info!(%jaeger_http_bound, "traces distributor Jaeger thrift HTTP listening");
    let zipkin_bound =
        distributor::serve(zipkin_addr, Arc::clone(&state), shutdown.clone()).await?;
    tracing::info!(%zipkin_bound, "traces distributor Zipkin HTTP listening");
    let bound = distributor::serve(addr, state, shutdown.clone()).await?;
    tracing::info!(%bound, "traces distributor listening");
    shutdown.cancelled().await;
    Ok(())
}
