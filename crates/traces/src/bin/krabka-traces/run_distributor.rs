use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};

use super::{
    Arc, CancellationToken, Cli, DistributorState, KafkaSink, Producer, ServiceMetrics, SocketAddr,
    distributor, ingest_rate_from_cli,
};

/// Accepts pushes on every ingest protocol traces speaks, and writes the WAL.
///
/// `serve_primary_listen` is false only under `--target all`, where `--listen`
/// is the query-frontend's Tempo API port and cannot also be the
/// distributor's. Nothing is lost by dropping it there: the primary listener
/// serves [`distributor::serve`]'s router, and `--otlp-http-listen` serves the
/// *same* router from the same state, so every route reachable on `--listen`
/// is reachable on 4318. The six protocol ports below are unaffected, because
/// those are the ports a collector is configured with.
pub(crate) async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    serve_primary_listen: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Nowhere to put a push until the WAL producer has a broker, and the seven
    // ingest listeners below all bind after it. The admin port is already up,
    // so `/ready` reports the connect.
    let wal_broker = readiness.gate("wal-broker");
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
    wal_broker.mark_ready();
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
    let grpc_addr: SocketAddr = cli.grpc_listen.parse()?;
    let otlp_http_addr: SocketAddr = cli.otlp_http_listen.parse()?;
    let jaeger_grpc_addr: SocketAddr = cli.jaeger_grpc_listen.parse()?;
    let jaeger_compact_addr: SocketAddr = cli.jaeger_compact_listen.parse()?;
    let jaeger_http_addr: SocketAddr = cli.jaeger_http_listen.parse()?;
    let zipkin_addr: SocketAddr = cli.zipkin_listen.parse()?;

    // Seven listeners, one role. A distributor that has lost one of them still
    // binds the other six and still passes a liveness probe, while everything
    // pushed to the lost port is dropped. Each accept loop is therefore
    // supervised by name, and any of them ending -- including by a panic,
    // which no `if let Err` in the task body could have seen -- ends the role.
    let mut tasks = SupervisedTasks::new(shutdown.clone());

    let grpc_shutdown = shutdown.clone();
    let grpc_state = Arc::clone(&state);
    tasks.spawn("traces distributor OTLP/gRPC", async move {
        if let Err(err) = distributor::serve_otlp_grpc(grpc_addr, grpc_state, grpc_shutdown).await {
            tracing::error!(error = %err, "traces distributor OTLP/gRPC server stopped");
        }
    });
    let jaeger_grpc_shutdown = shutdown.clone();
    let jaeger_grpc_state = Arc::clone(&state);
    tasks.spawn("traces distributor Jaeger gRPC", async move {
        if let Err(err) = distributor::serve_jaeger_grpc(
            jaeger_grpc_addr,
            jaeger_grpc_state,
            jaeger_grpc_shutdown,
        )
        .await
        {
            tracing::error!(error = %err, "traces distributor Jaeger gRPC server stopped");
        }
    });
    let (jaeger_compact_bound, jaeger_compact) = distributor::serve_jaeger_compact_udp(
        jaeger_compact_addr,
        Arc::clone(&state),
        shutdown.clone(),
    )
    .await?;
    tasks.adopt("traces distributor Jaeger compact UDP", jaeger_compact);
    tracing::info!(%jaeger_compact_bound, "traces distributor Jaeger compact UDP listening");
    let (otlp_http_bound, otlp_http) =
        distributor::serve(otlp_http_addr, Arc::clone(&state), shutdown.clone()).await?;
    tasks.adopt("traces distributor OTLP/HTTP", otlp_http);
    tracing::info!(%otlp_http_bound, "traces distributor OTLP/HTTP listening");
    let (jaeger_http_bound, jaeger_http) =
        distributor::serve(jaeger_http_addr, Arc::clone(&state), shutdown.clone()).await?;
    tasks.adopt("traces distributor Jaeger thrift HTTP", jaeger_http);
    tracing::info!(%jaeger_http_bound, "traces distributor Jaeger thrift HTTP listening");
    let (zipkin_bound, zipkin) =
        distributor::serve(zipkin_addr, Arc::clone(&state), shutdown.clone()).await?;
    tasks.adopt("traces distributor Zipkin HTTP", zipkin);
    tracing::info!(%zipkin_bound, "traces distributor Zipkin HTTP listening");
    if serve_primary_listen {
        let addr: SocketAddr = cli.listen.parse()?;
        let (bound, primary) = distributor::serve(addr, state, shutdown.clone()).await?;
        tasks.adopt("traces distributor HTTP", primary);
        tracing::info!(%bound, "traces distributor listening");
    }

    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    tasks.shutdown().await;
    outcome
}
