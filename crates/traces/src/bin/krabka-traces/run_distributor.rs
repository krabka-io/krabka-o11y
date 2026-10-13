use krabka_observability::SupervisedTasks;

use super::{
    Arc, DistributorRole, DistributorState, KafkaSink, PrimaryListen, Producer, SocketAddr,
    distributor, limits_from_cli, load_traces_limits_overrides_config,
};

/// Accepts pushes on every ingest protocol traces speaks, and writes the WAL.
///
/// `primary_listen` is [`PrimaryListen::Skip`] only under `--target all`, where `--listen`
/// is the query-frontend's Tempo API port and cannot also be the
/// distributor's. Nothing is lost by dropping it there: the primary listener
/// serves [`distributor::serve`]'s router, and `--otlp-http-listen` serves the
/// *same* router from the same state, so every route reachable on `--listen`
/// is reachable on 4318. The six protocol ports below are unaffected, because
/// those are the ports a collector is configured with.
///
/// Every TCP listener serves with `security.server`, and the WAL producer
/// connects with `security.wal`. The Jaeger compact UDP receiver does not
/// start when authentication is on, because a datagram cannot carry a
/// credential.
pub(crate) async fn run_distributor(
    role: DistributorRole<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let DistributorRole {
        cli,
        metrics,
        readiness,
        shutdown,
        primary_listen,
        security,
    } = role;
    // Nowhere to put a push until the WAL producer has a broker, and the seven
    // ingest listeners below all bind after it. The admin port is already up,
    // so `/ready` reports the connect.
    let wal_broker = readiness.gate("wal-broker");
    // Built before the producer, so that a malformed overrides file stops the
    // role at once rather than after it has connected and bound seven ports.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        cli.traces_api_overrides_file.as_deref(),
        limits_from_cli(&cli),
    )?;
    // Boxed: the producer-startup future is several KB and would otherwise be
    // inlined into this role's future (and from there into `run`'s). One
    // allocation at startup keeps the role futures small.
    let producer = Box::pin(
        Producer::builder()
            .bootstrap(cli.bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .maybe_security(security.wal.clone())
            .build(),
    )
    .await?;
    wal_broker.mark_ready();
    let mut state =
        DistributorState::with_metrics(Arc::new(KafkaSink::new(Arc::new(producer))), metrics);
    state.overrides = overrides;
    state.max_decompressed = cli.max_decompressed_bytes;
    let state = Arc::new(state);
    let grpc_addr: SocketAddr = cli.grpc_listen.parse()?;
    let otlp_http_addr: SocketAddr = cli.otlp_http_listen.parse()?;
    let jaeger_grpc_addr: SocketAddr = cli.jaeger_grpc_listen.parse()?;
    let jaeger_compact_addr: SocketAddr = cli.jaeger_compact_listen.parse()?;
    let jaeger_http_addr: SocketAddr = cli.jaeger_http_listen.parse()?;
    let zipkin_addr: SocketAddr = cli.zipkin_listen.parse()?;
    let server = &security.server;
    let endpoint = |addr: SocketAddr| distributor::ReceiverEndpoint {
        addr,
        state: Arc::clone(&state),
        security: server,
        shutdown: shutdown.clone(),
    };

    // Seven listeners, one role. A distributor that has lost one of them still
    // binds the other six and still passes a liveness probe, while everything
    // pushed to the lost port is dropped. Each accept loop is therefore
    // supervised by name, and any of them ending -- including by a panic,
    // which no `if let Err` in the task body could have seen -- ends the role.
    let mut tasks = SupervisedTasks::new(shutdown.clone());

    let grpc = distributor::serve_otlp_grpc(endpoint(grpc_addr)).await?;
    tasks.adopt("traces distributor OTLP/gRPC", grpc.task);
    tracing::info!(bound = %grpc.bound_addr, "traces distributor OTLP/gRPC listening");
    let jaeger_grpc = distributor::serve_jaeger_grpc(endpoint(jaeger_grpc_addr)).await?;
    tasks.adopt("traces distributor Jaeger gRPC", jaeger_grpc.task);
    tracing::info!(bound = %jaeger_grpc.bound_addr, "traces distributor Jaeger gRPC listening");
    if let Some(jaeger_compact) =
        distributor::serve_jaeger_compact_udp(endpoint(jaeger_compact_addr)).await?
    {
        tasks.adopt("traces distributor Jaeger compact UDP", jaeger_compact.task);
        tracing::info!(bound = %jaeger_compact.bound_addr, "traces distributor Jaeger compact UDP listening");
    }
    let otlp_http = distributor::serve(endpoint(otlp_http_addr)).await?;
    tasks.adopt("traces distributor OTLP/HTTP", otlp_http.task);
    tracing::info!(bound = %otlp_http.bound_addr, "traces distributor OTLP/HTTP listening");
    let jaeger_http = distributor::serve(endpoint(jaeger_http_addr)).await?;
    tasks.adopt("traces distributor Jaeger thrift HTTP", jaeger_http.task);
    tracing::info!(bound = %jaeger_http.bound_addr, "traces distributor Jaeger thrift HTTP listening");
    let zipkin = distributor::serve(endpoint(zipkin_addr)).await?;
    tasks.adopt("traces distributor Zipkin HTTP", zipkin.task);
    tracing::info!(bound = %zipkin.bound_addr, "traces distributor Zipkin HTTP listening");
    if primary_listen == PrimaryListen::Serve {
        let addr: SocketAddr = cli.listen.parse()?;
        let primary = distributor::serve(endpoint(addr)).await?;
        tasks.adopt("traces distributor HTTP", primary.task);
        tracing::info!(bound = %primary.bound_addr, "traces distributor listening");
    }

    tasks.run_until_shutdown().await?;
    Ok(())
}
