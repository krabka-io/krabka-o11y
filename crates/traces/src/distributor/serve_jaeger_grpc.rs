use super::{
    CollectorServiceServer, GrpcAuthenticationLayer, GrpcServer, JaegerGrpcService,
    ReceiverEndpoint, SocketAddr, bind_listener, grpc_incoming, spawn_server,
};

/// Serve the Jaeger API v2 gRPC trace receiver until cancelled, returning the
/// bound address and the server's handle.
///
/// The server accepts connections through a
/// [`ServerListener`](krabka_observability::server_security::ServerListener), so it gets the
/// TLS of `security`, and it authenticates each call with a
/// [`GrpcAuthenticationLayer`]. It does not use tonic's own
/// `ServerTlsConfig`, which panics in a process that compiles in two rustls
/// crypto providers, as this one does.
///
/// Supervise the handle: a server that stops leaves a process that takes no
/// Jaeger gRPC spans.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_jaeger_grpc(
    endpoint: ReceiverEndpoint<'_>,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let ReceiverEndpoint {
        addr,
        state,
        security,
        shutdown,
    } = endpoint;
    let listener = bind_listener(addr, security).await?;
    let bound = listener.local_addr();
    let server = GrpcServer::builder()
        .layer(GrpcAuthenticationLayer::new(security))
        .add_service(CollectorServiceServer::new(JaegerGrpcService::new(state)))
        .serve_with_incoming_shutdown(grpc_incoming(listener), shutdown.cancelled_owned());
    Ok((
        bound,
        spawn_server(server, "traces distributor Jaeger gRPC server"),
    ))
}
