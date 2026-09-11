use super::{
    Arc, CancellationToken, CollectorServiceServer, DistributorState, GrpcAuthenticationLayer,
    GrpcServer, JaegerGrpcService, ServerListener, ServerSecurity, SocketAddr, grpc_incoming,
};

/// Serve the Jaeger API v2 gRPC trace receiver until cancelled, returning the
/// bound address and the server's handle.
///
/// The server accepts connections through a [`ServerListener`], so it gets the
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
    addr: SocketAddr,
    state: Arc<DistributorState>,
    security: &ServerSecurity,
    shutdown: CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let tcp = tokio::net::TcpListener::bind(addr).await?;
    let listener = ServerListener::bind(tcp, security).map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let server = GrpcServer::builder()
        .layer(GrpcAuthenticationLayer::new(security))
        .add_service(CollectorServiceServer::new(JaegerGrpcService::new(state)))
        .serve_with_incoming_shutdown(grpc_incoming(listener), shutdown.cancelled_owned());
    let handle = tokio::spawn(async move {
        if let Err(err) = server.await {
            tracing::error!(error = %err, "traces distributor Jaeger gRPC server stopped");
        }
    });
    Ok((bound, handle))
}
