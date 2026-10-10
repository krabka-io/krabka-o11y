use tonic::service::Routes;

use super::{
    GrpcReceiver, OtlpGrpcService, ReceiverEndpoint, SocketAddr, TraceServiceServer,
    serve_grpc_receiver,
};

/// Serve the OTLP/gRPC trace receiver until cancelled, returning the bound
/// address and the server's handle.
///
/// The server accepts connections through a
/// [`ServerListener`](krabka_observability::server_security::ServerListener), so it gets the
/// TLS of `security`, and it authenticates each call with a
/// [`GrpcAuthenticationLayer`](krabka_observability::server_security::GrpcAuthenticationLayer). It does not use tonic's own
/// `ServerTlsConfig`, which panics in a process that compiles in two rustls
/// crypto providers, as this one does.
///
/// Supervise the handle: a server that stops leaves a process that takes no
/// OTLP/gRPC spans.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_otlp_grpc(
    endpoint: ReceiverEndpoint<'_>,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    serve_grpc_receiver(
        endpoint,
        GrpcReceiver {
            routes: |state| Routes::new(TraceServiceServer::new(OtlpGrpcService::new(state))),
            server_name: "traces distributor OTLP/gRPC server",
        },
    )
    .await
}
