use super::{
    Arc, CancellationToken, CollectorServiceServer, DistributorState, GrpcServer,
    JaegerGrpcService, SocketAddr,
};

/// Serve the Jaeger API v2 gRPC trace receiver until cancelled.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn serve_jaeger_grpc(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    GrpcServer::builder()
        .add_service(CollectorServiceServer::new(JaegerGrpcService::new(state)))
        .serve_with_shutdown(addr, async move {
            shutdown.cancelled().await;
        })
        .await
}
