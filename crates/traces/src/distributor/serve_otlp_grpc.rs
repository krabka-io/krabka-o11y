use super::*;

/// Serve the OTLP/gRPC trace receiver until cancelled.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn serve_otlp_grpc(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    GrpcServer::builder()
        .add_service(TraceServiceServer::new(OtlpGrpcService::new(state)))
        .serve_with_shutdown(addr, async move {
            shutdown.cancelled().await;
        })
        .await
}
