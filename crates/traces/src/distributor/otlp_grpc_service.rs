use super::{
    Arc, DistributorState, ExportTraceServiceRequest, ExportTraceServiceResponse, GrpcRequest,
    GrpcResponse, GrpcStatus, TraceService, TracesData, decode_otlp,
};

/// OTLP/gRPC trace export service backed by the traces WAL.
///
/// Each export reads its [`Principal`](krabka_observability::server_security::Principal)
/// from the request extensions, where
/// [`GrpcAuthenticationLayer`](krabka_observability::server_security::GrpcAuthenticationLayer)
/// puts it. [`serve_otlp_grpc`](super::serve_otlp_grpc) adds that layer.
pub struct OtlpGrpcService {
    pub(crate) state: Arc<DistributorState>,
}

impl OtlpGrpcService {
    #[must_use]
    pub fn new(state: Arc<DistributorState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl TraceService for OtlpGrpcService {
    async fn export(
        &self,
        request: GrpcRequest<ExportTraceServiceRequest>,
    ) -> Result<GrpcResponse<ExportTraceServiceResponse>, GrpcStatus> {
        let tenant = self.state.resolve_grpc_tenant(&request)?;
        let data = TracesData {
            resource_spans: request.into_inner().resource_spans,
        };
        let spans =
            decode_otlp(&data).map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        self.state.ingest_grpc_spans(&tenant, spans).await?;
        Ok(GrpcResponse::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}


use tonic::service::Routes;

use super::{GrpcReceiver, ReceiverEndpoint, RunningServer, TraceServiceServer, serve_grpc_receiver};

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
pub async fn serve_otlp_grpc(endpoint: ReceiverEndpoint<'_>) -> std::io::Result<RunningServer> {
    serve_grpc_receiver(
        endpoint,
        GrpcReceiver {
            routes: |state| Routes::new(TraceServiceServer::new(OtlpGrpcService::new(state))),
            server_name: "traces distributor OTLP/gRPC server",
        },
    )
    .await
}
