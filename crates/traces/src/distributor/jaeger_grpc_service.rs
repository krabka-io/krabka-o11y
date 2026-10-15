use super::{
    Arc, CollectorService, DistributorState, GrpcRequest, GrpcResponse, GrpcStatus,
    PostSpansRequest, PostSpansResponse, decode_jaeger_grpc_batch,
};

/// Jaeger API v2 gRPC collector backed by the traces WAL.
///
/// Each call reads its [`Principal`](krabka_observability::server_security::Principal)
/// from the request extensions, where
/// [`GrpcAuthenticationLayer`](krabka_observability::server_security::GrpcAuthenticationLayer)
/// puts it. [`serve_jaeger_grpc`](super::serve_jaeger_grpc) adds that layer.
pub struct JaegerGrpcService {
    pub(crate) state: Arc<DistributorState>,
}

impl JaegerGrpcService {
    #[must_use]
    pub fn new(state: Arc<DistributorState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl CollectorService for JaegerGrpcService {
    async fn post_spans(
        &self,
        request: GrpcRequest<PostSpansRequest>,
    ) -> Result<GrpcResponse<PostSpansResponse>, GrpcStatus> {
        let tenant = self.state.resolve_grpc_tenant(&request)?;
        let batch = request
            .into_inner()
            .batch
            .ok_or_else(|| GrpcStatus::invalid_argument("missing jaeger batch"))?;
        let spans = decode_jaeger_grpc_batch(batch)
            .map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        self.state.ingest_grpc_spans(&tenant, spans).await?;
        Ok(GrpcResponse::new(PostSpansResponse {}))
    }
}


use tonic::service::Routes;

use super::{GrpcReceiver, ReceiverEndpoint, RunningServer, CollectorServiceServer, serve_grpc_receiver};

/// Serve the Jaeger API v2 gRPC trace receiver until cancelled, returning the
/// bound address and the server's handle.
///
/// The server accepts connections through a
/// [`ServerListener`](krabka_observability::server_security::ServerListener), so it gets the
/// TLS of `security`, and it authenticates each call with a
/// [`GrpcAuthenticationLayer`](krabka_observability::server_security::GrpcAuthenticationLayer). It does not use tonic's own
/// `ServerTlsConfig`, which panics in a process that compiles in two rustls
/// crypto providers, as this one does.
///
/// Supervise the handle: a server that stops leaves a process that takes no
/// Jaeger gRPC spans.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_jaeger_grpc(endpoint: ReceiverEndpoint<'_>) -> std::io::Result<RunningServer> {
    serve_grpc_receiver(
        endpoint,
        GrpcReceiver {
            routes: |state| Routes::new(CollectorServiceServer::new(JaegerGrpcService::new(state))),
            server_name: "traces distributor Jaeger gRPC server",
        },
    )
    .await
}
