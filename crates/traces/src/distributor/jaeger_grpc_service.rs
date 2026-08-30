use super::{
    Arc, CollectorService, DistributorState, GrpcRequest, GrpcResponse, GrpcStatus,
    PostSpansRequest, PostSpansResponse, decode_jaeger_grpc_batch, grpc_status_from_error,
    produce_spans, tenant_metadata,
};

/// Jaeger API v2 gRPC collector backed by the traces WAL.
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
        let metadata = request.metadata().clone();
        let batch = request
            .into_inner()
            .batch
            .ok_or_else(|| GrpcStatus::invalid_argument("missing jaeger batch"))?;
        let spans = decode_jaeger_grpc_batch(batch)
            .map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        let tenant = tenant_metadata(&metadata);
        self.state
            .enforce_ingest(&tenant, &spans)
            .map_err(|err| grpc_status_from_error(&err))?;
        produce_spans(self.state.sink.as_ref(), &tenant, spans)
            .await
            .map_err(|err| GrpcStatus::internal(err.to_string()))?;
        Ok(GrpcResponse::new(PostSpansResponse {}))
    }
}
