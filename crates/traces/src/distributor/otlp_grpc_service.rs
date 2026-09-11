use super::{
    Arc, AsciiMetadataValue, DistributorState, ExportTraceServiceRequest,
    ExportTraceServiceResponse, GrpcRequest, GrpcResponse, GrpcStatus, TENANT_HEADER, TraceService,
    TracesData, decode_otlp, grpc_status_from_error, produce_spans, request_principal,
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
        let tenant = self
            .state
            .resolve_tenant(
                request_principal(&request)?,
                request
                    .metadata()
                    .get(TENANT_HEADER)
                    .map(AsciiMetadataValue::as_bytes),
            )
            .map_err(|err| grpc_status_from_error(&err))?;
        let data = TracesData {
            resource_spans: request.into_inner().resource_spans,
        };
        let spans =
            decode_otlp(&data).map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        self.state
            .enforce_ingest(&tenant, &spans)
            .map_err(|err| grpc_status_from_error(&err))?;
        produce_spans(self.state.sink.as_ref(), &tenant, spans)
            .await
            .map_err(|err| GrpcStatus::internal(err.to_string()))?;
        Ok(GrpcResponse::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}
