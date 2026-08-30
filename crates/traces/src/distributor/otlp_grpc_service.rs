use super::*;

/// OTLP/gRPC trace export service backed by the traces WAL.
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
        let metadata = request.metadata().clone();
        let data = TracesData {
            resource_spans: request.into_inner().resource_spans,
        };
        let spans =
            decode_otlp(&data).map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        let tenant = tenant_metadata(&metadata);
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
