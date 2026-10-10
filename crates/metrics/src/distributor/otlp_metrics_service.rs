use super::{
    Arc, ByteSize, ByteSizeExt, DistributorState, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse, IngestRequest, MetricsService, RequestOutcome, Status,
    StdDurationExt, TonicRequest, TonicResponse, otlp_grpc_export_inner, status_from_push_error,
};

/// Builds the OTLP gRPC metrics service implementation.
#[must_use]
pub fn otlp_metrics_service(state: Arc<DistributorState>) -> OtlpMetricsService {
    OtlpMetricsService { state }
}

/// OTLP `MetricsService` implementation backed by the distributor WAL pipeline.
#[derive(Clone)]
pub struct OtlpMetricsService {
    pub(crate) state: Arc<DistributorState>,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: TonicRequest<ExportMetricsServiceRequest>,
    ) -> Result<TonicResponse<ExportMetricsServiceResponse>, Status> {
        let started = std::time::Instant::now();
        let result = otlp_grpc_export_inner(&self.state, request).await;
        if let Some(metrics) = &self.state.metrics {
            metrics.record_ingest(IngestRequest {
                outcome: RequestOutcome::from_result(&result),
                body: ByteSize::ZERO,
                items: result.as_ref().map_or(0, |items| *items),
                elapsed: started.elapsed().as_time(),
            });
        }
        match result {
            Ok(_) => Ok(TonicResponse::new(ExportMetricsServiceResponse {
                partial_success: None,
            })),
            Err(error) => Err(status_from_push_error(&error)),
        }
    }
}
