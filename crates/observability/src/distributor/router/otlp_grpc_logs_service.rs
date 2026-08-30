use super::{
    AllowAllIngestLimiter, Arc, AtomicBool, DistributorState, LogIngestLimiter, LogWalSink,
    LogsService, ProtoExportLogsServiceRequest, ProtoExportLogsServiceResponse, ServiceMetrics,
    Time, append_distributor_wal_records, distributor_error_to_grpc_status, grpc_tenant,
    normalize_otlp_proto_logs_for_tenant, otlp_grpc_logs_service_with_limiter,
};

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    pub(crate) sink: Arc<dyn LogWalSink>,
    pub(crate) ingest_limiter: Arc<dyn LogIngestLimiter>,
    pub(crate) wal_append_timeout: Option<Time>,
    pub(crate) metrics: ServiceMetrics,
}

pub fn otlp_grpc_logs_service(sink: impl LogWalSink) -> OtlpGrpcLogsService {
    otlp_grpc_logs_service_with_limiter(sink, AllowAllIngestLimiter)
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcLogsService {
    async fn export(
        &self,
        request: tonic::Request<ProtoExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ProtoExportLogsServiceResponse>, tonic::Status> {
        let (metadata, _, payload) = request.into_parts();
        let tenant = grpc_tenant(&metadata)?;
        let records = normalize_otlp_proto_logs_for_tenant(tenant, payload, None, None)
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        let state = DistributorState {
            sink: Arc::clone(&self.sink),
            ingest_limiter: Arc::clone(&self.ingest_limiter),
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            max_ingest_body: None,
            wal_append_timeout: self.wal_append_timeout,
            reject_old_samples_max_age: None,
            creation_grace_period: None,
            metrics: self.metrics.clone(),
        };
        append_distributor_wal_records(&state, records)
            .await
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        Ok(tonic::Response::new(ProtoExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
