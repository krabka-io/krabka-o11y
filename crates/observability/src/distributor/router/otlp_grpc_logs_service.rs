use prost::Message as _;

use super::{
    AllowAllIngestLimiter, Arc, DRAINING_GATE, DistributorState, LogIngestLimiter, LogWalSink,
    LogsService, OverridesProvider, ProtoExportLogsServiceRequest, ProtoExportLogsServiceResponse,
    ReadinessGate, RequestSecurity, ServiceMetrics, Time, append_distributor_wal_records,
    distributor_error_to_grpc_status, grpc_tenant, measured_size,
    normalize_otlp_proto_logs_for_tenant, otlp_grpc_logs_service_with_limiter,
    validate_ingest_body_limit,
};

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    pub(crate) sink: Arc<dyn LogWalSink>,
    pub(crate) ingest_limiter: Arc<dyn LogIngestLimiter>,
    /// The same provider the HTTP push routes carry.
    ///
    /// The gRPC export path is an ingest door like the others, so it answers a
    /// tenant with that tenant's limits and not with none.
    pub(crate) overrides: Arc<OverridesProvider>,
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
        let (metadata, extensions, payload) = request.into_parts();
        // The router serves this service, so the authentication layer has put
        // the principal into the same extensions as for an HTTP request.
        let security = RequestSecurity::from_extensions(&extensions)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let tenant = grpc_tenant(&metadata)?;
        security
            .authorize_tenant(&tenant)
            .map_err(|denied| tonic::Status::permission_denied(denied.to_string()))?;
        // One resolution for the whole export, as the HTTP push handlers do.
        let limits = self.overrides.for_tenant(&tenant).clone();
        // The export is this door's request body, so the body cap applies to
        // it. `encoded_len` is what the same message occupied on the wire.
        validate_ingest_body_limit(&limits, measured_size(payload.encoded_len()))
            .map_err(|error| distributor_error_to_grpc_status(&error))?;
        let records = normalize_otlp_proto_logs_for_tenant(&tenant, payload, &limits)
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        // A state for this one append. The gRPC export path has no drain
        // route of its own, so the gate it carries is met and nothing drops it.
        let accepting_writes = ReadinessGate::unmet(DRAINING_GATE);
        accepting_writes.mark_ready();
        let state = DistributorState {
            sink: Arc::clone(&self.sink),
            ingest_limiter: Arc::clone(&self.ingest_limiter),
            prepare_shutdown: accepting_writes,
            overrides: Arc::clone(&self.overrides),
            wal_append_timeout: self.wal_append_timeout,
            metrics: self.metrics.clone(),
        };
        append_distributor_wal_records(&state, &security, &tenant, records)
            .await
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        Ok(tonic::Response::new(ProtoExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
