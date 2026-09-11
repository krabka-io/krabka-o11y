use super::{
    Arc, Limits, LogIngestLimiter, LogWalSink, OtlpGrpcLogsService, OverridesProvider,
    ServiceMetrics,
};

/// An export service with every limit turned off.
///
/// It takes a sink and a limiter and nothing else, so there is no operator
/// configuration for it to read a limit from. The service path builds its
/// export service from the distributor's push routes instead, and that one
/// carries the process's provider.
pub fn otlp_grpc_logs_service_with_limiter(
    sink: impl LogWalSink,
    ingest_limiter: impl LogIngestLimiter,
) -> OtlpGrpcLogsService {
    OtlpGrpcLogsService {
        sink: Arc::new(sink),
        ingest_limiter: Arc::new(ingest_limiter),
        overrides: Arc::new(OverridesProvider::new(Limits::unenforced())),
        wal_append_timeout: None,
        metrics: ServiceMetrics::new(),
    }
}
