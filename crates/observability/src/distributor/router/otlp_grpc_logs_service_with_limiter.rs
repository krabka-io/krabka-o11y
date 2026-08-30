use super::{Arc, LogIngestLimiter, LogWalSink, OtlpGrpcLogsService, ServiceMetrics};

pub fn otlp_grpc_logs_service_with_limiter(
    sink: impl LogWalSink,
    ingest_limiter: impl LogIngestLimiter,
) -> OtlpGrpcLogsService {
    OtlpGrpcLogsService {
        sink: Arc::new(sink),
        ingest_limiter: Arc::new(ingest_limiter),
        wal_append_timeout: None,
        metrics: ServiceMetrics::new(),
    }
}
