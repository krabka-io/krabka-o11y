use super::*;

/// Builds a tonic server for OTLP metrics export.
#[must_use]
pub fn otlp_metrics_service_server(
    state: Arc<DistributorState>,
) -> MetricsServiceServer<OtlpMetricsService> {
    MetricsServiceServer::new(otlp_metrics_service(state))
}
