use super::{
    Arc, ByteSizeExt, DistributorState, MetricsServiceServer, OtlpMetricsService,
    otlp_metrics_service,
};

/// Builds a tonic server for OTLP metrics export.
#[must_use]
pub fn otlp_metrics_service_server(
    state: Arc<DistributorState>,
) -> MetricsServiceServer<OtlpMetricsService> {
    let max_decompressed = state.max_decompressed.bytes_usize();
    MetricsServiceServer::new(otlp_metrics_service(state))
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(max_decompressed)
}
