pub(crate) mod router;
pub use router::{
    DistributorState, OtlpGrpcLogsService, distributor_router, distributor_router_with_overrides,
    otlp_grpc_logs_service, otlp_grpc_logs_service_with_limiter,
};
pub(crate) mod ingest;
pub(crate) mod loki_normalization;
pub(crate) mod otlp_normalization;
pub(crate) mod value_conversion;

pub(crate) fn format_loki_label_set(labels: &crate::Labels, separator: &str) -> String {
    let values = labels
        .iter()
        .map(|entry| format!("{}={}", entry.0, crate::quote_logql_string(entry.1)))
        .collect::<Vec<_>>()
        .join(separator);
    format!("{{{values}}}")
}
