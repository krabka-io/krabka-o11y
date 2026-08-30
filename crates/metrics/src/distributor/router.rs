use super::*;

/// Builds the distributor HTTP router.
pub fn router(state: Arc<DistributorState>) -> Router {
    let grpc_service = otlp_metrics_service_server(Arc::clone(&state));
    // Cap the (compressed) push body explicitly rather than relying on axum's
    // implicit 2 MiB default. A snappy body cannot usefully exceed the
    // decompressed cap, so `max_decompressed` is a sound, configurable ceiling
    // — applied per-route so the tonic gRPC `route_service` keeps its own limit.
    let max_body = state.max_decompressed.bytes_usize();
    Router::new()
        .route(
            "/api/v1/push",
            post(push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/api/v1/write",
            post(push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/api/v1/clocks",
            post(clocks_push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/otlp/v1/metrics",
            post(otlp_push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route_service(
            "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            grpc_service,
        )
        .with_state(state)
}
