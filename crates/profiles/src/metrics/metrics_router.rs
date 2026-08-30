use super::{SharedRegistry, export};

/// Build the `/metrics` router that serves the `OpenMetrics` text exposition of
/// `registry`. `serve_admin` merges the pprof routes separately.
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}
