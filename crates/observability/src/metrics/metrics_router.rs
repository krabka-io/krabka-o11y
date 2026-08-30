use super::*;

/// `/metrics` router that serves the `OpenMetrics` text encoding of
/// `registry`.
///
/// `serve_admin_from_env_with` merges it onto the admin port. It does NOT
/// include the pprof routes, which `serve_admin` adds.
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}
