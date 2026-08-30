use super::{SharedRegistry, export};

/// `/metrics` router that serves the `OpenMetrics` text encoding of `registry`.
///
/// `serve_admin_from_env_with` merges it onto the admin port. This router does
/// NOT include the pprof routes. `serve_admin` adds those.
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}
