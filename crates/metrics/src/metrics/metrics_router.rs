use super::*;

/// Builds the `/metrics` exporter router. The admin server merges this with the
/// pprof routes through `serve_admin_from_env_with`. Do not merge
/// `pprof_router` here.
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}
