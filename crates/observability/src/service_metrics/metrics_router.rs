use super::SharedRegistry;

/// Encodes `registry` in the `OpenMetrics` text format.
///
/// # Errors
/// Returns the formatter error if a metric fails to encode.
pub async fn encode_registry(registry: &SharedRegistry) -> Result<String, std::fmt::Error> {
    let mut text = String::new();
    let guard = registry.lock().await;
    prometheus_client::encoding::text::encode(&mut text, &guard)?;
    Ok(text)
}

async fn export(
    axum::extract::State(registry): axum::extract::State<SharedRegistry>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    match encode_registry(&registry).await {
        Ok(text) => (
            axum::http::StatusCode::OK,
            [(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode: {e}"),
        )
            .into_response(),
    }
}

/// `/metrics` router that serves the `OpenMetrics` text encoding of
/// `registry`.
///
/// `serve_admin_from_env_with` merges it onto the admin port. It does NOT
/// include the pprof routes, which `serve_admin` adds. Do not merge
/// `pprof_router` here.
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}
