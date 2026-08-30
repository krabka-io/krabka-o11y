use super::{Arc, DistributorState, Router, jaeger_push, otlp_push, post, zipkin_push};

/// Build the distributor HTTP router.
pub fn router(state: Arc<DistributorState>) -> Router {
    Router::new()
        .route("/v1/traces", post(otlp_push))
        .route("/api/push", post(otlp_push))
        .route("/api/v2/spans", post(zipkin_push))
        .route("/api/traces", post(jaeger_push))
        .with_state(state)
}
