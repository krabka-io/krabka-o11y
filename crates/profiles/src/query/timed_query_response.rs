use krabka_observability::service_metrics::{QueryRequest, RequestOutcome};

use super::*;

/// Times `fut`, a raw axum handler body, and records the outcome on `route`.
///
/// `fut` returns a `Response`. A 2xx or 3xx status counts as `ok`. A 4xx or 5xx
/// status counts as `error`.
pub(crate) async fn timed_query_response(
    metrics: &ServiceMetrics,
    route: &str,
    fut: impl Future<Output = Response>,
) -> Response {
    let start = std::time::Instant::now();
    let response = fut.await;
    let status = response.status();
    let outcome = if status.is_success() || status.is_redirection() {
        RequestOutcome::Ok
    } else {
        RequestOutcome::Error
    };
    metrics.record_query(QueryRequest {
        route,
        outcome,
        elapsed: start.elapsed().as_time(),
    });
    response
}
