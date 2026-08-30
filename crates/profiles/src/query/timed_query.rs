use super::*;

/// Times `fut`, a Connect handler body, and records the outcome on `route`.
///
/// `Ok` gives `status="ok"` and any `ConnectError` gives `status="error"`. The
/// function observes the latency for every outcome.
pub(crate) async fn timed_query<T>(
    metrics: &ServiceMetrics,
    route: &str,
    fut: impl Future<Output = Result<T, ConnectError>>,
) -> Result<T, ConnectError> {
    let start = std::time::Instant::now();
    let result = fut.await;
    metrics.record_query(route, result.is_ok(), start.elapsed().as_time());
    result
}
