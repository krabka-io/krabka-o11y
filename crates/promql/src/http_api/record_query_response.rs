use super::{Arc, MetricStore, PrometheusApiState, Response, StdDurationExt};

/// Records a query handler outcome from its final response status.
///
/// A response with a client or server error status, which is `>= 400`, counts
/// as `status="error"`.
pub(crate) fn record_query_response<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    route: &str,
    response: &Response,
    started: std::time::Instant,
) {
    let ok = !response.status().is_client_error() && !response.status().is_server_error();
    state.record_query(route, ok, started.elapsed().as_time());
}
