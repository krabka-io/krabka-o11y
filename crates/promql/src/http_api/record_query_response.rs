use krabka_observability::service_metrics::{QueryRequest, RequestOutcome};

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
    let status = response.status();
    let outcome = if status.is_client_error() || status.is_server_error() {
        RequestOutcome::Error
    } else {
        RequestOutcome::Ok
    };
    state.record_query(QueryRequest {
        route,
        outcome,
        elapsed: started.elapsed().as_time(),
    });
}
