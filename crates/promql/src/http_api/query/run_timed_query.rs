use super::{
    ApiError, Arc, IntoResponse, MetricStore, PrometheusApiState, QueryRequestTiming, Response,
    TimeExt, acquire_query_permit, query_timeout, record_query_response,
};

/// How one query request is timed and recorded.
pub(crate) struct TimedQuery<'a> {
    /// The route the response is recorded under.
    pub(crate) route: &'a str,
    /// The request's raw `timeout` parameter.
    pub(crate) timeout: Option<&'a str>,
}

/// Runs one query request under its `timeout` parameter and the query
/// concurrency gate, and records the response under its route.
///
/// `dispatch` evaluates the query once a permit is held. A request that runs
/// past its timeout answers with the Prometheus timeout error.
pub(crate) async fn run_timed_query<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    query: TimedQuery<'_>,
    dispatch: impl AsyncFnOnce(QueryRequestTiming) -> Response,
) -> Response {
    let TimedQuery { route, timeout } = query;
    let started = std::time::Instant::now();
    let timeout = match query_timeout(timeout, state.query_timeout) {
        Ok(timeout) => timeout,
        Err(error) => return error.into_response(),
    };
    let outcome = tokio::time::timeout(timeout.to_std(), async {
        let queue_started = std::time::Instant::now();
        let _query_permit = acquire_query_permit(state).await;
        let queue = queue_started.elapsed();
        // Held across dispatch so `active_queries` reflects queries admitted past
        // the concurrency gate and now executing; decremented on drop.
        let _active = state.active_query_guard();
        dispatch(QueryRequestTiming { started, queue }).await
    })
    .await;
    let response = match outcome {
        Ok(response) => response,
        Err(_) => ApiError::timeout("query timed out").into_response(),
    };
    record_query_response(state, route, &response, started);
    response
}
