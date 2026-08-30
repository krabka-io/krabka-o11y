use super::{Arc, HeaderMap, InstantQueryParams, MetricStore, PrometheusApiState, Response, acquire_query_permit, query_dispatch, record_query_response};

pub(crate) async fn query_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: InstantQueryParams,
) -> Response {
    let started = std::time::Instant::now();
    let _query_permit = acquire_query_permit(&state).await;
    // Held across dispatch so `active_queries` reflects queries admitted past
    // the concurrency gate and now executing; decremented on drop.
    let _active = state.active_query_guard();
    let response = query_dispatch(&state, &headers, params).await;
    record_query_response(&state, "query", &response, started);
    response
}
