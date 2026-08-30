use super::{MetricStore, Arc, PrometheusApiState, HeaderMap, RangeQueryParams, Response, acquire_query_permit, query_range_dispatch, record_query_response};

pub(crate) async fn query_range_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: RangeQueryParams,
) -> Response {
    let started = std::time::Instant::now();
    let _query_permit = acquire_query_permit(&state).await;
    let _active = state.active_query_guard();
    let response = query_range_dispatch(&state, &headers, params).await;
    record_query_response(&state, "query_range", &response, started);
    response
}
