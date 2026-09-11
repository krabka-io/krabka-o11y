use super::{
    Arc, HeaderMap, MetricStore, Principal, PrometheusApiState, RangeQueryParams, Response,
    acquire_query_permit, query_range_dispatch, record_query_response,
};

pub(crate) async fn query_range_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: RangeQueryParams,
) -> Response {
    let started = std::time::Instant::now();
    let _query_permit = acquire_query_permit(&state).await;
    let _active = state.active_query_guard();
    let response = query_range_dispatch(&state, &headers, &principal, params).await;
    record_query_response(&state, "query_range", &response, started);
    response
}
