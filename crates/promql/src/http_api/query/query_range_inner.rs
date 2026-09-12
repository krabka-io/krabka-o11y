use super::{
    ApiError, Arc, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    QueryRequestTiming, RangeQueryParams, Response, TimeExt, acquire_query_permit,
    query_range_dispatch, query_timeout, record_query_response,
};

pub(crate) async fn query_range_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: RangeQueryParams,
) -> Response {
    let started = std::time::Instant::now();
    let timeout = match query_timeout(params.timeout.as_deref(), state.query_timeout) {
        Ok(timeout) => timeout,
        Err(error) => return error.into_response(),
    };
    let outcome = tokio::time::timeout(timeout.to_std(), async {
        let queue_started = std::time::Instant::now();
        let _query_permit = acquire_query_permit(&state).await;
        let queue = queue_started.elapsed();
        let _active = state.active_query_guard();
        query_range_dispatch(
            &state,
            &headers,
            &principal,
            params,
            QueryRequestTiming { started, queue },
        )
        .await
    })
    .await;
    let response = match outcome {
        Ok(response) => response,
        Err(_) => ApiError::timeout("query timed out").into_response(),
    };
    record_query_response(&state, "query_range", &response, started);
    response
}
