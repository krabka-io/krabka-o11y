use super::{
    Arc, MetricStore, PrometheusApiState, RangeQueryParams, RequestAuth, Response, TimedQuery,
    query_range_dispatch, run_timed_query,
};

pub(crate) async fn query_range_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: RangeQueryParams,
) -> Response {
    let timeout = params.timeout.clone();
    run_timed_query(
        state,
        TimedQuery {
            route: "query_range",
            timeout: timeout.as_deref(),
        },
        async |timing| {
            query_range_dispatch(state, auth.headers, auth.principal, params, timing).await
        },
    )
    .await
}
