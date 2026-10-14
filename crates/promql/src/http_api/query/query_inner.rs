use super::{
    Arc, InstantQueryParams, MetricStore, PrometheusApiState, QueryDispatchRequest, RequestAuth,
    Response, TimedQuery, query_dispatch, run_timed_query,
};

pub(crate) async fn query_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: InstantQueryParams,
) -> Response {
    let timeout = params.timeout.clone();
    run_timed_query(
        state,
        TimedQuery {
            route: "query",
            timeout: timeout.as_deref(),
        },
        async |timing| {
            query_dispatch(QueryDispatchRequest {
                state,
                headers: auth.headers,
                principal: auth.principal,
                params,
                timing,
            })
            .await
        },
    )
    .await
}
