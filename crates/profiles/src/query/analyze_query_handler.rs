use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, analyze_query_inner, pb, timed_query,
};

pub(crate) async fn analyze_query_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::AnalyzeQueryRequest>,
) -> Result<ConnectResponse<pb::querier::v1::AnalyzeQueryResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "analyze_query",
        analyze_query_inner(state, headers, req),
    )
    .await
}
