use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, pb, select_merge_stacktraces_inner, timed_query,
};

pub(crate) async fn select_merge_stacktraces_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeStacktracesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeStacktracesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_merge_stacktraces",
        select_merge_stacktraces_inner(state, headers, req),
    )
    .await
}
