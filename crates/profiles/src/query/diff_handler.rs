use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, diff_inner, pb, timed_query,
};

pub(crate) async fn diff_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::DiffRequest>,
) -> Result<ConnectResponse<pb::querier::v1::DiffResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(&metrics, "diff", diff_inner(state, headers, req)).await
}
