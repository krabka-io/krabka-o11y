use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileStore, QuerierState, pb, select_series_inner, timed_connect_query,
};

pub(crate) async fn select_series_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectSeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(
        state,
        principal,
        headers,
        req,
        "select_series",
        select_series_inner,
    )
    .await
}
