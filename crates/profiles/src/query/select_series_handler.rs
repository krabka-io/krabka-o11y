use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, pb, select_series_inner, timed_query,
};

pub(crate) async fn select_series_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectSeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_series",
        select_series_inner(state, headers, req),
    )
    .await
}
