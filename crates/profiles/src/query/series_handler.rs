use super::*;

pub(crate) async fn series_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(&metrics, "series", series_inner(state, headers, req)).await
}
