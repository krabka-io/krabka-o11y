use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, pb, select_heatmap_inner, timed_query,
};

pub(crate) async fn select_heatmap_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectHeatmapRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectHeatmapResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_heatmap",
        select_heatmap_inner(state, headers, req),
    )
    .await
}
