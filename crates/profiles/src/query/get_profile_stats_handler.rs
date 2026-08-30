use super::*;

pub(crate) async fn get_profile_stats_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::GetProfileStatsRequest>,
) -> Result<ConnectResponse<pb::querier::v1::GetProfileStatsResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "profile_stats",
        get_profile_stats_inner(state, headers, req),
    )
    .await
}
