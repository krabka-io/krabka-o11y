use super::*;

pub(crate) async fn select_merge_profile_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb::google::v1::Profile>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_merge_profile",
        select_merge_profile_inner(state, headers, req),
    )
    .await
}
