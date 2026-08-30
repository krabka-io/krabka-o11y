use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, pb, profile_types_inner, timed_query,
};

pub(crate) async fn profile_types_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::ProfileTypesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::ProfileTypesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "profile_types",
        profile_types_inner(state, headers, req),
    )
    .await
}
