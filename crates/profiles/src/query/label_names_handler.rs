use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileStore, QuerierState, label_names_inner, pb, timed_connect_query,
};

pub(crate) async fn label_names_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelNamesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelNamesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(
        state,
        principal,
        headers,
        req,
        "label_names",
        label_names_inner,
    )
    .await
}
