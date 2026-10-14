use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileStore, QuerierState, label_values_inner, pb, timed_connect_query,
};

pub(crate) async fn label_values_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelValuesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelValuesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(
        state,
        principal,
        headers,
        req,
        "label_values",
        label_values_inner,
    )
    .await
}
