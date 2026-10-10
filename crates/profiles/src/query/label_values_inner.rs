use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, MetadataScope,
    Principal, ProfileStore, QuerierState, connect_error, is_internal_label, metadata_scope, pb,
};

pub(crate) async fn label_values_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelValuesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelValuesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let MetadataScope {
        tenant,
        matchers,
        range,
    } = metadata_scope(
        &state,
        &principal,
        &headers,
        &req.0.matchers,
        (req.0.start, req.0.end),
    )?;
    if is_internal_label(&req.0.name) {
        return Ok(ConnectResponse::new(pb::querier::v1::LabelValuesResponse {
            names: Vec::new(),
        }));
    }
    let names = state
        .store
        .label_values(
            tenant.as_str(),
            &req.0.name,
            &matchers,
            range.start_ms,
            range.end_ms,
        )
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::querier::v1::LabelValuesResponse {
        names,
    }))
}
