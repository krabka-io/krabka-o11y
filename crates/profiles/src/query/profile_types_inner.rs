use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, MetadataRange,
    Principal, ProfileStore, ProfileType, QuerierState, connect_error, pb,
};

pub(crate) async fn profile_types_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::ProfileTypesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::ProfileTypesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = super::authorized_connect_tenant(&headers, &principal, &state.tenant_policy)?;
    let req = req.0;
    let range = MetadataRange::from_request(req.start, req.end)
        .validate(&state, &tenant)
        .map_err(connect_error)?;
    let types = state
        .store
        .profile_types(tenant.as_str(), range.start_ms, range.end_ms)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(
        pb::querier::v1::ProfileTypesResponse {
            profile_types: types
                .into_iter()
                .map(|id| {
                    ProfileType::parse(&id).map(|parsed| pb::querier::v1::ProfileType {
                        id,
                        name: parsed.name,
                        sample_type: parsed.sample_type,
                        sample_unit: parsed.sample_unit,
                        period_type: parsed.period_type,
                        period_unit: parsed.period_unit,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(connect_error)?,
        },
    ))
}
