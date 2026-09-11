use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, MetadataRange,
    Principal, ProfileStore, QuerierState, authorize_tenant, connect_error, is_internal_label,
    parse_matchers, pb, tenant_connect_error, tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn label_names_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelNamesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelNamesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    let matchers = parse_matchers(&req.0.matchers).map_err(connect_error)?;
    let range = MetadataRange::from_request(req.0.start, req.0.end)
        .validate(&state, &tenant)
        .map_err(connect_error)?;
    let mut names = state
        .store
        .label_names(tenant.as_str(), &matchers, range.start_ms, range.end_ms)
        .await
        .map_err(connect_error)?;
    names.retain(|name| !is_internal_label(name));
    Ok(ConnectResponse::new(pb::querier::v1::LabelNamesResponse {
        names,
    }))
}
