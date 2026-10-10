use super::{
    ConnectError, HeaderMap, LabelMatcher, MetadataRange, Principal, ProfileStore, QuerierState,
    TenantId, authorize_tenant, connect_error, parse_matchers, tenant_connect_error,
    tenant_denied_connect_error, tenant_from_headers,
};

/// What a label-metadata request reads: its authorized tenant, its parsed
/// matchers, and its validated time range.
pub(crate) struct MetadataScope {
    pub(crate) tenant: TenantId,
    pub(crate) matchers: Vec<LabelMatcher>,
    pub(crate) range: MetadataRange,
}

/// Resolves and authorizes the request's tenant, then parses `matchers` and
/// validates the `start`/`end` range of a label-metadata request.
pub(crate) fn metadata_scope<S: ProfileStore>(
    state: &QuerierState<S>,
    principal: &Principal,
    headers: &HeaderMap,
    matchers: &[String],
    (start_ms, end_ms): (i64, i64),
) -> Result<MetadataScope, ConnectError> {
    let tenant = tenant_from_headers(headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    let matchers = parse_matchers(matchers).map_err(connect_error)?;
    let range = MetadataRange::from_request(start_ms, end_ms)
        .validate(state, &tenant)
        .map_err(connect_error)?;
    Ok(MetadataScope {
        tenant,
        matchers,
        range,
    })
}
