use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Message, Principal,
    ProfileError, ProfileStore, QuerierState, SampleSelector, authorize_tenant, connect_error,
    merge_profile_id_selector, parse_trace_selectors, pb, stack_trace_call_sites,
    tenant_connect_error, tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn select_merge_profile_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb::google::v1::Profile>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    let req = req.0;
    let label_selector = merge_profile_id_selector(&req.label_selector, &req.profile_id_selector)
        .map_err(connect_error)?;
    let stack_trace_call_sites = stack_trace_call_sites(req.stack_trace_selector.as_ref());
    let trace_ids = parse_trace_selectors(&req.trace_id_selector).map_err(connect_error)?;
    let sample_selector = if trace_ids.is_empty() {
        SampleSelector::None
    } else {
        SampleSelector::Trace(&trace_ids)
    };
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let max_nodes = state.effective_max_nodes(&tenant, req.max_nodes.unwrap_or_default());
    let profile = state
        .engine
        .select_merge_profile_with_selectors(
            (tenant.as_str(), &req.profile_type_id, &label_selector),
            (req.start, req.end),
            max_nodes,
            &stack_trace_call_sites,
            sample_selector,
        )
        .await
        .map_err(connect_error)?;
    let profile = pb::google::v1::Profile::decode(profile.as_slice())
        .map_err(|err| connect_error(ProfileError::Decode(err.to_string())))?;
    Ok(ConnectResponse::new(profile))
}
