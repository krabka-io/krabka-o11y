use super::*;

pub(crate) async fn select_merge_profile_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb::google::v1::Profile>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let label_selector = merge_profile_id_selector(&req.label_selector, &req.profile_id_selector)
        .map_err(connect_error)?;
    let stack_trace_call_sites = stack_trace_call_sites(req.stack_trace_selector.as_ref());
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let max_nodes = state.effective_max_nodes(&tenant, req.max_nodes);
    let profile = state
        .engine
        .select_merge_profile_with_max_nodes_and_stack_trace_selector(
            (&tenant, &req.profile_type_id, &label_selector),
            (req.start, req.end),
            max_nodes,
            &stack_trace_call_sites,
        )
        .await
        .map_err(connect_error)?;
    let profile = pb::google::v1::Profile::decode(profile.as_slice())
        .map_err(|err| connect_error(ProfileError::Decode(err.to_string())))?;
    Ok(ConnectResponse::new(profile))
}
