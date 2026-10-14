use super::{
    ConnectError, ConnectQueryRequest, ConnectResponse, Message, ProfileError, ProfileStore,
    QuerierState, SampleSelector, apply_go_pgo, connect_error, merge_profile_id_selector,
    parse_trace_selectors, pb, stack_trace_call_sites,
};

pub(crate) async fn select_merge_profile_inner<S>(
    request: ConnectQueryRequest<S, pb::querier::v1::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb::google::v1::Profile>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = request.authorized_tenant()?;
    let state = request.state.0;
    let req = request.request.0;
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
            krabka_pprof::ProfileSelection::new(
                tenant.as_str(),
                &req.profile_type_id,
                &label_selector,
            ),
            super::QueryRange::new(req.start, req.end),
            max_nodes,
            &stack_trace_call_sites,
            sample_selector,
        )
        .await
        .map_err(connect_error)?;
    let mut profile = pb::google::v1::Profile::decode(profile.as_slice())
        .map_err(|err| connect_error(ProfileError::Decode(err.to_string())))?;
    apply_go_pgo(&mut profile, req.stack_trace_selector.as_ref());
    Ok(ConnectResponse::new(profile))
}
