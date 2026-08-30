use super::*;

pub(crate) async fn diff_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::DiffRequest>,
) -> Result<ConnectResponse<pb::querier::v1::DiffResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let left = req
        .0
        .left
        .ok_or_else(|| connect_error(ProfileError::Plan("missing left query".to_string())))?;
    let right = req
        .0
        .right
        .ok_or_else(|| connect_error(ProfileError::Plan("missing right query".to_string())))?;
    state
        .validate_query_range(&tenant, left.start, left.end)
        .map_err(connect_error)?;
    state
        .validate_query_range(&tenant, right.start, right.end)
        .map_err(connect_error)?;
    let left_label_selector =
        merge_profile_id_selector(&left.label_selector, &left.profile_id_selector)
            .map_err(connect_error)?;
    let right_label_selector =
        merge_profile_id_selector(&right.label_selector, &right.profile_id_selector)
            .map_err(connect_error)?;
    let left_call_sites =
        stack_trace_call_sites_from_json(&left.stack_trace_selector).map_err(connect_error)?;
    let right_call_sites =
        stack_trace_call_sites_from_json(&right.stack_trace_selector).map_err(connect_error)?;
    let max_nodes = state.effective_max_nodes(&tenant, left.max_nodes.max(right.max_nodes));
    let flamegraph = state
        .engine
        .diff_with_stack_trace_selector(
            &tenant,
            (
                &left.profile_type_id,
                &left_label_selector,
                left.start,
                left.end,
            ),
            (
                &right.profile_type_id,
                &right_label_selector,
                right.start,
                right.end,
            ),
            max_nodes,
            &left_call_sites,
            &right_call_sites,
        )
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::querier::v1::DiffResponse {
        flamegraph: Some(flamegraph.into()),
    }))
}
