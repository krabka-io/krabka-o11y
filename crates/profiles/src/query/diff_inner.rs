use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileError, ProfileStore, QuerierState, SampleSelector, authorize_tenant, connect_error,
    merge_profile_id_selector, parse_span_selectors, parse_trace_selectors, pb,
    stack_trace_call_sites, tenant_connect_error, tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn diff_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::DiffRequest>,
) -> Result<ConnectResponse<pb::querier::v1::DiffResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
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
    let left_call_sites = stack_trace_call_sites(left.stack_trace_selector.as_ref());
    let right_call_sites = stack_trace_call_sites(right.stack_trace_selector.as_ref());
    let left_span_ids = parse_span_selectors(&left.span_selector).map_err(connect_error)?;
    let right_span_ids = parse_span_selectors(&right.span_selector).map_err(connect_error)?;
    let left_trace_ids = parse_trace_selectors(&left.trace_id_selector).map_err(connect_error)?;
    let right_trace_ids = parse_trace_selectors(&right.trace_id_selector).map_err(connect_error)?;
    let left_selector = sample_selector(&left_span_ids, &left_trace_ids).map_err(connect_error)?;
    let right_selector =
        sample_selector(&right_span_ids, &right_trace_ids).map_err(connect_error)?;
    let max_nodes = state.effective_max_nodes(
        &tenant,
        left.max_nodes.max(right.max_nodes).unwrap_or_default(),
    );
    let flamegraph = state
        .engine
        .diff_with_selectors(
            tenant.as_str(),
            (
                (
                    &left.profile_type_id,
                    &left_label_selector,
                    left.start,
                    left.end,
                ),
                &left_call_sites,
                left_selector,
            ),
            (
                (
                    &right.profile_type_id,
                    &right_label_selector,
                    right.start,
                    right.end,
                ),
                &right_call_sites,
                right_selector,
            ),
            max_nodes,
        )
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::querier::v1::DiffResponse {
        flamegraph: Some(flamegraph.into()),
    }))
}

fn sample_selector<'a>(
    span_ids: &'a [u64],
    trace_ids: &'a [Vec<u8>],
) -> Result<SampleSelector<'a>, ProfileError> {
    match (span_ids.is_empty(), trace_ids.is_empty()) {
        (false, false) => Err(ProfileError::Plan(
            "span_selector and trace_id_selector cannot be combined".to_string(),
        )),
        (false, true) => Ok(SampleSelector::Span(span_ids)),
        (true, false) => Ok(SampleSelector::Trace(trace_ids)),
        (true, true) => Ok(SampleSelector::None),
    }
}
