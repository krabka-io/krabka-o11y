use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Message, Principal,
    ProfileError, ProfileStore, QuerierState, SampleSelector, apply_go_pgo, authorize_tenant,
    connect_error, merge_profile_id_selector, parse_span_selectors, parse_trace_selectors, pb,
    pprof_dot, stack_trace_call_sites, tenant_connect_error, tenant_denied_connect_error,
    tenant_from_headers,
};

pub(crate) async fn select_merge_stacktraces_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeStacktracesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeStacktracesResponse>, ConnectError>
where
    S: ProfileStore + 'static,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    let req = req.0;
    if state.query_architecture == super::PyroscopeQueryArchitecture::V2
        && state.async_queries_enabled
        && req.r#async.as_ref().is_some_and(|query| query.r#type != 0)
    {
        return super::async_stacktrace_query(state, tenant, req)
            .await
            .map(ConnectResponse::new);
    }
    execute_stacktrace_query(&state, &tenant, req)
        .await
        .map(ConnectResponse::new)
}

pub(crate) async fn execute_stacktrace_query<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &super::TenantId,
    req: pb::querier::v1::SelectMergeStacktracesRequest,
) -> Result<pb::querier::v1::SelectMergeStacktracesResponse, ConnectError> {
    let label_selector = merge_profile_id_selector(&req.label_selector, &req.profile_id_selector)
        .map_err(connect_error)?;
    let stack_trace_call_sites = stack_trace_call_sites(req.stack_trace_selector.as_ref());
    let span_ids = parse_span_selectors(&req.span_selector).map_err(connect_error)?;
    let trace_ids = parse_trace_selectors(&req.trace_id_selector).map_err(connect_error)?;
    let sample_selector = match (span_ids.is_empty(), trace_ids.is_empty()) {
        (false, false) => {
            return Err(connect_error(ProfileError::Plan(
                "span_selector and trace_id_selector cannot be combined".to_string(),
            )));
        }
        (false, true) => SampleSelector::Span(&span_ids),
        (true, false) => SampleSelector::Trace(&trace_ids),
        (true, true) => SampleSelector::None,
    };
    let max_nodes = req
        .max_nodes
        .map(|count| {
            if count > 0 {
                count.saturating_add(1)
            } else {
                count
            }
        })
        .unwrap_or_default();
    let response = match req.format {
        format if format == pb::querier::v1::ProfileFormat::Tree as i32 => {
            let tree = state
                .select_merge_stacktraces_tree_with_selectors(
                    (tenant, &req.profile_type_id, &label_selector),
                    (req.start, req.end),
                    max_nodes,
                    &stack_trace_call_sites,
                    sample_selector,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: None,
                tree,
                dot: String::new(),
                ..Default::default()
            }
        }
        format
            if format == pb::querier::v1::ProfileFormat::Dot as i32
                || format == pb::querier::v1::ProfileFormat::Pprof as i32 =>
        {
            state
                .validate_query_range(tenant, req.start, req.end)
                .map_err(connect_error)?;
            let bytes = state
                .engine
                .select_merge_profile_with_selectors(
                    (tenant.as_str(), &req.profile_type_id, &label_selector),
                    (req.start, req.end),
                    state.effective_max_nodes(tenant, max_nodes),
                    &stack_trace_call_sites,
                    sample_selector,
                )
                .await
                .map_err(connect_error)?;
            let mut profile = pb::google::v1::Profile::decode(bytes.as_slice())
                .map_err(|error| connect_error(ProfileError::Decode(error.to_string())))?;
            if format == pb::querier::v1::ProfileFormat::Dot as i32 {
                pb::querier::v1::SelectMergeStacktracesResponse {
                    dot: pprof_dot(&profile).map_err(connect_error)?,
                    ..Default::default()
                }
            } else {
                apply_go_pgo(&mut profile, req.stack_trace_selector.as_ref());
                pb::querier::v1::SelectMergeStacktracesResponse {
                    pprof: Some(pb::querier::v1::PprofProfile {
                        profile: Some(profile),
                    }),
                    ..Default::default()
                }
            }
        }
        _ => {
            let flamegraph = state
                .select_merge_stacktraces_with_selectors(
                    (tenant, &req.profile_type_id, &label_selector),
                    (req.start, req.end),
                    max_nodes,
                    &stack_trace_call_sites,
                    sample_selector,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: Some(flamegraph.into()),
                tree: Vec::new(),
                dot: String::new(),
                ..Default::default()
            }
        }
    };
    Ok(response)
}
