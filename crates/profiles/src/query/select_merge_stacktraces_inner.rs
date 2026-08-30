use super::*;

pub(crate) async fn select_merge_stacktraces_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeStacktracesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeStacktracesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let label_selector = merge_profile_id_selector(&req.label_selector, &req.profile_id_selector)
        .map_err(connect_error)?;
    let stack_trace_call_sites =
        stack_trace_call_sites_from_json(&req.stack_trace_selector).map_err(connect_error)?;
    let response = match req.format {
        format if format == pb::querier::v1::ProfileFormat::Tree as i32 => {
            let tree = state
                .select_merge_stacktraces_tree_with_stack_trace_selector(
                    (&tenant, &req.profile_type_id, &label_selector),
                    (req.start, req.end),
                    req.max_nodes,
                    &stack_trace_call_sites,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: None,
                tree,
                dot: String::new(),
            }
        }
        format if format == pb::querier::v1::ProfileFormat::Dot as i32 => {
            let flamegraph = state
                .select_merge_stacktraces_with_stack_trace_selector(
                    (&tenant, &req.profile_type_id, &label_selector),
                    (req.start, req.end),
                    req.max_nodes,
                    &stack_trace_call_sites,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: None,
                tree: Vec::new(),
                dot: flamegraph_dot(&flamegraph),
            }
        }
        _ => {
            let flamegraph = state
                .select_merge_stacktraces_with_stack_trace_selector(
                    (&tenant, &req.profile_type_id, &label_selector),
                    (req.start, req.end),
                    req.max_nodes,
                    &stack_trace_call_sites,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: Some(flamegraph.into()),
                tree: Vec::new(),
                dot: String::new(),
            }
        }
    };
    Ok(ConnectResponse::new(response))
}
