use super::{
    ConnectError, ConnectQueryRequest, ConnectResponse, ProfileStore, connect_error,
    parse_span_selectors, pb,
};

pub(crate) async fn select_merge_span_profile_inner<S>(
    request: ConnectQueryRequest<S, pb::querier::v1::SelectMergeSpanProfileRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeSpanProfileResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = request.authorized_tenant()?;
    let state = request.state.0;
    let req = request.request.0;
    let span_ids = parse_span_selectors(&req.span_selector).map_err(connect_error)?;
    let max_nodes = req.max_nodes.map_or(0, |count| {
        if count > 0 {
            count.saturating_add(1)
        } else {
            count
        }
    });
    let query_target = super::QueryTarget::new(&tenant, &req.profile_type_id, &req.label_selector);
    let query_range = super::QueryRange::new(req.start, req.end);
    let response = if req.format == pb::querier::v1::ProfileFormat::Tree as i32 {
        let tree = state
            .select_merge_span_profile_tree(query_target, &span_ids, query_range, max_nodes)
            .await
            .map_err(connect_error)?;
        pb::querier::v1::SelectMergeSpanProfileResponse {
            flamegraph: None,
            tree,
        }
    } else {
        let flamegraph = state
            .select_merge_span_profile(query_target, &span_ids, query_range, max_nodes)
            .await
            .map_err(connect_error)?;
        pb::querier::v1::SelectMergeSpanProfileResponse {
            flamegraph: Some(flamegraph.into()),
            tree: Vec::new(),
        }
    };
    Ok(ConnectResponse::new(response))
}
