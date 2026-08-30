use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, ProfileStore,
    QuerierState, connect_error, parse_span_selectors, pb, tenant_from_headers,
};

pub(crate) async fn select_merge_span_profile_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeSpanProfileRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeSpanProfileResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let span_ids = parse_span_selectors(&req.span_selector).map_err(connect_error)?;
    let response = if req.format == pb::querier::v1::ProfileFormat::Tree as i32 {
        let tree = state
            .select_merge_span_profile_tree(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &span_ids,
                (req.start, req.end),
                req.max_nodes,
            )
            .await
            .map_err(connect_error)?;
        pb::querier::v1::SelectMergeSpanProfileResponse {
            flamegraph: None,
            tree,
        }
    } else {
        let flamegraph = state
            .select_merge_span_profile(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &span_ids,
                (req.start, req.end),
                req.max_nodes,
            )
            .await
            .map_err(connect_error)?;
        pb::querier::v1::SelectMergeSpanProfileResponse {
            flamegraph: Some(flamegraph.into()),
            tree: Vec::new(),
        }
    };
    Ok(ConnectResponse::new(response))
}
