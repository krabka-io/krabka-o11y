use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileStore, QuerierState, authorize_tenant, connect_error, parse_span_selectors, pb,
    tenant_connect_error, tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn select_merge_span_profile_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeSpanProfileRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeSpanProfileResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
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
