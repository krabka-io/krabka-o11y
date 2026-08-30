use super::*;

pub(crate) async fn label_names_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelNamesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelNamesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let matchers = parse_matchers(&req.0.matchers).map_err(connect_error)?;
    let range = MetadataRange::from_request(req.0.start, req.0.end)
        .validate(&state, &tenant)
        .map_err(connect_error)?;
    let mut names = state
        .store
        .label_names(&tenant, &matchers, range.start_ms, range.end_ms)
        .await
        .map_err(connect_error)?;
    names.retain(|name| !is_internal_label(name));
    Ok(ConnectResponse::new(pb::querier::v1::LabelNamesResponse {
        names,
    }))
}
