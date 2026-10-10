use super::{
    ConnectError, ConnectRequest, ConnectResponse, MetadataRequest, MetadataScope, ProfileStore,
    QuerierRequestParts, client_allows_utf8_label_names, connect_error, is_internal_label,
    is_legacy_label_name, metadata_scope, pb,
};

pub(crate) async fn label_names_inner<S>(
    QuerierRequestParts {
        state,
        principal,
        headers,
    }: QuerierRequestParts<S>,
    req: ConnectRequest<pb::querier::v1::LabelNamesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelNamesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let MetadataScope {
        tenant,
        matchers,
        range,
    } = metadata_scope(
        &state,
        MetadataRequest {
            principal: &principal,
            headers: &headers,
            matchers: &req.0.matchers,
            start_ms: req.0.start,
            end_ms: req.0.end,
        },
    )?;
    let mut names = state
        .store
        .label_names(tenant.as_str(), &matchers, range.start_ms, range.end_ms)
        .await
        .map_err(connect_error)?;
    let allow_utf8 = client_allows_utf8_label_names(&headers);
    names.retain(|name| !is_internal_label(name) && (allow_utf8 || is_legacy_label_name(name)));
    Ok(ConnectResponse::new(pb::querier::v1::LabelNamesResponse {
        names,
    }))
}
