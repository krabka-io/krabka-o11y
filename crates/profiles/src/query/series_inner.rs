use super::{
    ConnectError, ConnectRequest, ConnectResponse, MetadataRequest, MetadataScope, ProfileStore,
    QuerierRequestParts, client_allows_utf8_label_names, connect_error, is_internal_label,
    is_legacy_label_name, label_pairs, metadata_scope, pb,
};

pub(crate) async fn series_inner<S>(
    QuerierRequestParts {
        state,
        principal,
        headers,
    }: QuerierRequestParts<S>,
    req: ConnectRequest<pb::querier::v1::SeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    // An omitted range (start == end == 0) means "unbounded" — the Grafana
    // Profiles Drilldown enumerates series without a range. Match Pyroscope:
    // expand to the full range and skip the range-limit check (mirrors
    // `profile_types_inner`). Honoring [0, 0] literally filters out every row
    // and leaves the drilldown with no series to chart.
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
    let mut label_names = req.0.label_names.clone();
    if label_names.is_empty() {
        label_names = state
            .store
            .label_names(tenant.as_str(), &matchers, range.start_ms, range.end_ms)
            .await
            .map_err(connect_error)?;
        label_names.retain(|name| !is_internal_label(name));
    }
    if !client_allows_utf8_label_names(&headers) {
        label_names.retain(|name| is_legacy_label_name(name));
    }
    let labels_set = state
        .store
        .series(
            tenant.as_str(),
            &matchers,
            &label_names,
            range.start_ms,
            range.end_ms,
        )
        .await
        .map_err(connect_error)?
        .into_iter()
        .map(|labels| pb::querier::v1::Labels {
            labels: label_pairs(labels),
        })
        .collect();
    Ok(ConnectResponse::new(pb::querier::v1::SeriesResponse {
        labels_set,
    }))
}
