use super::*;

pub(crate) async fn series_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let matchers = parse_matchers(&req.0.matchers).map_err(connect_error)?;
    // An omitted range (start == end == 0) means "unbounded" — the Grafana
    // Profiles Drilldown enumerates series without a range. Match Pyroscope:
    // expand to the full range and skip the range-limit check (mirrors
    // `profile_types_inner`). Honoring [0, 0] literally filters out every row
    // and leaves the drilldown with no series to chart.
    let range = MetadataRange::from_request(req.0.start, req.0.end)
        .validate(&state, &tenant)
        .map_err(connect_error)?;
    let labels_set = state
        .store
        .series(
            &tenant,
            &matchers,
            &req.0.label_names,
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
