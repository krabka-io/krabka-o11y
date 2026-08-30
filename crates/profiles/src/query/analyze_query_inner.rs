use super::*;

pub(crate) async fn analyze_query_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::AnalyzeQueryRequest>,
) -> Result<ConnectResponse<pb::querier::v1::AnalyzeQueryResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let (profile_type, selector) = parse_render_query(&req.query).map_err(connect_error)?;
    let selector = merge_profile_type_selector(&selector, &profile_type).map_err(connect_error)?;
    let matchers = parse_label_selector(&selector).map_err(connect_error)?;
    let mut label_names = state
        .store
        .label_names(&tenant, &matchers, req.start, req.end)
        .await
        .map_err(connect_error)?;
    label_names.retain(|name| !is_internal_label(name));
    let series_count = state
        .store
        .series(&tenant, &matchers, &label_names, req.start, req.end)
        .await
        .map_err(connect_error)?
        .len() as u64;
    let response = pb::querier::v1::AnalyzeQueryResponse {
        query_scopes: vec![pb::querier::v1::QueryScope {
            component_type: "Long term storage".to_string(),
            component_count: u64::from(series_count > 0),
            block_count: 0,
            series_count,
            profile_count: 0,
            sample_count: 0,
            index_bytes: 0,
            profile_bytes: 0,
            symbol_bytes: 0,
        }],
        query_impact: Some(pb::querier::v1::QueryImpact {
            total_bytes_in_time_range: 0,
            total_queried_series: series_count,
            deduplication_needed: false,
        }),
    };
    Ok(ConnectResponse::new(response))
}
