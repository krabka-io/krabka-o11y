use super::{
    Arc, AsArray, BTreeSet, COL_FINGERPRINT, COL_TIMESTAMP, ConnectError, ConnectRequest,
    ConnectResponse, Extension, HeaderMap, Int64Type, Principal, ProfileError, ProfileStore,
    QuerierState, UInt64Type, authorize_tenant, connect_error, merge_profile_type_selector,
    parse_label_selector, parse_render_query, pb, tenant_connect_error,
    tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn analyze_query_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::AnalyzeQueryRequest>,
) -> Result<ConnectResponse<pb::querier::v1::AnalyzeQueryResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    let req = req.0;
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let (profile_type, selector) = parse_render_query(&req.query).map_err(connect_error)?;
    let selector = merge_profile_type_selector(&selector, &profile_type).map_err(connect_error)?;
    let matchers = parse_label_selector(&selector).map_err(connect_error)?;
    let scan = state
        .store
        .select(
            tenant.as_str(),
            &profile_type,
            &matchers,
            req.start,
            req.end,
        )
        .await
        .map_err(connect_error)?;
    let batches = scan
        .ctx
        .sql(&format!(
            "SELECT {COL_FINGERPRINT}, {COL_TIMESTAMP} FROM {}",
            scan.samples_table
        ))
        .await
        .map_err(|error| connect_error(ProfileError::Plan(error.to_string())))?
        .collect()
        .await
        .map_err(|error| connect_error(ProfileError::Exec(error.to_string())))?;
    let mut series = BTreeSet::new();
    let mut profiles = BTreeSet::new();
    let mut sample_count = 0_u64;
    for batch in batches {
        let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        sample_count = sample_count.saturating_add(batch.num_rows() as u64);
        for row in 0..batch.num_rows() {
            series.insert(fingerprints.value(row));
            profiles.insert((fingerprints.value(row), timestamps.value(row)));
        }
    }
    let series_count = series.len() as u64;
    let profile_count = profiles.len() as u64;
    let has_data = sample_count > 0;
    let response = pb::querier::v1::AnalyzeQueryResponse {
        query_scopes: vec![pb::querier::v1::QueryScope {
            component_type: "Long term storage".to_string(),
            component_count: u64::from(has_data),
            block_count: u64::from(has_data),
            series_count,
            profile_count,
            sample_count,
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
