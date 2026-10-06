use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileStore, QuerierState, authorize_tenant, connect_error, merge_profile_type_selector,
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
    // The pinned v2 query frontend exposes this diagnostic as an empty stub.
    if state.query_architecture == super::PyroscopeQueryArchitecture::V2 {
        return Ok(ConnectResponse::new(
            pb::querier::v1::AnalyzeQueryResponse::default(),
        ));
    }
    let req = req.0;
    // The pinned public frontend returns an empty diagnostic before parsing
    // the selector whenever either bound is omitted (frontend_analyze_query.go).
    if req.start == 0 || req.end == 0 {
        return Ok(ConnectResponse::new(
            pb::querier::v1::AnalyzeQueryResponse::default(),
        ));
    }
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let (profile_type, selector) =
        if !state.query_analysis_series_enabled || req.query.trim().is_empty() {
            (String::new(), "{}".to_string())
        } else if req.query.trim().starts_with('{') {
            (String::new(), req.query.clone())
        } else {
            let (profile_type, selector) = parse_render_query(&req.query).map_err(connect_error)?;
            let selector =
                merge_profile_type_selector(&selector, &profile_type).map_err(connect_error)?;
            (profile_type, selector)
        };
    let matchers = parse_label_selector(&selector).map_err(connect_error)?;
    let query_stats = state
        .store
        .query_stats(
            tenant.as_str(),
            &profile_type,
            &matchers,
            req.start,
            req.end,
        )
        .await
        .map_err(connect_error)?;
    let series_count = if state.query_analysis_series_enabled {
        query_stats.fingerprints.len() as u64
    } else {
        0
    };
    let mut scopes = vec![
        pb::querier::v1::QueryScope {
            component_type: "Short term storage".into(),
            ..Default::default()
        },
        pb::querier::v1::QueryScope {
            component_type: "Long term storage".into(),
            ..Default::default()
        },
    ];
    for scope in query_stats.scopes {
        let destination = if scope.component_type == "Short term storage" {
            &mut scopes[0]
        } else {
            &mut scopes[1]
        };
        destination.component_count = destination
            .component_count
            .saturating_add(scope.component_count);
        destination.block_count = destination.block_count.saturating_add(scope.block_count);
        destination.series_count = destination.series_count.saturating_add(scope.series_count);
        destination.profile_count = destination
            .profile_count
            .saturating_add(scope.profile_count);
        destination.sample_count = destination.sample_count.saturating_add(scope.sample_count);
        destination.index_bytes = destination.index_bytes.saturating_add(scope.index_bytes);
        destination.profile_bytes = destination
            .profile_bytes
            .saturating_add(scope.profile_bytes);
        destination.symbol_bytes = destination.symbol_bytes.saturating_add(scope.symbol_bytes);
    }
    let total_bytes = scopes.iter().fold(0_u64, |total, scope| {
        total
            .saturating_add(scope.index_bytes)
            .saturating_add(scope.profile_bytes)
            .saturating_add(scope.symbol_bytes)
    });
    let response = pb::querier::v1::AnalyzeQueryResponse {
        query_scopes: scopes,
        query_impact: Some(pb::querier::v1::QueryImpact {
            total_bytes_in_time_range: total_bytes,
            total_queried_series: series_count,
            deduplication_needed: query_stats.deduplication_needed,
        }),
    };
    Ok(ConnectResponse::new(response))
}
