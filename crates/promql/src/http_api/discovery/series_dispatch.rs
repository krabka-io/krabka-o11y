use super::{
    ApiError, Arc, BTreeMap, DiscoveryParams, HeaderMap, IntoResponse, MetricStore, Principal,
    PrometheusApiState, Response, apply_limit, authorized_tenant_from_headers, discovery_matchers,
    discovery_window, enforce_query_range_limit, enforce_selected_series_limit, labels_json,
    labels_key, success_data_response,
};

pub(crate) async fn series_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    principal: &Principal,
    params: DiscoveryParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(headers, principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = enforce_query_range_limit(state, &tenant, window.start_ms, window.end_ms) {
        return error.into_response();
    }

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .series(tenant.as_str(), &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(series) => {
                for labels in series {
                    by_key.insert(labels_key(&labels), labels);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut series = by_key
        .into_values()
        .map(|labels| labels_json(&labels))
        .collect::<Vec<_>>();
    if let Err(error) = enforce_selected_series_limit(state, &tenant, series.len()) {
        return error.into_response();
    }
    apply_limit(&mut series, params.limit);
    success_data_response(series)
}
