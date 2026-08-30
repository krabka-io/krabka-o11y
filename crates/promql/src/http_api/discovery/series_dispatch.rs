use super::{
    ApiError, Arc, BTreeMap, DiscoveryParams, HeaderMap, IntoResponse, MetricStore,
    PrometheusApiState, Response, apply_limit, discovery_matchers, discovery_window,
    enforce_selected_series_limit, labels_json, labels_key, success_data_response,
    tenant_from_headers,
};

pub(crate) async fn series_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
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

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .series(&tenant, &matchers, window.start_ms, window.end_ms)
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
