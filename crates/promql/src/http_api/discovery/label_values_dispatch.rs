use super::{
    ApiError, Arc, BTreeMap, DiscoveryParams, HeaderMap, IntoResponse, MetricStore, Principal,
    PrometheusApiState, Response, apply_limit, authorized_tenant_from_headers, discovery_matchers,
    discovery_window, enforce_query_range_limit, enforce_selected_series_limit,
    success_data_response,
};

pub(crate) async fn label_values_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    principal: &Principal,
    name: String,
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

    let mut values = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_values(
                tenant.as_str(),
                &name,
                &matchers,
                window.start_ms,
                window.end_ms,
            )
            .await
        {
            Ok(label_values) => {
                for value in label_values {
                    values.insert(value.clone(), value);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut values = values.into_values().collect::<Vec<_>>();
    if let Err(error) = enforce_selected_series_limit(state, &tenant, values.len()) {
        return error.into_response();
    }
    apply_limit(&mut values, params.limit);
    success_data_response(values)
}
