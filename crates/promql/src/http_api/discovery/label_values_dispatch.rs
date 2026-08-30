use super::{ApiError, Arc, BTreeMap, DiscoveryParams, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, apply_limit, discovery_matchers, discovery_window, success_data_response, tenant_from_headers};

pub(crate) async fn label_values_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    name: String,
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

    let mut values = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_values(&tenant, &name, &matchers, window.start_ms, window.end_ms)
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
    apply_limit(&mut values, params.limit);
    success_data_response(values)
}
