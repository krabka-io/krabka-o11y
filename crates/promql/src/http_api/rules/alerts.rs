use super::{
    ApiError, Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State, json,
    prometheus_alerts_json, success_data_response, tenant_from_headers,
};

pub(crate) async fn alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let alerts = match prometheus_alerts_json(&state, &tenant, rules).await {
        Ok(alerts) => alerts,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "alerts": alerts,
    }))
}
