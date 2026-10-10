use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RequestAuth, Response, State, json, prometheus_alerts_json, success_data_response,
    tenant_ruler_rules,
};

pub(crate) async fn alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let (tenant, rules) = match tenant_ruler_rules(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
    ) {
        Ok(tenant_rules) => tenant_rules,
        Err(rejection) => return rejection.into_response(),
    };
    let alerts = match prometheus_alerts_json(&state, &tenant, rules).await {
        Ok(alerts) => alerts,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "alerts": alerts,
    }))
}
