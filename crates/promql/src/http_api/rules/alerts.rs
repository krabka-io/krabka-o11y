use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RequestAuth, Response, State, json, prometheus_alerts_json, success_data_response,
    tenant_ruler_rules, tenant_ruler_rules::TenantRuleSnapshot,
};

pub(super) fn tenant_ruler_rules_for_request<S: MetricStore>(
    state: &PrometheusApiState<S>,
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantRuleSnapshot, Response> {
    tenant_ruler_rules(state, RequestAuth { headers, principal })
        .map_err(IntoResponse::into_response)
}

macro_rules! tenant_rules_or_return {
    ($state:expr, $headers:expr, $principal:expr) => {
        match $crate::http_api::rules::alerts::tenant_ruler_rules_for_request(
            $state, $headers, $principal,
        ) {
            Ok(tenant_rules) => tenant_rules,
            Err(response) => return response,
        }
    };
}

pub(super) use tenant_rules_or_return;

pub(crate) async fn alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let snapshot = tenant_rules_or_return!(&state, &headers, &principal);
    let alerts = match prometheus_alerts_json(&state, &snapshot.tenant, snapshot.rules).await {
        Ok(alerts) => alerts,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "alerts": alerts,
    }))
}
