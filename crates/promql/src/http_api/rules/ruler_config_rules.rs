use super::{ApiError, Arc, BTreeMap, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State, StatusCode, tenant_from_headers, yaml_response};

pub(crate) async fn ruler_config_rules<S: MetricStore>(
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
    let rules = rules
        .into_iter()
        .map(|(namespace, groups)| (namespace, groups.into_values().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();
    yaml_response(StatusCode::OK, &rules)
}
