use super::{ApiError, Arc, Bytes, HeaderMap, IntoResponse, MetricStore, Path, PrometheusApiState, Response, State, StatusCode, require_yaml_content_type, rule_group_name, tenant_from_headers, validate_rule_group};

pub(crate) async fn set_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_yaml_content_type(&headers) {
        return error.into_response();
    }
    let group: serde_yaml::Value = match serde_yaml::from_slice(&body) {
        Ok(group) => group,
        Err(error) => {
            return ApiError::bad_data(format!("rule group YAML decode failed: {error}"))
                .into_response();
        }
    };
    let group_name = match rule_group_name(&group) {
        Ok(name) => name,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_rule_group(&group) {
        return error.into_response();
    }

    match state.ruler_rules.write() {
        Ok(mut rules) => {
            rules
                .entry(tenant)
                .or_default()
                .entry(namespace)
                .or_default()
                .insert(group_name, group);
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}
