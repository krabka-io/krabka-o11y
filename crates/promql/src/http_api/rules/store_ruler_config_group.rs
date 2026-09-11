use super::{
    ApiError, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, StatusCode,
    TenantId, require_yaml_content_type, rule_group_name, validate_rule_group,
};

/// Validates one posted rule group and stores it under `tenant` and `namespace`.
///
/// Returns the response, and the group name once the body names a group. The
/// handler records both in the audit trail.
pub(crate) fn store_ruler_config_group<S: MetricStore>(
    state: &PrometheusApiState<S>,
    headers: &HeaderMap,
    tenant: TenantId,
    namespace: &str,
    body: &[u8],
) -> (Response, Option<String>) {
    if let Err(error) = require_yaml_content_type(headers) {
        return (error.into_response(), None);
    }
    let group: serde_yaml::Value = match serde_yaml::from_slice(body) {
        Ok(group) => group,
        Err(error) => {
            return (
                ApiError::bad_data(format!("rule group YAML decode failed: {error}"))
                    .into_response(),
                None,
            );
        }
    };
    let group_name = match rule_group_name(&group) {
        Ok(name) => name,
        Err(error) => return (error.into_response(), None),
    };
    if let Err(error) = validate_rule_group(&group) {
        return (error.into_response(), Some(group_name));
    }

    let response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            rules
                .entry(tenant)
                .or_default()
                .entry(namespace.to_owned())
                .or_default()
                .insert(group_name.clone(), group);
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    (response, Some(group_name))
}
