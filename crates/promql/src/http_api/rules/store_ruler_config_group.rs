use super::{
    ApiError, IntoResponse, MetricStore, PrometheusApiState, Response, StatusCode, TenantId,
    accepted_response, response_with_status, rule_group_name, validate_rule_group,
};

pub(crate) struct StoredRulerConfigGroup {
    pub(crate) response: Response,
    pub(crate) group_name: Option<String>,
}

/// Validates one posted rule group and stores it under `tenant` and `namespace`.
///
/// Returns the response, and the group name once the body names a group. The
/// handler records both in the audit trail.
pub(crate) fn store_ruler_config_group<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: TenantId,
    namespace: &str,
    body: &[u8],
) -> StoredRulerConfigGroup {
    let group: serde_yaml::Value = match serde_yaml::from_slice(body) {
        Ok(group) => group,
        Err(_) => {
            return StoredRulerConfigGroup {
                response: response_with_status(
                    StatusCode::BAD_REQUEST,
                    "unable to decode rule group\n",
                ),
                group_name: None,
            };
        }
    };
    let group_name = match rule_group_name(&group) {
        Ok(name) => name,
        Err(error) => {
            return StoredRulerConfigGroup {
                response: response_with_status(
                    StatusCode::BAD_REQUEST,
                    format!("{}\n", error.message),
                ),
                group_name: None,
            };
        }
    };
    if let Err(error) = validate_rule_group(&group) {
        return StoredRulerConfigGroup {
            response: response_with_status(StatusCode::BAD_REQUEST, format!("{}\n", error.message)),
            group_name: Some(group_name),
        };
    }

    let response = match state.ruler_rules.write() {
        Ok(mut rules) => {
            rules
                .entry(tenant)
                .or_default()
                .entry(namespace.to_owned())
                .or_default()
                .insert(group_name.clone(), group);
            accepted_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    StoredRulerConfigGroup {
        response,
        group_name: Some(group_name),
    }
}
