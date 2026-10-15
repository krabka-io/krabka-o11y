use super::*;

pub(crate) fn parse_rules_params(raw_query: Option<&str>) -> Result<RulesParams, ApiError> {
    let mut params = RulesParams::default();
    let mut bracket_rule_names = Vec::new();
    let mut bracket_rule_groups = Vec::new();
    let mut bracket_files = Vec::new();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for pair in form_urlencoded::parse(raw_query.as_bytes()) {
        let name = pair.0;
        let value = pair.1;
        match name.as_ref() {
            "type" => match value.to_ascii_lowercase().as_str() {
                kind @ ("alert" | "record") => params.rule_type = Some(kind.to_string()),
                _ => {
                    return Err(ApiError::bad_data(format!(
                        "not supported value {:?}",
                        value.as_ref()
                    )));
                }
            },
            "exclude_alerts" => {
                params.exclude_alerts = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid exclude_alerts parameter"))?,
                );
            }
            "rule_name" if !value.is_empty() => {
                params.rule_names.insert(value.into_owned());
            }
            "rule_name[]" if !value.is_empty() => bracket_rule_names.push(value.into_owned()),
            "rule_group" if !value.is_empty() => {
                params.rule_groups.insert(value.into_owned());
            }
            "rule_group[]" if !value.is_empty() => bracket_rule_groups.push(value.into_owned()),
            "file" if !value.is_empty() => {
                params.files.insert(value.into_owned());
            }
            "file[]" if !value.is_empty() => bracket_files.push(value.into_owned()),
            "group_limit" if !value.is_empty() => {
                let limit = value
                    .parse::<i32>()
                    .map_err(|_| ApiError::bad_data("invalid group limit value"))?;
                if limit < 0 {
                    return Err(ApiError::bad_data("invalid group limit value"));
                }
                params.group_limit = usize::try_from(limit).ok();
            }
            "group_next_token" => params.group_next_token = Some(value.into_owned()),
            _ => {}
        }
    }
    if !bracket_rule_names.is_empty() {
        params.rule_names = bracket_rule_names.into_iter().collect();
    }
    if !bracket_rule_groups.is_empty() {
        params.rule_groups = bracket_rule_groups.into_iter().collect();
    }
    if !bracket_files.is_empty() {
        params.files = bracket_files.into_iter().collect();
    }
    Ok(params)
}

struct DurationParts<'a> {
    amount: &'a str,
    unit_seconds: i64,
}

/// Parses a duration with an `s`, `m`, or `h` suffix, the suffixes this surface
/// accepts. A bare number is a count of seconds. The amount parses as `u64`, so
/// this function returns `None` for a negative or otherwise malformed value.
/// The caller reads `None` as "no duration", not as a backwards window.
pub(crate) fn parse_yaml_duration(value: &str) -> Option<Time> {
    let value = value.trim();
    let parts = if let Some(amount) = value.strip_suffix('s') {
        DurationParts {
            amount,
            unit_seconds: 1,
        }
    } else if let Some(amount) = value.strip_suffix('m') {
        DurationParts {
            amount,
            unit_seconds: 60,
        }
    } else if let Some(amount) = value.strip_suffix('h') {
        DurationParts {
            amount,
            unit_seconds: 60 * 60,
        }
    } else {
        DurationParts {
            amount: value,
            unit_seconds: 1,
        }
    };
    let amount = i64::try_from(parts.amount.parse::<u64>().ok()?).ok()?;
    Some(Time::from_secs(amount.saturating_mul(parts.unit_seconds)))
}

/// Records one ruler config mutation in the audit trail, with the outcome of `response`.
///
/// A handler calls it only after the principal may use the tenant. The
/// server's security events already record a refused principal, so a 403
/// does not reach this function. `peer` is `None` for a request that did not
/// arrive through a listener, and the event then carries the unknown source
/// endpoint.
pub(crate) fn record_ruler_config_change(
    audit: &AuditHandle,
    principal: &Principal,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    operation: &'static str,
    resources: Vec<AuditResource>,
    response: &Response,
) {
    let source = peer.map_or_else(unknown_source_endpoint, |Extension(ConnectInfo(peer))| {
        source_endpoint(peer.socket)
    });
    let outcome = if response.status().is_success() {
        AuditOutcome::Success
    } else {
        AuditOutcome::Failure
    };
    audit.admin_operation(
        audit_principal_of(principal),
        source,
        operation,
        resources,
        outcome,
    );
}

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
