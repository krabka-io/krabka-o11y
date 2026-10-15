use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    Json,
    body::Bytes,
    extract::{ConnectInfo, Path, RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::TenantId;
use krabka_observability::{
    audit::{
        AuditOutcome, AuditResource, OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET,
        OPERATION_RULE_NAMESPACE_DELETE, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE,
        RESOURCE_TENANT, audit_principal_of, resource, source_endpoint, unknown_source_endpoint,
    },
    server_security::PeerAddr,
};
use krabka_units::prelude::*;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;

use super::{
    AlertStateKey, ApiError, AuditHandle, Extension, Principal, PrometheusApiState, Rejection,
    RequestAuth, RulesParams, authorized_tenant_from_headers, response_with_status, sample_string,
    success_data_response,
};
use crate::{MetricStore, PromqlError, QueryResult, SampleValue, parse_promql};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuleTypeFilter {
    Any,
    Alert,
    Record,
}

impl RuleTypeFilter {
    pub(crate) fn from_param(value: Option<&str>) -> Self {
        match value {
            Some("alert") => Self::Alert,
            Some("record") => Self::Record,
            _ => Self::Any,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RuleRenderOptions {
    pub(crate) type_filter: RuleTypeFilter,
    pub(crate) exclude_alerts: bool,
}

#[derive(serde::Deserialize)]
pub(super) struct RuleGroupPath {
    pub(super) namespace: String,
    pub(super) group_name: String,
}

pub(super) struct RulerConfigRequest<S: MetricStore> {
    state: axum::extract::State<Arc<PrometheusApiState<S>>>,
    principal: Extension<Principal>,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    headers: HeaderMap,
    namespace: Path<String>,
}

impl<S: MetricStore> axum::extract::FromRequestParts<Arc<PrometheusApiState<S>>>
    for RulerConfigRequest<S>
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        router_state: &Arc<PrometheusApiState<S>>,
    ) -> Result<Self, Self::Rejection> {
        let state = State::from_request_parts(parts, router_state)
            .await
            .map_err(IntoResponse::into_response)?;
        let principal = Extension::from_request_parts(parts, router_state)
            .await
            .map_err(IntoResponse::into_response)?;
        let peer =
            Option::<Extension<ConnectInfo<PeerAddr>>>::from_request_parts(parts, router_state)
                .await
                .map_err(IntoResponse::into_response)?;
        let headers = HeaderMap::from_request_parts(parts, router_state)
            .await
            .map_err(IntoResponse::into_response)?;
        let namespace = Path::from_request_parts(parts, router_state)
            .await
            .map_err(IntoResponse::into_response)?;

        Ok(Self {
            state,
            principal,
            peer,
            headers,
            namespace,
        })
    }
}

#[allow(clippy::result_large_err)]
pub(super) fn authorize_config_tenant(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantId, Response> {
    authorized_tenant_from_headers(headers, principal).map_err(IntoResponse::into_response)
}
mod alerts;
mod endpoints;
mod config_ops;
mod json_builders;
mod ruler_config_rules;
mod tenant_ruler_rules;
pub(super) use alerts::alerts;
pub(super) use endpoints::{delete_ruler_config_group, delete_ruler_config_namespace, ruler_config_group, rules, set_ruler_config_group};
use config_ops::{parse_rules_params, parse_yaml_duration, record_ruler_config_change, store_ruler_config_group};
use json_builders::{labels_map_json, prometheus_alerts_for_rule_json, prometheus_rule_groups_json, prometheus_rule_json, prometheus_rules_json};
pub(super) use ruler_config_rules::ruler_config_rules;
pub(super) use tenant_ruler_rules::tenant_ruler_rules;

pub(super) use crate::yaml_optional_string;

async fn persist_ruler_config_or_restore<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    previous_rules: Option<super::RulerRuleStore>,
    mut response: Response,
) -> Response {
    if response.status().is_success()
        && let Err(error) = state.persist_ruler_config(tenant).await
    {
        if let Some(previous_rules) = previous_rules
            && let Ok(mut rules) = state.ruler_rules.write()
        {
            *rules = previous_rules;
        }
        response = response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist ruler config: {error}\n"),
        );
    }
    response
}

#[allow(clippy::result_large_err)]
fn authorized_ruler_group_tenant(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantId, Response> {
    authorized_tenant_from_headers(headers, principal).map_err(IntoResponse::into_response)
}

macro_rules! authorized_ruler_group_tenant_or_return {
    ($headers:expr, $principal:expr, $authorize:path) => {
        match $authorize($headers, $principal) {
            Ok(tenant) => tenant,
            Err(response) => return response,
        }
    };
}

pub(crate) use authorized_ruler_group_tenant_or_return;

pub(crate) fn rfc3339_time_string(ts_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts_ms) * 1_000_000).map_or_else(
        |_| zero_evaluation_time().to_string(),
        |time| {
            time.format(&Rfc3339)
                .unwrap_or_else(|_| zero_evaluation_time().to_string())
        },
    )
}

pub(crate) fn rule_group_name(group: &serde_yaml::Value) -> Result<String, ApiError> {
    group
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain a non-empty name"))
}

pub(crate) fn yaml_string(value: &serde_yaml::Value, key: &str) -> String {
    yaml_optional_string(value, key).unwrap_or_default()
}

pub(crate) fn zero_evaluation_time() -> &'static str {
    "0001-01-01T00:00:00Z"
}

pub(crate) fn accepted_response() -> Response {
    response_with_status(
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "success",
            "data": null,
            "errorType": "",
            "error": "",
        })),
    )
}

pub(crate) async fn prometheus_alerts_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
) -> Result<Vec<Value>, PromqlError> {
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let mut alerts = Vec::new();
    for namespace_groups in rules.into_values() {
        for group in namespace_groups.into_values() {
            if let Some(group_rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) {
                for rule in group_rules {
                    alerts.extend(
                        prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await?,
                    );
                }
            }
        }
    }
    Ok(alerts)
}

pub(crate) async fn ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let groups = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match groups {
        Some(groups) => yaml_response(StatusCode::OK, &groups.into_values().collect::<Vec<_>>()),
        None => yaml_response(
            StatusCode::OK,
            &std::collections::BTreeMap::<String, String>::new(),
        ),
    }
}

pub(crate) fn validate_rule(rule: &serde_yaml::Value) -> Result<(), ApiError> {
    let has_record = yaml_optional_string(rule, "record").is_some();
    let has_alert = yaml_optional_string(rule, "alert").is_some();
    if has_record == has_alert {
        return Err(ApiError::bad_data(
            "rule must contain exactly one of record or alert",
        ));
    }
    let expr = yaml_optional_string(rule, "expr")
        .filter(|expr| !expr.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule must contain a non-empty expr"))?;
    parse_promql(&expr)
        .map(|_| ())
        .map_err(|error| ApiError::bad_data(format!("rule PromQL expression is invalid: {error}")))
}

pub(crate) fn validate_rule_group(group: &serde_yaml::Value) -> Result<(), ApiError> {
    let rules = group
        .get("rules")
        .and_then(serde_yaml::Value::as_sequence)
        .filter(|rules| !rules.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain at least one rule"))?;
    for rule in rules {
        validate_rule(rule)?;
    }
    Ok(())
}

/// Returns the duration at a rule-file key as an extent.
///
/// The keys are `for:` and the `interval:` of a group. This function returns a
/// zero extent for an absent or unparseable value.
pub(crate) fn yaml_duration(value: &serde_yaml::Value, key: &str) -> Time {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .and_then(parse_yaml_duration)
        .unwrap_or(Time::ZERO)
}

pub(crate) fn yaml_mapping_json(value: &serde_yaml::Value, key: &str) -> Value {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map_or_else(
            || json!({}),
            |mapping| {
                let mut object = Map::new();
                for entry in mapping {
                    let Some(name) = entry.0.as_str() else {
                        continue;
                    };
                    object.insert(
                        name.to_string(),
                        Value::String(entry.1.as_str().unwrap_or_default().to_string()),
                    );
                }
                Value::Object(object)
            },
        )
}

pub(crate) fn yaml_response(status: StatusCode, value: &impl serde::Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(yaml) => {
            let mut response = response_with_status(status, yaml);
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/yaml"),
            );
            response
        }
        Err(error) => ApiError::internal(format!("YAML encode failed: {error}")).into_response(),
    }
}
