use axum::response::IntoResponse;

use super::{
    loki_yaml_mapping, prometheus_alerts_response, prometheus_rule_groups_response, serde_yaml_key,
    yaml_string_field,
};
use crate::{
    BTreeMap, BTreeSet, Bytes, HeaderMap, HttpQueryError, LokiRuleNamespaces, LokiRuleTenants,
    Path, QuerierState, RawQuery, RequestSecurity, Response, Serialize, State, StatusCode,
    StreamQuery, TenantErrorSurface, TenantId, Value,
    audit::{
        AuditOutcome, AuditResource, OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET,
        OPERATION_RULE_NAMESPACE_DELETE, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE,
        RESOURCE_TENANT, resource,
    },
    authorized_tenant, current_unix_time_ns, json, json_response, text_response,
};

mod authorized_ruler_tenant;
mod create_loki_rule_group;
mod delete_loki_rule_group;
mod delete_loki_rule_namespace;
mod loki_page_not_found;
mod loki_rule_group;
mod loki_rule_group_name;
mod loki_rule_namespace;
mod loki_rule_namespace_response;
mod loki_rules;
mod loki_yaml_response;
mod missing_loki_rule_directory_response;
mod missing_loki_rule_namespace_response;
mod parse_loki_rule_group;
mod prometheus_alerts;
mod prometheus_ruler_request;
mod prometheus_rules;
mod prometheus_rules_filters;
mod ring_status_page;
mod ruler_status_page;
mod validate_loki_rule;
mod validate_loki_rule_group;

fn prometheus_ruler_success(data: Value) -> Response {
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": data,
            "errorType": "",
            "error": "",
        }),
    )
}

fn status_html_page(title: &str, body: &str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/html; charset=utf-8")
        .body(
            format!(
                "<!doctype html><html><head><title>{title}</title></head>\
                 <body><h1>{title}</h1>{body}</body></html>"
            )
            .into(),
        )
        .expect("status page headers are valid")
}

pub(crate) use authorized_ruler_tenant::{RulerTenant, authorized_ruler_tenant};
pub(crate) use create_loki_rule_group::create_loki_rule_group;
pub(crate) use delete_loki_rule_group::delete_loki_rule_group;
pub(crate) use delete_loki_rule_namespace::delete_loki_rule_namespace;
pub(crate) use loki_page_not_found::loki_page_not_found;
pub(crate) use loki_rule_group::loki_rule_group;
pub(crate) use loki_rule_group_name::loki_rule_group_name;
pub(crate) use loki_rule_namespace::loki_rule_namespace;
pub(crate) use loki_rule_namespace_response::loki_rule_namespace_response;
pub(crate) use loki_rules::loki_rules;
pub(crate) use loki_yaml_response::loki_yaml_response;
pub(crate) use missing_loki_rule_directory_response::missing_loki_rule_directory_response;
pub(crate) use missing_loki_rule_namespace_response::missing_loki_rule_namespace_response;
pub(crate) use parse_loki_rule_group::parse_loki_rule_group;
pub(crate) use prometheus_alerts::prometheus_alerts;
pub(crate) use prometheus_ruler_request::{PrometheusRulerRequest, prometheus_ruler_request};
pub(crate) use prometheus_rules::prometheus_rules;
pub(crate) use prometheus_rules_filters::PrometheusRulesFilters;
pub(crate) use ring_status_page::ring_status_page;
pub(crate) use ruler_status_page::ruler_status_page;
pub(crate) use validate_loki_rule::validate_loki_rule;
pub(crate) use validate_loki_rule_group::validate_loki_rule_group;

fn deleted_rule_response(
    state: &QuerierState,
    security: RequestSecurity,
    operation: &'static str,
    resources: Vec<AuditResource>,
    tenant: &str,
    snapshot: &LokiRuleTenants,
) -> Result<Response, HttpQueryError> {
    if let Err(error) = state.rules.persist_snapshot(snapshot) {
        security.admin_operation(operation, resources, AuditOutcome::Failure);
        return Err(HttpQueryError::from(error));
    }
    security.admin_operation(operation, resources, AuditOutcome::Success);
    state.alert_states.clear_tenant(tenant);
    Ok(json_response(
        StatusCode::ACCEPTED,
        &json!({ "status": "success" }),
    ))
}

fn snapshot_after_rule_removal(
    state: &QuerierState,
    tenant: &str,
    remove: impl FnOnce(&mut LokiRuleNamespaces) -> bool,
) -> Option<LokiRuleTenants> {
    let mut rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let namespaces = rules.get_mut(tenant)?;
    if !remove(namespaces) {
        return None;
    }
    if namespaces.is_empty() {
        rules.remove(tenant);
    }
    Some(rules.clone())
}

#[derive(serde::Deserialize)]
pub(crate) struct LokiRuleGroupPath {
    pub(crate) namespace: String,
    pub(crate) group_name: String,
}
