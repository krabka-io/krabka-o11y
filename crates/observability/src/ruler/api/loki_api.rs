use axum::response::IntoResponse;

use super::{
    loki_yaml_mapping, prometheus_alerts_response, prometheus_rule_groups_response, serde_yaml_key,
    yaml_string_field,
};
use crate::{
    BTreeMap, BTreeSet, Bytes, HeaderMap, HttpQueryError, LokiRuleNamespaces, Path, QuerierState,
    RawQuery, Response, Serialize, State, StatusCode, StreamQuery, current_unix_time_ns, json,
    json_response, text_response,
};

// === split-modules: generated submodules ===
mod create_loki_rule_group;
mod delete_loki_rule_group;
mod delete_loki_rule_namespace;
mod loki_page_not_found;
mod loki_rule_group;
mod loki_rule_group_name;
mod loki_rule_namespace;
mod loki_rule_namespace_response;
mod loki_ruler_tenant;
mod loki_rules;
mod loki_yaml_response;
mod missing_loki_rule_directory_response;
mod missing_loki_rule_namespace_response;
mod parse_loki_rule_group;
mod prometheus_alerts;
mod prometheus_rules;
mod prometheus_rules_filters;
mod ring_status_page;
mod ruler_status_page;
mod validate_loki_rule;
mod validate_loki_rule_group;

pub(crate) use create_loki_rule_group::create_loki_rule_group;
pub(crate) use delete_loki_rule_group::delete_loki_rule_group;
pub(crate) use delete_loki_rule_namespace::delete_loki_rule_namespace;
pub(crate) use loki_page_not_found::loki_page_not_found;
pub(crate) use loki_rule_group::loki_rule_group;
pub(crate) use loki_rule_group_name::loki_rule_group_name;
pub(crate) use loki_rule_namespace::loki_rule_namespace;
pub(crate) use loki_rule_namespace_response::loki_rule_namespace_response;
pub(crate) use loki_ruler_tenant::loki_ruler_tenant;
pub(crate) use loki_rules::loki_rules;
pub(crate) use loki_yaml_response::loki_yaml_response;
pub(crate) use missing_loki_rule_directory_response::missing_loki_rule_directory_response;
pub(crate) use missing_loki_rule_namespace_response::missing_loki_rule_namespace_response;
pub(crate) use parse_loki_rule_group::parse_loki_rule_group;
pub(crate) use prometheus_alerts::prometheus_alerts;
pub(crate) use prometheus_rules::prometheus_rules;
pub(crate) use prometheus_rules_filters::PrometheusRulesFilters;
pub(crate) use ring_status_page::ring_status_page;
pub(crate) use ruler_status_page::ruler_status_page;
pub(crate) use validate_loki_rule::validate_loki_rule;
pub(crate) use validate_loki_rule_group::validate_loki_rule_group;
