use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::Labels;
use krabka_units::prelude::*;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;

use super::{
    AlertStateKey, ApiError, PrometheusApiState, RulesParams,
    alert_templates::{expand_alert_mapping_json, expand_alert_template, labels_from_map},
    sample_string, success_data_response, tenant_from_headers,
};
use crate::{MetricStore, PromqlError, QueryResult, SampleValue, parse_promql};

// === split-modules: generated submodules ===
mod alert_labels_map;
mod alerts;
mod delete_ruler_config_group;
mod delete_ruler_config_namespace;
mod labels_map_json;
mod parse_rules_params;
mod parse_yaml_duration;
mod prometheus_alerts_for_rule_json;
mod prometheus_alerts_json;
mod prometheus_rule_groups_json;
mod prometheus_rule_json;
mod prometheus_rules_json;
mod require_yaml_content_type;
mod rfc3339_time_string;
mod rule_group_name;
mod rule_render_options;
mod rule_type_filter;
mod ruler_config_group;
mod ruler_config_namespace;
mod ruler_config_rules;
mod rules;
mod set_ruler_config_group;
mod validate_rule;
mod validate_rule_group;
mod yaml_duration;
mod yaml_mapping_json;
mod yaml_optional_string;
mod yaml_response;
mod yaml_string;
mod zero_evaluation_time;

use alert_labels_map::alert_labels_map;
pub (super) use alerts::alerts;
pub (super) use delete_ruler_config_group::delete_ruler_config_group;
pub (super) use delete_ruler_config_namespace::delete_ruler_config_namespace;
use labels_map_json::labels_map_json;
use parse_rules_params::parse_rules_params;
use parse_yaml_duration::parse_yaml_duration;
use prometheus_alerts_for_rule_json::prometheus_alerts_for_rule_json;
use prometheus_alerts_json::prometheus_alerts_json;
use prometheus_rule_groups_json::prometheus_rule_groups_json;
use prometheus_rule_json::prometheus_rule_json;
use prometheus_rules_json::prometheus_rules_json;
use require_yaml_content_type::require_yaml_content_type;
use rfc3339_time_string::rfc3339_time_string;
use rule_group_name::rule_group_name;
use rule_render_options::RuleRenderOptions;
use rule_type_filter::RuleTypeFilter;
pub (super) use ruler_config_group::ruler_config_group;
pub (super) use ruler_config_namespace::ruler_config_namespace;
pub (super) use ruler_config_rules::ruler_config_rules;
pub (super) use rules::rules;
pub (super) use set_ruler_config_group::set_ruler_config_group;
use validate_rule::validate_rule;
use validate_rule_group::validate_rule_group;
use yaml_duration::yaml_duration;
use yaml_mapping_json::yaml_mapping_json;
use yaml_optional_string::yaml_optional_string;
use yaml_response::yaml_response;
use yaml_string::yaml_string;
use zero_evaluation_time::zero_evaluation_time;
