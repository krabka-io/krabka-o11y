//! Prometheus/Mimir-compatible HTTP query API adapter.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use krabka_metrics::{LimitError, OverridesProvider, wire::WireError};
use krabka_units::prelude::*;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    EngineOpts, MetricStore, PromqlEngine, PromqlError,
    metrics::ServiceMetrics,
    query_frontend::{QueryFrontendCache, QueryFrontendOptions, RangeQueryCache},
    ruler::{RulerAlertStateRecord, RulerGroupState, RulerGroupStateRecord},
};

mod alert_templates;
mod cardinality;
mod discovery;
mod metadata;
mod parse;
mod query;
mod remote_read;
mod request;
mod response;
mod rules;
mod status;

pub(crate) use alert_templates::expand_alert_template;
use cardinality::{
    cardinality_active_series, cardinality_active_series_post, cardinality_label_names,
    cardinality_label_names_post, cardinality_label_values, cardinality_label_values_post,
};
use discovery::{label_values, label_values_post, labels, labels_post, series, series_post};
use metadata::{metadata, target_metadata};
use parse::{format_query, format_query_post, parse_query, parse_query_post};
use query::{
    query, query_exemplars, query_exemplars_post, query_post, query_range, query_range_post,
};
use remote_read::remote_read;
use request::{
    CardinalityParams, DiscoveryParams, apply_limit, apply_result_limit, check_range_resolution,
    discovery_matchers, discovery_window, duration_param, enforce_sample_count,
    enforce_selected_series_limit, optional_timestamp_ms, parse_cardinality_form,
    parse_cardinality_params, parse_discovery_form, parse_discovery_params, parse_limit_parameter,
    required_form_param, selector_matchers, tenant_from_headers, timestamp_ms, unix_now_ms,
    validate_timestamp_range,
};
pub(crate) use response::format_sample_value;
use response::{
    active_series_response, cardinality_label_names_response, cardinality_label_values_response,
    exemplar_key, exemplars_json, labels_json, labels_key, sample_string, success_data_response,
    success_response,
};
use rules::{
    alerts, delete_ruler_config_group, delete_ruler_config_namespace, ruler_config_group,
    ruler_config_namespace, ruler_config_rules, rules, set_ruler_config_group,
};
use status::{
    alertmanagers, build_info, runtime_info, scrape_pools, status_config, status_flags, targets,
    tsdb_blocks, tsdb_status, wal_replay_status,
};

#[cfg(test)]
mod tests;

// === split-modules: generated submodules ===
mod acquire_query_permit;
mod active_query_guard;
mod alert_state_key;
mod api_error;
mod prometheus_api_state;
mod prometheus_router;
mod query_frontend_state;
mod record_query_response;
mod ruler_alert_state_store;
mod ruler_rule_store;
mod rules_params;

use acquire_query_permit::acquire_query_permit;
use active_query_guard::ActiveQueryGuard;
use alert_state_key::AlertStateKey;
use api_error::ApiError;
pub use prometheus_api_state::PrometheusApiState;
pub use prometheus_router::prometheus_router;
use query_frontend_state::QueryFrontendState;
use record_query_response::record_query_response;
use ruler_alert_state_store::RulerAlertStateStore;
use ruler_rule_store::RulerRuleStore;
use rules_params::RulesParams;
