//! Prometheus/Mimir-compatible HTTP query API adapter.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use krabka_blockstore::{
    ERASURE_REQUEST_PREFIX, ErasureRequest, TenantId, has_erasure_requests, put_erasure_request,
};
use krabka_metrics::{
    LimitError, Limits, OverridesProvider, authorized_tenant_from_headers, wire::WireError,
};
use krabka_observability::{
    ReadinessGate,
    audit::AuditHandle,
    server_security::{Principal, authorize_admin},
};
use krabka_units::prelude::*;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    EngineOpts, MetricStore, PromqlEngine, PromqlError, WalHead,
    metrics::ServiceMetrics,
    query_frontend::{QueryFrontendCache, QueryFrontendOptions, RangeQueryCache},
    ruler::{RulerAlertStateRecord, RulerGroupState, RulerGroupStateRecord},
};

mod admin;
#[cfg(test)]
mod alert_templates;
mod cardinality;
mod discovery;
mod metadata;
mod mimir_alertmanager_router;
mod mimir_config_persistence;
mod mimir_ruler_router;
mod parse;
mod query;
mod remote_read;
mod request;
mod response;
mod rules;
mod search;
mod status;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RulesParams {
    #[serde(rename = "type")]
    pub(crate) rule_type: Option<String>,
    pub(crate) exclude_alerts: Option<bool>,
    pub(crate) rule_names: BTreeSet<String>,
    pub(crate) rule_groups: BTreeSet<String>,
    pub(crate) files: BTreeSet<String>,
    pub(crate) group_limit: Option<usize>,
    pub(crate) group_next_token: Option<String>,
}

pub(crate) fn parse_query_pairs(
    raw_query: Option<&str>,
    mut apply: impl FnMut(String, String) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    let Some(raw_query) = raw_query else {
        return Ok(());
    };
    for (name, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
        apply(name.into_owned(), value.into_owned())?;
    }
    Ok(())
}

fn request_content_type(headers: &HeaderMap) -> &str {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
}

pub(crate) fn response_with_status(status: StatusCode, body: impl IntoResponse) -> Response {
    let mut response = body.into_response();
    *response.status_mut() = status;
    response
}

#[cfg(test)]
pub(crate) use alert_templates::expand_alert_template;
#[cfg(test)]
pub(crate) use alert_templates::expand_alert_template_with_external;
use cardinality::{
    cardinality_active_native_histogram_metrics, cardinality_active_native_histogram_metrics_post,
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
pub use remote_read::remote_read_reset_hint;
use request::{
    CardinalityParams, DiscoveryParams, apply_limit, apply_result_limit, check_range_resolution,
    discovery_matchers, discovery_window, duration_param, enforce_query_range_limit,
    enforce_sample_count, enforce_selected_series_limit, optional_timestamp_ms,
    parse_cardinality_form, parse_cardinality_params, parse_discovery_form, parse_discovery_params,
    parse_limit_parameter, query_timeout, required_form_param, selector_matchers, timestamp_ms,
    validate_timestamp_range,
};
use response::{
    QueryResponseStats, active_series_response, cardinality_label_names_response,
    cardinality_label_values_response, exemplar_key, exemplars_json, labels_json, labels_key,
    sample_string, success_data_response, success_response, success_response_with_stats,
};
pub(crate) use response::{format_sample_value, native_histogram_buckets, native_histogram_string};
use rules::{
    alerts, delete_ruler_config_group, delete_ruler_config_namespace, ruler_config_group,
    ruler_config_namespace, ruler_config_rules, rules, set_ruler_config_group,
};
use search::{
    search_label_names, search_label_names_post, search_label_values, search_label_values_post,
    search_metric_names, search_metric_names_post,
};
use status::{
    alertmanagers, build_info, runtime_info, scrape_pools, status_config, status_flags, targets,
    tsdb_blocks, tsdb_status, wal_replay_status,
};

#[cfg(test)]
mod tests;

mod acquire_query_permit;
mod active_query_guard;
mod api_error;
mod prometheus_api_state;
mod prometheus_router;
mod record_query_response;
mod request_auth;
mod request_caller;

use acquire_query_permit::acquire_query_permit;
use active_query_guard::ActiveQueryGuard;
use admin::{clean_tombstones, delete_series};
use api_error::ApiError;
pub use mimir_alertmanager_router::mimir_alertmanager_router;
pub use mimir_ruler_router::mimir_ruler_router;
pub use prometheus_api_state::PrometheusApiState;
pub use prometheus_router::{mimir_ruler_prometheus_router, prometheus_router};
use record_query_response::record_query_response;
use request_auth::{Rejection, RequestAuth};
use request_caller::{AuthorizedTenant, ParsedForm, ParsedQuery, RequestCaller};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AlertStateKey {
    pub(crate) tenant: String,
    pub(crate) rule_id: String,
    pub(crate) labels: crate::PromqlLabels,
}

pub(crate) struct QueryFrontendState {
    pub(crate) opts: QueryFrontendOptions,
    pub(crate) cache: Arc<dyn RangeQueryCache>,
}

pub(crate) type RulerAlertStateStore = BTreeMap<AlertStateKey, i64>;

pub(crate) type RulerRuleStore =
    BTreeMap<TenantId, BTreeMap<String, BTreeMap<String, serde_yaml::Value>>>;
