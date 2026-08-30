use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use krabka_blockstore::LabelMatcher;
use krabka_metrics::{QueryEnforcer, validate_tenant};
use krabka_units::prelude::*;
use num_traits::ToPrimitive;
use promql_parser::parser::Expr;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;

use super::{ApiError, PrometheusApiState};
use crate::{
    MetricStore, PromqlError, QueryResult,
    engine::{MAX_RESOLUTION_POINTS, label_matcher_sets},
    parse_promql,
};

// === split-modules: generated submodules ===
mod apply_limit;
mod apply_result_limit;
mod cardinality_params;
mod check_range_resolution;
mod discovery_matchers;
mod discovery_params;
mod discovery_window;
mod duration_param;
mod enforce_sample_count;
mod enforce_selected_series_limit;
mod optional_timestamp_ms;
mod parse_cardinality_form;
mod parse_cardinality_params;
mod parse_discovery_form;
mod parse_discovery_params;
mod parse_limit_parameter;
mod prometheus_duration_ms;
mod required_form_param;
mod rfc3339_to_ms;
mod seconds_to_ms;
mod selector_matchers;
mod tenant_from_headers;
mod timestamp_ms;
mod unix_now_ms;
mod validate_timestamp_range;

pub(super) use apply_limit::apply_limit;
pub(super) use apply_result_limit::apply_result_limit;
pub(super) use cardinality_params::CardinalityParams;
pub(super) use check_range_resolution::check_range_resolution;
pub(super) use discovery_matchers::discovery_matchers;
pub(super) use discovery_params::DiscoveryParams;
pub(super) use discovery_window::discovery_window;
pub(super) use duration_param::duration_param;
pub(super) use enforce_sample_count::enforce_sample_count;
pub(super) use enforce_selected_series_limit::enforce_selected_series_limit;
pub(super) use optional_timestamp_ms::optional_timestamp_ms;
pub(super) use parse_cardinality_form::parse_cardinality_form;
pub(super) use parse_cardinality_params::parse_cardinality_params;
pub(super) use parse_discovery_form::parse_discovery_form;
pub(super) use parse_discovery_params::parse_discovery_params;
pub(super) use parse_limit_parameter::parse_limit_parameter;
use prometheus_duration_ms::prometheus_duration_ms;
pub(super) use required_form_param::required_form_param;
use rfc3339_to_ms::rfc3339_to_ms;
use seconds_to_ms::seconds_to_ms;
pub(super) use selector_matchers::selector_matchers;
pub(super) use tenant_from_headers::tenant_from_headers;
pub(super) use timestamp_ms::timestamp_ms;
pub(super) use unix_now_ms::unix_now_ms;
pub(super) use validate_timestamp_range::validate_timestamp_range;
