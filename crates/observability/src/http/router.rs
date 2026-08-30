use axum::response::IntoResponse;

use crate::{
    AtomicOrdering, Bytes, COMPACTOR_OPS, CompactorDeleteState, DistributorState, Extension,
    HttpQueryError, QUERIER_OPS, QuerierState, RawQuery, Response, RoleOps, Router,
    ServiceReadiness, SharedLogDeleteRequests, State, StatusCode, api_prom_label_names,
    api_prom_label_names_post, api_prom_label_values, api_prom_label_values_post, api_prom_query,
    api_prom_query_post, api_prom_query_range, api_prom_query_range_post, api_prom_series,
    api_prom_series_post, cancel_delete_request, create_delete_request, decode_form_component,
    detected_field_values, detected_field_values_post, detected_fields, detected_fields_post,
    detected_labels, detected_labels_post, form_body_query, format_query, format_query_post, get,
    index_stats, index_stats_post, index_volume, index_volume_post, index_volume_range,
    index_volume_range_post, json, json_response, label_names, label_names_post, label_values,
    label_values_post, list_delete_requests, patterns, patterns_post, query, query_post,
    query_range, query_range_post,
    ruler::{
        create_loki_rule_group, delete_loki_rule_group, delete_loki_rule_namespace,
        loki_page_not_found, loki_rule_group, loki_rule_namespace, loki_rules, prometheus_alerts,
        prometheus_rules, ring_status_page, ruler_status_page,
    },
    series, series_post, status_metrics, tail, text_response, with_role_ops_routes,
};

// === split-modules: generated submodules ===
mod compactor_router_with_delete_requests;
mod flush_ingester_chunks;
mod get_prepare_shutdown;
mod log_level;
mod log_level_failed_response;
mod log_level_post;
mod loki_config_target;
mod loki_router;
mod loki_router_with_readiness;
mod memberlist_status;
mod parse_log_level_param;
mod query_param_value;
mod ready;
mod role_config;
mod role_metrics;
mod role_ring;
mod role_services;
mod ruler_ring;
mod scheduler_ring;
mod set_prepare_shutdown;
mod shutdown_ingester;
mod status_config;
mod status_services;
mod unset_prepare_shutdown;

pub(crate) use compactor_router_with_delete_requests::compactor_router_with_delete_requests;
pub(crate) use flush_ingester_chunks::flush_ingester_chunks;
pub(crate) use get_prepare_shutdown::get_prepare_shutdown;
pub(crate) use log_level::log_level;
pub(crate) use log_level_failed_response::log_level_failed_response;
pub(crate) use log_level_post::log_level_post;
pub(crate) use loki_config_target::LOKI_CONFIG_TARGET;
pub use loki_router::loki_router;
pub(crate) use loki_router_with_readiness::loki_router_with_readiness;
pub(crate) use memberlist_status::memberlist_status;
pub(crate) use parse_log_level_param::parse_log_level_param;
pub(crate) use query_param_value::query_param_value;
pub(crate) use ready::ready;
pub(crate) use role_config::role_config;
pub(crate) use role_metrics::role_metrics;
pub(crate) use role_ring::role_ring;
pub(crate) use role_services::role_services;
pub(crate) use ruler_ring::ruler_ring;
pub(crate) use scheduler_ring::scheduler_ring;
pub(crate) use set_prepare_shutdown::set_prepare_shutdown;
pub(crate) use shutdown_ingester::shutdown_ingester;
pub(crate) use status_config::status_config;
pub(crate) use status_services::status_services;
pub(crate) use unset_prepare_shutdown::unset_prepare_shutdown;
