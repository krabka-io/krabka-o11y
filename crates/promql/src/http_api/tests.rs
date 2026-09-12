use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::check;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_blockstore::{LabelMatcher, Labels};
use krabka_metrics::{Limits, NativeHistogram, OverridesProvider, ResetHint};
use krabka_observability::server_security::{ServerSecurity, authenticate_requests};
use krabka_units::prelude::*;
use tower::ServiceExt;

use super::{request::unix_now_ms, *};
use crate::{
    ExemplarScan, InMemoryMetricStore, LabelNameCardinality, LabelValueCardinality, MetadataScan,
    ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

// Every request reaches the handlers through the authentication layer, as it
// does on a served listener. With no credentials file, the layer marks each
// request unauthenticated and lets it through.
fn prometheus_router<S: MetricStore + 'static>(state: Arc<PrometheusApiState<S>>) -> Router {
    authenticate_requests(super::prometheus_router(state), &ServerSecurity::default())
}

mod a_read_that_repeats_its_tenant_reads_that_tenant;
mod admin_erasure;
mod an_unconfigured_state_enforces_the_default_query_limits;
mod annotated_query_body;
mod annotation_query_uri;
mod annotation_store;
mod cardinality_active_series_rejects_over_tenant_limit;
mod discovery_rejects_label_counts_over_tenant_series_limit;
mod expand_alert_template_substitutions;
mod float_formatting_matches_go;
mod instant_query_without_time_defaults_to_current_time;
mod promql_evaluation_rejects_series_over_tenant_limit;
mod query_annotations_reach_the_response_envelope;
mod query_handlers_respect_configured_concurrency_limit;
mod query_range_annotations_agree_through_the_frontend_and_the_direct_path;
mod query_range_rejects_ranges_over_tenant_limit;
mod query_range_rejects_resolution_over_point_cap_without_limits;
mod query_rejects_lookback_over_tenant_limit;
mod query_reports_sample_cap_as_execution_error;
mod query_timeout_and_stats;
mod read_paths_admit_requests_when_limits_are_off;
mod read_paths_answer_an_unusable_tenant_as_mimir_does;
mod read_paths_reject_windows_over_tenant_limit;
mod ruler_config_mutations_are_audited;
mod ruler_tenants_lists_each_configured_tenant;
mod series_rejects_selected_series_over_tenant_limit;
mod slow_empty_store;
mod two_series_store;

use annotated_query_body::annotated_query_body;
use annotation_query_uri::annotation_query_uri;
use annotation_store::annotation_store;
use slow_empty_store::SlowEmptyStore;
use two_series_store::two_series_store;
