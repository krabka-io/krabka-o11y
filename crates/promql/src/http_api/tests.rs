use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_blockstore::{LabelMatcher, Labels};
use krabka_metrics::{Limits, OverridesProvider};
use krabka_units::prelude::*;
use tower::ServiceExt;

use super::*;
use crate::{
    ExemplarRecord, InMemoryMetricStore, LabelNameCardinality, LabelValueCardinality,
    MetadataRecord, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

mod cardinality_active_series_rejects_over_tenant_limit;
mod expand_alert_template_substitutions;
mod float_formatting_matches_go;
mod instant_query_without_time_defaults_to_current_time;
mod query_handlers_respect_configured_concurrency_limit;
mod query_range_rejects_ranges_over_tenant_limit;
mod query_range_rejects_resolution_over_point_cap_without_limits;
mod rejects_tenant_id_with_unsupported_character;
mod series_rejects_selected_series_over_tenant_limit;
mod slow_empty_store;

use slow_empty_store::SlowEmptyStore;
