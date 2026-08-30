use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};

use super::{
    ApiError, DiscoveryParams, PrometheusApiState, apply_limit, discovery_matchers,
    discovery_window, enforce_selected_series_limit, labels_json, labels_key, parse_discovery_form,
    parse_discovery_params, record_query_response, success_data_response, tenant_from_headers,
};
use crate::MetricStore;

// === split-modules: generated submodules ===
mod label_values;
mod label_values_dispatch;
mod label_values_inner;
mod label_values_post;
mod labels;
mod labels_dispatch;
mod labels_inner;
mod labels_post;
mod series;
mod series_dispatch;
mod series_inner;
mod series_post;

pub (super) use label_values::label_values;
use label_values_dispatch::label_values_dispatch;
use label_values_inner::label_values_inner;
pub (super) use label_values_post::label_values_post;
pub (super) use labels::labels;
use labels_dispatch::labels_dispatch;
use labels_inner::labels_inner;
pub (super) use labels_post::labels_post;
pub (super) use series::series;
use series_dispatch::series_dispatch;
use series_inner::series_inner;
pub (super) use series_post::series_post;
