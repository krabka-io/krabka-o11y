use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json,
    body::Bytes,
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use krabka_blockstore::Labels;

use super::{
    ApiError, CardinalityParams, PrometheusApiState, active_series_response, apply_limit,
    cardinality_label_names_response, cardinality_label_values_response,
    enforce_selected_series_limit, labels_key, parse_cardinality_form, parse_cardinality_params,
    selector_matchers, tenant_from_headers,
};
use crate::MetricStore;

// === split-modules: generated submodules ===
mod cardinality_active_series;
mod cardinality_active_series_inner;
mod cardinality_active_series_post;
mod cardinality_label_names;
mod cardinality_label_names_inner;
mod cardinality_label_names_post;
mod cardinality_label_values;
mod cardinality_label_values_inner;
mod cardinality_label_values_post;
mod cardinality_series;
mod cardinality_series_for_params;

pub (super) use cardinality_active_series::cardinality_active_series;
use cardinality_active_series_inner::cardinality_active_series_inner;
pub (super) use cardinality_active_series_post::cardinality_active_series_post;
pub (super) use cardinality_label_names::cardinality_label_names;
use cardinality_label_names_inner::cardinality_label_names_inner;
pub (super) use cardinality_label_names_post::cardinality_label_names_post;
pub (super) use cardinality_label_values::cardinality_label_values;
use cardinality_label_values_inner::cardinality_label_values_inner;
pub (super) use cardinality_label_values_post::cardinality_label_values_post;
use cardinality_series::cardinality_series;
use cardinality_series_for_params::cardinality_series_for_params;
