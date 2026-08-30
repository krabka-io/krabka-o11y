use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_metrics::{BucketSpan, NativeHistogram};
use serde_json::{Map, Value, json};

use super::apply_limit;
use crate::{QueryResult, RangeSeries, SampleValue, store::ExemplarRecord};

// === split-modules: generated submodules ===
mod active_series_response;
mod append_custom_histogram_buckets;
mod append_spanned_buckets;
mod append_standard_histogram_buckets;
mod boundary_closed_both;
mod boundary_open_left;
mod boundary_open_right;
mod cardinality_label_names_response;
mod cardinality_label_values_response;
mod custom_histogram_bound;
mod exemplar_key;
mod exemplars_json;
mod format_float_exponent;
mod format_sample_value;
mod format_timestamp_token;
mod histogram_bucket_json;
mod labels_json;
mod labels_key;
mod native_histogram_buckets_json;
mod native_histogram_json;
mod range_matrix_json;
mod result_json;
mod sample_string;
mod standard_histogram_bound;
mod success_data_response;
mod success_response;
mod timestamp_seconds;

pub (super) use active_series_response::active_series_response;
use append_custom_histogram_buckets::append_custom_histogram_buckets;
use append_spanned_buckets::append_spanned_buckets;
use append_standard_histogram_buckets::append_standard_histogram_buckets;
use boundary_closed_both::BOUNDARY_CLOSED_BOTH;
use boundary_open_left::BOUNDARY_OPEN_LEFT;
use boundary_open_right::BOUNDARY_OPEN_RIGHT;
pub (super) use cardinality_label_names_response::cardinality_label_names_response;
pub (super) use cardinality_label_values_response::cardinality_label_values_response;
use custom_histogram_bound::custom_histogram_bound;
pub (super) use exemplar_key::exemplar_key;
pub (super) use exemplars_json::exemplars_json;
use format_float_exponent::format_float_exponent;
pub (crate) use format_sample_value::format_sample_value;
use format_timestamp_token::format_timestamp_token;
use histogram_bucket_json::HistogramBucketJson;
pub (super) use labels_json::labels_json;
pub (super) use labels_key::labels_key;
use native_histogram_buckets_json::native_histogram_buckets_json;
use native_histogram_json::native_histogram_json;
use range_matrix_json::range_matrix_json;
use result_json::result_json;
pub (super) use sample_string::sample_string;
use standard_histogram_bound::standard_histogram_bound;
pub (super) use success_data_response::success_data_response;
pub (super) use success_response::success_response;
use timestamp_seconds::timestamp_seconds;
