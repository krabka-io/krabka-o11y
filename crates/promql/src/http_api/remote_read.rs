use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{Float64Type, Int64Type, UInt64Type},
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::{LabelMatcher, Labels, MatchOp, SeriesFingerprint, TenantId};
use krabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms,
    wire::{encode_chunked_read_frames, negotiate_read_response_type, pb, snappy_block_decode},
};
use num_traits::ToPrimitive;
use prost::Message;

use super::{
    ApiError, Extension, Principal, PrometheusApiState, authorized_tenant_from_headers,
    enforce_query_range_limit, enforce_sample_count, enforce_selected_series_limit,
    validate_timestamp_range,
};
use crate::{
    MetricStore, PromqlError,
    store::{ExemplarRecord, ScanResult},
};

mod append_remote_read_exemplars;
mod append_remote_read_float_samples;
mod append_remote_read_histogram_samples;
mod header_list_includes;
mod negotiate_remote_read_response_type;
mod remote_read_bucket_spans;
mod remote_read_exemplar;
mod remote_read_fn;
mod remote_read_histogram;
mod remote_read_histogram_count;
mod remote_read_histogram_deltas;
mod remote_read_histogram_zero_count;
mod remote_read_labels;
mod remote_read_matchers;
mod remote_read_reset_hint;
mod remote_read_response;
mod remote_read_series;
mod require_remote_read_headers;

use append_remote_read_exemplars::append_remote_read_exemplars;
use append_remote_read_float_samples::append_remote_read_float_samples;
use append_remote_read_histogram_samples::append_remote_read_histogram_samples;
use header_list_includes::header_list_includes;
use negotiate_remote_read_response_type::negotiate_remote_read_response_type;
use remote_read_bucket_spans::remote_read_bucket_spans;
use remote_read_exemplar::remote_read_exemplar;
pub(super) use remote_read_fn::remote_read;
use remote_read_histogram::remote_read_histogram;
use remote_read_histogram_count::remote_read_histogram_count;
use remote_read_histogram_deltas::remote_read_histogram_deltas;
use remote_read_histogram_zero_count::remote_read_histogram_zero_count;
use remote_read_labels::remote_read_labels;
use remote_read_matchers::remote_read_matchers;
use remote_read_reset_hint::remote_read_reset_hint;
use remote_read_response::remote_read_response;
use remote_read_series::remote_read_series;
use require_remote_read_headers::require_remote_read_headers;
